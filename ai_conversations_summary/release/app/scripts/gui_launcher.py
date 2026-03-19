"""Tkinter GUI entry for data processing and vector search workflows.

This module orchestrates user actions, async subprocess execution, and
vector-search status updates. Business logic stays in dedicated scripts.
"""

import queue
import shutil
import subprocess
import sys
import threading
import ctypes
import os
import json
import re
import tempfile
import time
import calendar
import webbrowser
from uuid import uuid4
from urllib.parse import quote, unquote
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable
import tkinter as tk
from tkinter import filedialog, scrolledtext, simpledialog, ttk

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
if str(WORKSPACE_ROOT) not in sys.path:
	sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.config import get_settings

try:
	from tkcalendar import DateEntry as TkDateEntry
except Exception:  # noqa: BLE001
	TkDateEntry = None

from api_config import API_BASE_URL, API_KEY, EMBEDDING_MODEL, MODEL, TIMEOUT
from gui_common import (
	HoverToolTip,
	env_float as _env_float,
	env_int as _env_int,
	hide_console_on_windows as _hide_console_on_windows,
	no_window_creationflags as _no_window_creationflags,
	relaunch_with_pythonw_on_windows as _relaunch_with_pythonw_on_windows,
)
from rag_session_service import (
	build_rag_session_markdown,
	derive_local_session_title,
	parse_rag_session_markdown,
	sanitize_filename_part,
	sanitize_session_title,
	session_file_name,
)
from python_runtime_service import (
	build_python_candidates,
	discover_python_from_where,
	python_supports_module,
	resolve_python_executable,
	resolve_python_for_module,
)
from rag_vector_index import RAGIndexError, prune_stale_index_entries, search_vector_index_with_diagnostics


DATE_PICKER_MIN_YEAR = 1970
DATE_PICKER_MAX_YEAR = 2099
_CORE_SETTINGS = get_settings()


class SummaryGuiApp:
	def __init__(self, root: tk.Tk) -> None:
		self.root = root
		# Lock Tk text/layout scaling to keep UI proportions consistent across machines.
		self.gui_tk_scaling = 1.25
		try:
			self.root.tk.call("tk", "scaling", self.gui_tk_scaling)
		except tk.TclError:
			pass
		self.root.title("AI Chat RAG知识库")
		self.root.geometry("800x620")
		self.root.minsize(800, 520)

		self.colors = {
			"bg": "#272822",
			"panel": "#2F3129",
			"panel_soft": "#3E3D32",
			"text": "#F8F8F2",
			"text_muted": "#B7B7A4",
			"accent": "#A6E22E",
			"cyan": "#66D9EF",
			"orange": "#FD971F",
			"pink": "#F92672",
			"purple": "#AE81FF",
			"select": "#49483E",
		}

		self.script_dir = Path(__file__).resolve().parent
		self.workspace_root = self.script_dir.parent
		self.core_service_root = self.workspace_root.parent / "core_service"
		self.core_data_dir = self.core_service_root / "data"
		self.data_dir = self.workspace_root / "data"
		self.config_path = self.script_dir / "api_config.py"
		self.core_config_path = self.workspace_root.parent / "core_service" / "config.local.json"
		self.raw_dir = self.data_dir / "raw_dir"
		self.extracted_dir = self.data_dir / "extracted_dir"
		self.summarize_dir = self.data_dir / "summarize_dir"
		vector_db_env = (os.getenv("AI_SUMMARY_VECTOR_DB_DIR", "") or "").strip()
		if vector_db_env:
			self.vector_index_dir = Path(vector_db_env)
		else:
			preferred_vector_db = self.core_data_dir / "vector_db"
			legacy_vector_db = self.data_dir / "vector_db"

			def _has_vector_index(path: Path) -> bool:
				meta = path / "metadata.json"
				faiss = path / "faiss.index"
				backend = path / "backend.json"
				return meta.exists() and (faiss.exists() or backend.exists())

			preferred_ready = _has_vector_index(preferred_vector_db)
			legacy_ready = _has_vector_index(legacy_vector_db)
			if preferred_ready:
				self.vector_index_dir = preferred_vector_db
			elif legacy_ready:
				self.vector_index_dir = legacy_vector_db
			else:
				self.vector_index_dir = preferred_vector_db if preferred_vector_db.exists() or not legacy_vector_db.exists() else legacy_vector_db
		self.rag_sessions_dir = self.data_dir / "rag_sessions"
		self.rag_session_file_prefix = "session_"
		self.legacy_web_sessions_file = self.rag_sessions_dir / "web_sessions.json"
		self.documents_dir = self.workspace_root / "documents"
		self.readme_path = self.workspace_root / "README.md"
		for path in (self.data_dir, self.raw_dir, self.extracted_dir, self.summarize_dir, self.rag_sessions_dir, self.vector_index_dir):
			path.mkdir(parents=True, exist_ok=True)

		self.source_var = tk.StringVar(value="deepseek")
		self.base_url_var = tk.StringVar(value=API_BASE_URL)
		self.model_var = tk.StringVar(value=MODEL)
		self.rag_answer_mode_var = tk.StringVar(value="local")
		self.rag_model_label_var = tk.StringVar(value="")
		self._refresh_rag_model_label()
		self.raw_api_key = (os.getenv("DEEPSEEK_API_KEY") or API_KEY).strip()
		self.api_key_var = tk.StringVar(value=self.raw_api_key)
		self.api_key_visible = False
		today = date.today()
		self.start_date_var = tk.StringVar(value=(today - timedelta(days=365)).strftime("%Y-%m-%d"))
		self.end_date_var = tk.StringVar(value=today.strftime("%Y-%m-%d"))
		self.extracted_count_var = tk.StringVar(value="提取文件: 0")
		# Cross-thread event bus: worker threads push events, UI thread consumes them.
		self.log_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
		self.preview_node_paths: dict[str, Path] = {}
		self.preview_search_var = tk.StringVar(value="")
		self.preview_mode = "browse"
		self.vector_search_results: list[dict[str, object]] = []
		self._pending_vector_results: list[dict[str, object]] = []
		self.rag_question_var = tk.StringVar(value="")
		self.rag_chat_text: scrolledtext.ScrolledText | None = None
		self.rag_input_text: tk.Text | None = None
		self.rag_mode_local_btn: tk.Button | None = None
		self.rag_mode_deepseek_btn: tk.Button | None = None
		self.rag_mode_reasoner_btn: tk.Button | None = None
		self.rag_session_listbox: tk.Listbox | None = None
		self.rag_session_menu: tk.Menu | None = None
		self.rag_sessions: list[dict[str, object]] = []
		self.rag_current_session_idx = -1
		self.rag_local_answer_btn: ttk.Button | None = None
		self.rag_send_btn: ttk.Button | None = None
		self.rag_abort_btn: ttk.Button | None = None
		self.rag_qa_running = False
		self.rag_cancel_event = threading.Event()
		self.rag_active_process: subprocess.Popen | None = None
		self.rag_process_lock = threading.Lock()
		self.vector_fallback_warned = False
		self.vector_progress_window: tk.Toplevel | None = None
		self.vector_progress_label: tk.Label | None = None
		self.vector_progress_after_id: str | None = None
		self.vector_progress_start_ts: float | None = None
		self.vector_progress_stage: str = "准备中"
		self.is_running = False
		self.tooltips: list[HoverToolTip] = []
		self.main_notebook: ttk.Notebook | None = None
		self.preview_tab_frame: ttk.Frame | None = None
		self._markdown_link_seq = 0
		self.upload_btn_tooltip: HoverToolTip | None = None
		self.batch_btn_tooltip: HoverToolTip | None = None
		self.date_picker_window: tk.Toplevel | None = None
		self.date_picker_month: date | None = None
		self.date_picker_target_var: tk.StringVar | None = None
		self.date_picker_month_label: tk.Label | None = None
		self.date_picker_grid_frame: tk.Frame | None = None
		self.date_picker_anchor_widget: tk.Widget | None = None
		self.start_date_entry: ttk.Entry | None = None
		self.end_date_entry: ttk.Entry | None = None
		self.date_picker_focus_check_after_id: str | None = None

		self._setup_theme()
		self._build_ui()
		self._apply_dark_title_bar()
		self.root.after(100, self._drain_log_queue)

	def _apply_dark_title_bar(self) -> None:
		# Enable dark title bar on Windows 10/11 so non-client area matches app theme.
		if sys.platform != "win32":
			return

		try:
			self.root.update_idletasks()
			hwnd = ctypes.windll.user32.GetParent(self.root.winfo_id())
			if not hwnd:
				return

			DWMWA_USE_IMMERSIVE_DARK_MODE = 20
			enabled = ctypes.c_int(1)
			ctypes.windll.dwmapi.DwmSetWindowAttribute(
				hwnd,
				DWMWA_USE_IMMERSIVE_DARK_MODE,
				ctypes.byref(enabled),
				ctypes.sizeof(enabled),
			)
		except Exception:
			# Keep app functional if OS/theme API is unavailable.
			return

	def _setup_theme(self) -> None:
		style = ttk.Style(self.root)
		style.theme_use("clam")
		font_family = "Microsoft YaHei UI"
		base_size = 10
		title_size = 14
		caption_size = 10

		self.root.configure(bg=self.colors["bg"])

		style.configure("Root.TFrame", background=self.colors["bg"])
		style.configure("Card.TFrame", background=self.colors["panel"], relief="flat")

		style.configure(
			"Title.TLabel",
			background=self.colors["panel"],
			foreground=self.colors["accent"],
			font=(font_family, title_size, "bold"),
		)
		style.configure(
			"SectionTitle.TLabel",
			background=self.colors["panel"],
			foreground=self.colors["cyan"],
			font=(font_family, 16, "bold"),
		)
		style.configure(
			"Caption.TLabel",
			background=self.colors["panel"],
			foreground=self.colors["text_muted"],
			font=(font_family, caption_size),
		)
		style.configure(
			"Field.TLabel",
			background=self.colors["panel"],
			foreground=self.colors["text"],
			font=(font_family, base_size),
		)

		style.configure(
			"Action.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["panel_soft"],
			foreground=self.colors["text"],
		)
		style.map(
			"Action.TButton",
			background=[("active", self.colors["select"]), ("pressed", self.colors["select"])],
			foreground=[("disabled", "#8F908A")],
		)

		style.configure(
			"Primary.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["cyan"],
			foreground="#11120F",
		)
		style.map(
			"Primary.TButton",
			background=[("active", "#79E6FA"), ("pressed", "#57C9DF")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"Accent.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["orange"],
			foreground="#11120F",
		)
		style.map(
			"Accent.TButton",
			background=[("active", "#FFAF4A"), ("pressed", "#E48309")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"Blue.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["cyan"],
			foreground="#11120F",
		)
		style.map(
			"Blue.TButton",
			background=[("active", "#79E6FA"), ("pressed", "#57C9DF")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"Green.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["accent"],
			foreground="#11120F",
		)
		style.map(
			"Green.TButton",
			background=[("active", "#B8F05A"), ("pressed", "#8FC71F")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"Pink.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["pink"],
			foreground="#11120F",
		)
		style.map(
			"Pink.TButton",
			background=[("active", "#FF4C8E"), ("pressed", "#D6175E")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"Red.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["pink"],
			foreground="#11120F",
		)
		style.map(
			"Red.TButton",
			background=[("active", "#FF4C8E"), ("pressed", "#D6175E")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"Orange.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["orange"],
			foreground="#11120F",
		)
		style.map(
			"Orange.TButton",
			background=[("active", "#FFAF4A"), ("pressed", "#E48309")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"PurpleMini.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background=self.colors["purple"],
			foreground="#11120F",
		)
		style.map(
			"PurpleMini.TButton",
			background=[("active", "#BE95FF"), ("pressed", "#8C5DE8")],
			foreground=[("disabled", "#6E6E6E")],
		)

		# Batch tab palette: unified Monokai hierarchy to avoid rainbow-like collisions.
		style.configure(
			"BatchFlow.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background="#7FBF3F",
			foreground="#11120F",
		)
		style.map(
			"BatchFlow.TButton",
			background=[("active", "#95D353"), ("pressed", "#6EA935")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"BatchAssist.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background="#5FB9CC",
			foreground="#11120F",
		)
		style.map(
			"BatchAssist.TButton",
			background=[("active", "#73CCE0"), ("pressed", "#4AA6B9")],
			foreground=[("disabled", "#6E6E6E")],
		)
		style.configure(
			"BatchAssistCompact.TButton",
			font=(font_family, 10, "bold"),
			padding=(4, 6),
			borderwidth=0,
			background="#5FB9CC",
			foreground="#11120F",
		)
		style.map(
			"BatchAssistCompact.TButton",
			background=[("active", "#73CCE0"), ("pressed", "#4AA6B9")],
			foreground=[("disabled", "#6E6E6E")],
		)

		style.configure(
			"BatchTool.TButton",
			font=(font_family, 10, "bold"),
			padding=(14, 8),
			borderwidth=0,
			background="#4B4C40",
			foreground=self.colors["text"],
		)
		style.map(
			"BatchTool.TButton",
			background=[("active", "#5A5B4F"), ("pressed", "#3F4036")],
			foreground=[("disabled", "#8F908A")],
		)

		style.configure(
			"Monokai.TCombobox",
			font=(font_family, 10),
			fieldbackground=self.colors["panel_soft"],
			background=self.colors["panel_soft"],
			foreground=self.colors["text"],
			arrowcolor=self.colors["accent"],
			padding=6,
		)
		style.configure(
			"Monokai.TEntry",
			font=(font_family, 10),
			fieldbackground=self.colors["panel_soft"],
			foreground=self.colors["text"],
			insertcolor=self.colors["accent"],
			padding=6,
		)

		style.configure("Tab.TFrame", background=self.colors["bg"])
		style.configure("TabCard.TFrame", background=self.colors["panel"], relief="flat")
		style.configure(
			"Monokai.Treeview",
			background="#1E1F1C",
			foreground=self.colors["text"],
			fieldbackground="#1E1F1C",
			borderwidth=0,
			relief="flat",
		)
		# Use a minimal layout to suppress native Treeview borders from OS themes.
		try:
			style.layout("Monokai.Treeview", [("Treeview.treearea", {"sticky": "nswe"})])
		except tk.TclError:
			pass
		style.map("Monokai.Treeview", background=[("selected", self.colors["select"])], foreground=[("selected", self.colors["text"])])
		style.configure("Monokai.Treeview.Heading", background=self.colors["panel_soft"], foreground=self.colors["text"], font=(font_family, base_size, "bold"))
		style.configure("TNotebook", background=self.colors["bg"], borderwidth=0)
		style.configure("TNotebook.Tab", background=self.colors["panel_soft"], foreground=self.colors["text"], padding=(14, 8), font=(font_family, base_size, "bold"))
		style.map("TNotebook.Tab", background=[("selected", self.colors["select"]), ("active", "#555446")], foreground=[("selected", self.colors["accent"])])
		style.map(
			"Monokai.TCombobox",
			fieldbackground=[("readonly", self.colors["panel_soft"])],
			selectbackground=[("readonly", self.colors["select"])],
			selectforeground=[("readonly", self.colors["text"])],
		)

		# Style combobox dropdown list colors to match Monokai.
		self.root.option_add("*TCombobox*Listbox.background", self.colors["panel_soft"])
		self.root.option_add("*TCombobox*Listbox.foreground", self.colors["text"])
		self.root.option_add("*TCombobox*Listbox.selectBackground", self.colors["select"])
		self.root.option_add("*TCombobox*Listbox.selectForeground", self.colors["text"])

	def _build_ui(self) -> None:
		main = ttk.Frame(self.root, style="Root.TFrame", padding=12)
		main.pack(fill=tk.BOTH, expand=True)

		notebook = ttk.Notebook(main)
		notebook.pack(fill=tk.BOTH, expand=True)
		self.main_notebook = notebook

		batch_tab = ttk.Frame(notebook, style="Tab.TFrame", padding=10)
		preview_tab = ttk.Frame(notebook, style="Tab.TFrame", padding=10)
		self.preview_tab_frame = preview_tab
		rag_qa_tab = ttk.Frame(notebook, style="Tab.TFrame", padding=10)
		info_tab = ttk.Frame(notebook, style="Tab.TFrame", padding=10)

		notebook.add(preview_tab, text="预览")
		notebook.add(rag_qa_tab, text="RAG Q&A")
		notebook.add(batch_tab, text="批处理")
		notebook.add(info_tab, text="Info")

		self._build_batch_tab(batch_tab)
		self._build_preview_tab(preview_tab)
		self._build_rag_qa_tab(rag_qa_tab)
		self._build_info_tab(info_tab)

	def _build_batch_tab(self, parent: ttk.Frame) -> None:
		action_btn_width = 13
		side_btn_width = 8

		top = ttk.Frame(parent, style="TabCard.TFrame", padding=16)
		top.pack(fill=tk.X, pady=(0, 12))
		top.columnconfigure(0, weight=1)

		content_wrap = ttk.Frame(top, style="Card.TFrame")
		content_wrap.grid(row=0, column=0, sticky="ew")
		content_wrap.columnconfigure(0, weight=1)

		header_row = ttk.Frame(content_wrap, style="Card.TFrame")
		header_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
		header_row.columnconfigure(0, weight=1)

		header_left = ttk.Frame(header_row, style="Card.TFrame")
		header_left.grid(row=0, column=0, sticky="w")

		title = ttk.Label(header_left, text="数据处理面板", style="SectionTitle.TLabel")
		title.grid(row=0, column=0, sticky="w", pady=(0, 4))

		ttk.Label(
			header_left,
			text="上传源数据，选择来源并执行批处理或 AI 总结。",
			style="Caption.TLabel",
		).grid(row=1, column=0, sticky="w", pady=(0, 8))

		header_right = ttk.Frame(header_row, style="Card.TFrame")
		header_right.grid(row=0, column=1, sticky="e", padx=(12, 0))
		self._build_data_stats_panel(header_right)

		button_row = ttk.Frame(content_wrap, style="Card.TFrame")
		button_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
		for spacer_col in (1, 3, 5, 7):
			button_row.columnconfigure(spacer_col, weight=1)

		self.upload_btn = ttk.Button(
			button_row,
			text="上传源文件",
			style="Red.TButton",
			width=action_btn_width,
			command=self.upload_json_files,
		)
		self.upload_btn.grid(row=0, column=0, padx=(2, 2), pady=4, sticky="w")
		self.upload_btn_tooltip = self._attach_tooltip(self.upload_btn, "选择并复制源文件 (如JSON) 到 data/raw_dir。")

		self.batch_btn = ttk.Button(
			button_row,
			text="格式批处理",
			style="Orange.TButton",
			width=action_btn_width,
			command=self.run_batch_processing,
		)
		self.batch_btn.grid(row=0, column=2, padx=(2, 2), pady=4)
		self.batch_btn_tooltip = self._attach_tooltip(self.batch_btn, "对 raw_dir 文件执行格式批处理。")

		self.summary_btn = ttk.Button(
			button_row,
			text="AI 总结",
			style="BatchFlow.TButton",
			width=action_btn_width,
			command=self.run_ai_summary,
		)
		self.summary_btn.grid(row=0, column=4, padx=(2, 2), pady=4)
		self._attach_tooltip(self.summary_btn, "调用大模型批量生成总结并写入 summarize_dir。")

		self.classify_btn = ttk.Button(
			button_row,
			text="输出分类",
			style="Blue.TButton",
			width=action_btn_width,
			command=self.run_output_classification,
		)
		self.classify_btn.grid(row=0, column=6, padx=(2, 2), pady=4)
		self._attach_tooltip(self.classify_btn, "按分类规则整理总结结果到 documents 目录。")

		self.sync_embed_btn = ttk.Button(
			button_row,
			text="补齐向量",
			style="PurpleMini.TButton",
			width=action_btn_width,
			command=self.run_sync_missing_embeddings,
		)
		self.sync_embed_btn.grid(row=0, column=8, padx=(2, 2), pady=4, sticky="e")
		self._attach_tooltip(self.sync_embed_btn, "为未入库文档补齐向量并更新 FAISS 索引。")

		config_frame = ttk.Frame(top, style="Card.TFrame", padding=(0, 0, 0, 0))
		config_frame.grid(row=1, column=0, sticky="ew")
		for col in range(4):
			config_frame.columnconfigure(col, weight=0)
		config_frame.columnconfigure(1, weight=1)

		left_col = ttk.Frame(config_frame, style="Card.TFrame")
		left_col.grid(row=0, column=0, sticky="w", padx=(0, 8))
		left_col.columnconfigure(1, weight=1)
		left_col.rowconfigure(0, minsize=40)
		left_col.rowconfigure(1, minsize=40)
		ttk.Label(left_col, text="数据来源", style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
		self.source_combo = ttk.Combobox(
			left_col,
			textvariable=self.source_var,
			style="Monokai.TCombobox",
			state="readonly",
			width=14,
			values=["DeepSeek", "ChatGPT", "Other"],
		)
		self.source_combo.grid(row=0, column=1, sticky="ew", pady=4, ipady=3)
		self.source_combo.bind("<<ComboboxSelected>>", self._on_source_changed, add="+")
		self._update_batch_button_tooltip()
		ttk.Label(left_col, text="MODEL", style="Field.TLabel").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
		ttk.Entry(left_col, textvariable=self.model_var, style="Monokai.TEntry").grid(row=1, column=1, sticky="ew", pady=4, ipady=3)

		mid_col = ttk.Frame(config_frame, style="Card.TFrame")
		mid_col.grid(row=0, column=1, sticky="ew", padx=(4, 4))
		mid_col.columnconfigure(1, weight=1)
		mid_col.rowconfigure(0, minsize=40)
		mid_col.rowconfigure(1, minsize=40)
		ttk.Label(mid_col, text="API_BASE_URL", style="Field.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
		ttk.Entry(mid_col, textvariable=self.base_url_var, style="Monokai.TEntry").grid(row=0, column=1, sticky="ew", pady=4, ipady=3)
		ttk.Label(mid_col, text="API_KEY", style="Field.TLabel").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
		self.api_key_entry = ttk.Entry(mid_col, textvariable=self.api_key_var, show="*", style="Monokai.TEntry")
		self.api_key_entry.grid(row=1, column=1, sticky="ew", pady=4, ipady=3)

		right_col = ttk.Frame(config_frame, style="Card.TFrame")
		right_col.grid(row=0, column=3, sticky="e", padx=(4, 0))
		right_col.columnconfigure(0, weight=0)
		right_col.rowconfigure(0, minsize=40)
		right_col.rowconfigure(1, minsize=40)
		self.save_cfg_btn = ttk.Button(
			right_col,
			text="保存配置",
			style="BatchTool.TButton",
			width=side_btn_width,
			command=self.save_config,
		)
		self.save_cfg_btn.grid(row=0, column=0, sticky="w", pady=4)
		self._attach_tooltip(self.save_cfg_btn, "将当前 API_BASE_URL、MODEL、API_KEY 保存到配置文件。")
		self.show_key_btn = ttk.Button(
			right_col,
			text="显示",
			style="BatchTool.TButton",
			width=side_btn_width,
			command=self.toggle_api_key_visibility,
		)
		self.show_key_btn.grid(row=1, column=0, sticky="w", pady=4)
		self._attach_tooltip(self.show_key_btn, "切换 API_KEY 明文/掩码显示。")

		bottom = ttk.Frame(parent, style="TabCard.TFrame", padding=16)
		bottom.pack(fill=tk.BOTH, expand=True)

		ttk.Label(bottom, text="运行日志", style="SectionTitle.TLabel").pack(anchor="w", pady=(0, 8))
		self.log_text = scrolledtext.ScrolledText(bottom, wrap=tk.WORD, font=("Microsoft YaHei UI", 10))
		self.log_text.pack(fill=tk.BOTH, expand=True)
		self.log_text.configure(
			bg="#1E1F1C",
			fg=self.colors["text"],
			insertbackground=self.colors["accent"],
			selectbackground=self.colors["select"],
			selectforeground=self.colors["text"],
			relief="flat",
			borderwidth=0,
		)
		self.log_text.configure(state=tk.DISABLED)

		self.log_menu = tk.Menu(self.root, tearoff=0)
		self.log_menu.add_command(label="复制日志", command=self._copy_log_text)
		self.log_menu.add_command(label="清除日志", command=self._clear_log_text)
		self.log_text.bind("<Button-3>", self._show_log_context_menu)
		self._refresh_extracted_count()

	def _build_data_stats_panel(self, parent: ttk.Frame) -> None:
		ttk.Label(parent, textvariable=self.extracted_count_var, style="Field.TLabel").grid(
			row=0,
			column=0,
			columnspan=5,
			sticky="w",
			pady=(0, 4),
		)

		ttk.Label(parent, text="开始", style="Field.TLabel").grid(row=1, column=0, sticky="w", padx=(0, 4), pady=2)
		if TkDateEntry is not None:
			self.start_date_entry = TkDateEntry(
				parent,
				width=12,
				date_pattern="yyyy-mm-dd",
				textvariable=self.start_date_var,
				firstweekday="sunday",
				mindate=date(DATE_PICKER_MIN_YEAR, 1, 1),
				maxdate=date(DATE_PICKER_MAX_YEAR, 12, 31),
				weekendbackground="#56564D",
				weekendforeground="#ECECE3",
			)
			start_default = self._parse_date_str(self.start_date_var.get()) or (date.today() - timedelta(days=365))
			self.start_date_entry.set_date(start_default)
		else:
			self.start_date_entry = ttk.Entry(parent, textvariable=self.start_date_var, width=12, style="Monokai.TEntry")
		self.start_date_entry.grid(row=1, column=1, sticky="w", pady=2, ipady=2)

		ttk.Label(parent, text="结束", style="Field.TLabel").grid(row=1, column=2, sticky="w", padx=(8, 4), pady=2)
		if TkDateEntry is not None:
			self.end_date_entry = TkDateEntry(
				parent,
				width=12,
				date_pattern="yyyy-mm-dd",
				textvariable=self.end_date_var,
				firstweekday="sunday",
				mindate=date(DATE_PICKER_MIN_YEAR, 1, 1),
				maxdate=date(DATE_PICKER_MAX_YEAR, 12, 31),
				weekendbackground="#56564D",
				weekendforeground="#ECECE3",
			)
			end_default = self._parse_date_str(self.end_date_var.get()) or date.today()
			self.end_date_entry.set_date(end_default)
			self.start_date_var.set((end_default - timedelta(days=365)).strftime("%Y-%m-%d"))
			self.start_date_entry.set_date(end_default - timedelta(days=365))
		else:
			self.end_date_entry = ttk.Entry(parent, textvariable=self.end_date_var, width=12, style="Monokai.TEntry")
		self.end_date_entry.grid(row=1, column=3, sticky="w", pady=2, ipady=2)

		estimate_btn = ttk.Button(parent, text="估算Token", style="BatchAssist.TButton", width=13, command=self.estimate_tokens_for_selected_range)
		estimate_btn.grid(row=0, column=5, sticky="e", padx=(8, 0), pady=(0, 4))
		refresh_btn = ttk.Button(parent, text="刷新计数", style="Orange.TButton", width=13, command=self._refresh_extracted_count)
		refresh_btn.grid(row=1, column=5, sticky="e", padx=(8, 0), pady=(4, 0))

		if TkDateEntry is not None and hasattr(self.start_date_entry, "drop_down") and hasattr(self.end_date_entry, "drop_down"):
			self.start_date_entry.bind("<Button-1>", lambda _e: self.root.after_idle(self.start_date_entry.drop_down), add="+")
			self.end_date_entry.bind("<Button-1>", lambda _e: self.root.after_idle(self.end_date_entry.drop_down), add="+")
			self.start_date_entry.bind("<<DateEntrySelected>>", lambda _e: self._refresh_extracted_count(), add="+")
			self.end_date_entry.bind("<<DateEntrySelected>>", lambda _e: self._refresh_extracted_count(), add="+")
		else:
			self.start_date_entry.bind("<FocusIn>", lambda _e: self._open_date_picker(self.start_date_var, self.start_date_entry), add="+")
			self.end_date_entry.bind("<FocusIn>", lambda _e: self._open_date_picker(self.end_date_var, self.end_date_entry), add="+")
			self.start_date_entry.bind("<Button-1>", lambda _e: self.root.after_idle(lambda: self._open_date_picker(self.start_date_var, self.start_date_entry)), add="+")
			self.end_date_entry.bind("<Button-1>", lambda _e: self.root.after_idle(lambda: self._open_date_picker(self.end_date_var, self.end_date_entry)), add="+")
			self.start_date_entry.bind("<FocusOut>", lambda _e: self._schedule_date_picker_focus_check(), add="+")
			self.end_date_entry.bind("<FocusOut>", lambda _e: self._schedule_date_picker_focus_check(), add="+")
		self._attach_tooltip(self.start_date_entry, "点击输入框可弹出日历选择开始日期。")
		self._attach_tooltip(self.end_date_entry, "点击输入框可弹出日历选择结束日期。")
		self._attach_tooltip(refresh_btn, "按文件名日期重新统计 extracted_dir 文件数量。")
		self._attach_tooltip(estimate_btn, "基于日期范围估算 DeepSeek tokenizer token 数。")

	def _build_preview_tab(self, parent: ttk.Frame) -> None:
		container = ttk.Frame(parent, style="TabCard.TFrame", padding=12)
		container.pack(fill=tk.BOTH, expand=True)

		top_bar = ttk.Frame(container, style="TabCard.TFrame")
		top_bar.pack(fill=tk.X, pady=(0, 8))
		ttk.Label(top_bar, text="搜索", style="Field.TLabel").pack(side=tk.LEFT, padx=(0, 8))
		search_entry = ttk.Entry(top_bar, textvariable=self.preview_search_var, width=28)
		search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
		search_entry.bind("<Return>", self._on_preview_search_enter)
		self.keyword_search_btn = ttk.Button(top_bar, text="关键词搜索", style="Orange.TButton", command=self.run_keyword_search)
		self.keyword_search_btn.pack(side=tk.LEFT, padx=(8, 6))
		self._attach_tooltip(self.keyword_search_btn, "按关键词匹配文件名和内容并显示结果。")
		self.vector_search_btn = ttk.Button(top_bar, text="向量搜索", style="Blue.TButton", command=self.run_vector_search)
		self.vector_search_btn.pack(side=tk.LEFT, padx=(0, 6))
		self._attach_tooltip(self.vector_search_btn, "使用向量相似度检索相关文档片段。")
		self.refresh_docs_btn = ttk.Button(top_bar, text="刷新目录", style="Green.TButton", command=self._refresh_documents_and_prune)
		self.refresh_docs_btn.pack(side=tk.RIGHT)
		self._attach_tooltip(self.refresh_docs_btn, "重新扫描 documents 目录并刷新树形视图，同时清理缺失文档的失效向量项。")

		body = tk.PanedWindow(
			container,
			orient=tk.HORIZONTAL,
			bg=self.colors["panel"],
			sashwidth=8,
			sashrelief=tk.RAISED,
		)
		body.pack(fill=tk.BOTH, expand=True)

		left = ttk.Frame(body, style="TabCard.TFrame")
		right = ttk.Frame(body, style="TabCard.TFrame")
		body.add(left, minsize=140, width=340)
		body.add(right, minsize=420, width=760)

		tree_wrap = ttk.Frame(left, style="TabCard.TFrame")
		tree_wrap.pack(fill=tk.BOTH, expand=True)
		tree_wrap.columnconfigure(0, weight=1)
		tree_wrap.rowconfigure(0, weight=1)
		self.docs_tree = ttk.Treeview(tree_wrap, style="Monokai.Treeview", show="tree")
		tree_scroll = ttk.Scrollbar(tree_wrap, orient=tk.VERTICAL, command=self.docs_tree.yview)
		tree_xscroll = ttk.Scrollbar(tree_wrap, orient=tk.HORIZONTAL, command=self.docs_tree.xview)
		self.docs_tree.configure(yscrollcommand=tree_scroll.set, xscrollcommand=tree_xscroll.set)
		self.docs_tree.column("#0", width=340, minwidth=130, stretch=False)
		self.docs_tree.grid(row=0, column=0, sticky="nsew")
		tree_scroll.grid(row=0, column=1, sticky="ns")
		tree_xscroll.grid(row=1, column=0, sticky="ew")
		self.docs_tree.bind("<<TreeviewSelect>>", self._on_preview_tree_select)
		self.docs_menu = tk.Menu(self.root, tearoff=0)
		self.docs_menu.add_command(label="重命名", command=self._rename_selected_doc_node)
		self.docs_menu.add_command(label="打开文件位置", command=self._open_selected_doc_location)
		self.docs_tree.bind("<Button-3>", self._show_docs_context_menu)

		self.preview_text = scrolledtext.ScrolledText(right, wrap=tk.WORD, font=("Microsoft YaHei UI", 10))
		self.preview_text.pack(fill=tk.BOTH, expand=True)
		self.preview_text.configure(
			bg="#1E1F1C",
			fg=self.colors["text"],
			insertbackground=self.colors["accent"],
			selectbackground=self.colors["select"],
			selectforeground=self.colors["text"],
			relief="flat",
			borderwidth=0,
		)
		self.preview_text.configure(state=tk.DISABLED)
		self._configure_markdown_tags(self.preview_text)
		self._refresh_documents_tree()

	def _build_info_tab(self, parent: ttk.Frame) -> None:
		container = ttk.Frame(parent, style="TabCard.TFrame", padding=12)
		container.pack(fill=tk.BOTH, expand=True)

		top_bar = ttk.Frame(container, style="TabCard.TFrame")
		top_bar.pack(fill=tk.X, pady=(0, 8))
		ttk.Label(top_bar, text="Info", style="SectionTitle.TLabel").pack(side=tk.LEFT)
		self.refresh_readme_btn = ttk.Button(top_bar, text="刷新说明", style="Action.TButton", command=self._load_info_content)
		self.refresh_readme_btn.pack(side=tk.RIGHT)
		self._attach_tooltip(self.refresh_readme_btn, "重新生成并渲染 GUI 使用说明。")

		self.readme_text = scrolledtext.ScrolledText(container, wrap=tk.WORD, font=("Microsoft YaHei UI", 10))
		self.readme_text.pack(fill=tk.BOTH, expand=True)
		self.readme_text.configure(
			bg="#1E1F1C",
			fg=self.colors["text"],
			insertbackground=self.colors["accent"],
			selectbackground=self.colors["select"],
			selectforeground=self.colors["text"],
			relief="flat",
			borderwidth=0,
		)
		self.readme_text.configure(state=tk.DISABLED)
		self._configure_markdown_tags(self.readme_text)
		self._load_info_content()

	def _build_rag_qa_tab(self, parent: ttk.Frame) -> None:
		container = ttk.Frame(parent, style="TabCard.TFrame", padding=12)
		container.pack(fill=tk.BOTH, expand=True)
		self._refresh_rag_model_label()

		top_bar = ttk.Frame(container, style="TabCard.TFrame")
		top_bar.pack(fill=tk.X, pady=(0, 8))
		ttk.Label(top_bar, text="RAG Q&A", style="SectionTitle.TLabel").pack(side=tk.LEFT)
		ttk.Label(top_bar, text="使用本地大模型，基于知识库回答问题", style="Caption.TLabel").pack(side=tk.LEFT, padx=(10, 0))
		ttk.Label(top_bar, textvariable=self.rag_model_label_var, style="Caption.TLabel").pack(side=tk.RIGHT)

		body = ttk.Frame(container, style="TabCard.TFrame")
		body.pack(fill=tk.BOTH, expand=True)
		body.columnconfigure(0, weight=0)
		body.columnconfigure(1, weight=1)
		body.rowconfigure(0, weight=1)

		left = ttk.Frame(body, style="Card.TFrame", padding=8)
		left.grid(row=0, column=0, sticky="ns", padx=(0, 8))
		left.configure(width=220)
		left.grid_propagate(False)
		tk.Label(
			left,
			text="历史会话",
			bg=self.colors["panel"],
			fg=self.colors["cyan"],
			font=("Microsoft YaHei UI", 12, "bold"),
		).pack(anchor="w", pady=(0, 6))

		self.rag_session_listbox = tk.Listbox(
			left,
			activestyle="none",
			exportselection=False,
			bg="#1E1F1C",
			fg=self.colors["text"],
			selectbackground=self.colors["select"],
			selectforeground=self.colors["text"],
			relief="flat",
			borderwidth=0,
			highlightthickness=0,
			font=("Microsoft YaHei UI", 10),
		)
		self.rag_session_listbox.pack(fill=tk.BOTH, expand=True)
		self.rag_session_listbox.bind("<<ListboxSelect>>", self._on_rag_session_select, add="+")
		self.rag_session_listbox.bind("<Button-3>", self._show_rag_session_context_menu, add="+")

		self.rag_session_menu = tk.Menu(self.root, tearoff=0)
		self.rag_session_menu.add_command(label="删除会话", command=self._delete_selected_rag_session)

		left_btn_row = ttk.Frame(left, style="Card.TFrame")
		left_btn_row.pack(fill=tk.X, pady=(8, 0))
		ttk.Button(left_btn_row, text="新建会话", style="Action.TButton", command=self._new_rag_session).pack(fill=tk.X)

		right = ttk.Frame(body, style="Card.TFrame", padding=8)
		right.grid(row=0, column=1, sticky="nsew")
		right.columnconfigure(0, weight=1)
		right.rowconfigure(0, weight=1)
		right.rowconfigure(1, weight=0)

		self.rag_chat_text = scrolledtext.ScrolledText(right, wrap=tk.WORD, font=("Microsoft YaHei UI", 10))
		self.rag_chat_text.grid(row=0, column=0, sticky="nsew")
		self.rag_chat_text.configure(
			bg="#1E1F1C",
			fg=self.colors["text"],
			insertbackground=self.colors["accent"],
			selectbackground=self.colors["select"],
			selectforeground=self.colors["text"],
			relief="flat",
			borderwidth=0,
		)
		self.rag_chat_text.configure(state=tk.DISABLED)
		self._configure_markdown_tags(self.rag_chat_text)

		mode_bar = ttk.Frame(right, style="Card.TFrame")
		mode_bar.grid(row=1, column=0, sticky="ew", pady=(8, 0))
		ttk.Label(mode_bar, text="回答模式", style="Caption.TLabel").pack(side=tk.LEFT, padx=(2, 10))

		self.rag_mode_local_btn = tk.Button(
			mode_bar,
			text="本地模型",
			command=lambda: self._set_rag_answer_mode("local"),
			font=("Microsoft YaHei UI", 9, "bold"),
			padx=10,
			pady=3,
			relief="flat",
			borderwidth=0,
		)
		self.rag_mode_local_btn.pack(side=tk.LEFT, padx=(0, 6))

		self.rag_mode_deepseek_btn = tk.Button(
			mode_bar,
			text="DeepSeek",
			command=lambda: self._set_rag_answer_mode("deepseek"),
			font=("Microsoft YaHei UI", 9, "bold"),
			padx=10,
			pady=3,
			relief="flat",
			borderwidth=0,
		)
		self.rag_mode_deepseek_btn.pack(side=tk.LEFT, padx=(0, 6))

		self.rag_mode_reasoner_btn = tk.Button(
			mode_bar,
			text="深度思考",
			command=lambda: self._set_rag_answer_mode("reasoner"),
			font=("Microsoft YaHei UI", 9, "bold"),
			padx=10,
			pady=3,
			relief="flat",
			borderwidth=0,
		)
		self.rag_mode_reasoner_btn.pack(side=tk.LEFT)
		self._update_rag_mode_buttons()

		input_wrap = ttk.Frame(right, style="Card.TFrame")
		input_wrap.grid(row=2, column=0, sticky="ew", pady=(8, 0))
		input_wrap.columnconfigure(0, weight=1)
		input_wrap.columnconfigure(1, weight=0)

		self.rag_input_text = tk.Text(
			input_wrap,
			height=4,
			wrap=tk.WORD,
			bg="#1E1F1C",
			fg=self.colors["text"],
			insertbackground=self.colors["accent"],
			selectbackground=self.colors["select"],
			selectforeground=self.colors["text"],
			relief="flat",
			borderwidth=0,
			font=("Microsoft YaHei UI", 10),
		)
		self.rag_input_text.grid(row=0, column=0, sticky="ew")
		self.rag_input_text.bind("<Return>", self._on_rag_input_enter, add="+")
		self.rag_input_text.bind("<Shift-Return>", self._on_rag_input_shift_enter, add="+")

		right_btn_col = ttk.Frame(input_wrap, style="Card.TFrame")
		right_btn_col.grid(row=0, column=1, sticky="ns", padx=(8, 0))
		self.rag_local_answer_btn = ttk.Button(right_btn_col, text="本地回答", style="Action.TButton", command=self.run_rag_qa_local_only, width=8)
		self.rag_local_answer_btn.pack(fill=tk.X)
		self._attach_tooltip(self.rag_local_answer_btn, "仅使用 5 条本地资料回答。")
		self.rag_send_btn = ttk.Button(right_btn_col, text="联网搜索", style="Blue.TButton", command=self.run_rag_qa_hybrid, width=8)
		self.rag_send_btn.pack(fill=tk.X, pady=(6, 0))
		self._attach_tooltip(self.rag_send_btn, "使用 3 条本地资料 + 3 条联网结果回答。")
		self.rag_abort_btn = ttk.Button(right_btn_col, text="中止", style="Action.TButton", command=self._abort_rag_qa, width=8)
		self.rag_abort_btn.pack(fill=tk.X, pady=(6, 0))
		self._attach_tooltip(self.rag_abort_btn, "中止当前问答任务。")
		self.rag_abort_btn.configure(state=tk.DISABLED)

		self._load_rag_sessions()
		if not self.rag_sessions:
			self._new_rag_session()
		else:
			self.rag_current_session_idx = 0
			self._refresh_rag_session_list()
			self._render_rag_current_session()

	def _get_default_rag_system_message(self) -> str:
		return "欢迎使用 RAG Q&A 助手。本助手基于本地向量知识库回答问题。请输入问题并按 Enter 发送，Shift+Enter 换行。"

	def _new_rag_session(self) -> None:
		session_title = "新会话"
		now = datetime.now().isoformat(timespec="seconds")
		session = {
			"id": str(uuid4()),
			"title": session_title,
			"created_at": now,
			"updated_at": now,
			"title_locked": False,
			"messages": [("系统", self._get_default_rag_system_message())],
		}
		self.rag_sessions.insert(0, session)
		self.rag_current_session_idx = 0
		self._refresh_rag_session_list()
		self._render_rag_current_session()
		self._persist_rag_session(self.rag_current_session_idx)

	def _refresh_rag_session_list(self) -> None:
		if self.rag_session_listbox is None:
			return
		self.rag_session_listbox.delete(0, tk.END)
		for session in self.rag_sessions:
			title = str(session.get("title", "会话"))
			self.rag_session_listbox.insert(tk.END, title)
		if self.rag_current_session_idx >= 0 and self.rag_current_session_idx < len(self.rag_sessions):
			self.rag_session_listbox.selection_clear(0, tk.END)
			self.rag_session_listbox.selection_set(self.rag_current_session_idx)
			self.rag_session_listbox.activate(self.rag_current_session_idx)

	def _on_rag_session_select(self, _event: tk.Event) -> None:
		if self.rag_session_listbox is None:
			return
		selection = self.rag_session_listbox.curselection()
		if not selection:
			return
		self.rag_current_session_idx = int(selection[0])
		self._render_rag_current_session()

	def _render_rag_current_session(self) -> None:
		if self.rag_chat_text is None:
			return
		markdown_lines: list[str] = []
		if 0 <= self.rag_current_session_idx < len(self.rag_sessions):
			messages = self.rag_sessions[self.rag_current_session_idx].get("messages", [])
			if isinstance(messages, list):
				for role, text in messages:
					role_text = str(role).strip() or "助手"
					content = str(text).strip()
					
					# Extract think blocks if assistant message
					if role_text == "助手":
						thoughts, clean_answer = self._extract_think_blocks(content)
						for thought in thoughts:
							markdown_lines.append("MSGBLOCK_START:系统")
							markdown_lines.append("THINK_BLOCK_START")
							markdown_lines.append(thought)
							markdown_lines.append("THINK_BLOCK_END")
							markdown_lines.append("MSGBLOCK_END")
							markdown_lines.append("")
						content = clean_answer
					
					content = self._normalize_markdown_for_chat(content)
					# Use special markers to indicate message blocks
					markdown_lines.append(f"MSGBLOCK_START:{role_text}")
					# Show role as heading for user/assistant, but not for system
					if role_text != "系统":
						markdown_lines.append(f"### {role_text}")
					if content:
						markdown_lines.append(content)
					markdown_lines.append("MSGBLOCK_END")
					markdown_lines.append("")
		self._set_markdown_text(self.rag_chat_text, "\n".join(markdown_lines).strip())
		self.rag_chat_text.see(tk.END)

	def _normalize_markdown_for_chat(self, text: str) -> str:
		if not text:
			return ""
		parts = text.split("```")
		for idx in range(0, len(parts), 2):
			chunk = parts[idx]
			# Add newlines before headings, lists, but preserve horizontal rules (---, ***, ___)
			chunk = re.sub(r"([^\n])\s*(#{1,6}\s+)", r"\1\n\2", chunk)
			# Only add newline before list markers if not part of horizontal rule
			chunk = re.sub(r"([^\n-*_])\s*([-*+]\s+)", r"\1\n\2", chunk)
			chunk = re.sub(r"([^\n])\s*(\d+\.\s+)", r"\1\n\2", chunk)
			parts[idx] = chunk
		return "```".join(parts)

	def _append_rag_chat(self, role: str, text: str, session_idx: int | None = None) -> None:
		if session_idx is None:
			session_idx = self.rag_current_session_idx
		if session_idx < 0 or session_idx >= len(self.rag_sessions):
			return
		messages = self.rag_sessions[session_idx].setdefault("messages", [])
		if isinstance(messages, list):
			messages.append((role, text.strip()))
		self._persist_rag_session(session_idx)
		if session_idx == self.rag_current_session_idx:
			self._render_rag_current_session()

	def _update_last_rag_message(self, role: str, text: str, session_idx: int | None = None) -> None:
		"""Update the last message in the session if it matches the given role."""
		if session_idx is None:
			session_idx = self.rag_current_session_idx
		if session_idx < 0 or session_idx >= len(self.rag_sessions):
			return
		messages = self.rag_sessions[session_idx].get("messages", [])
		if isinstance(messages, list) and messages and messages[-1][0] == role:
			messages[-1] = (role, text.strip())
			if session_idx == self.rag_current_session_idx:
				self._render_rag_current_session()

	def _clear_rag_chat(self) -> None:
		if self.rag_current_session_idx < 0 or self.rag_current_session_idx >= len(self.rag_sessions):
			return
		self.rag_sessions[self.rag_current_session_idx]["messages"] = [("系统", "当前会话已清空。")]
		self._persist_rag_session(self.rag_current_session_idx)
		self._render_rag_current_session()

	def _sanitize_session_title(self, title: str) -> str:
		return sanitize_session_title(title)

	def _derive_local_session_title(self, question: str, answer: str, max_len: int = 15) -> str:
		return derive_local_session_title(question=question, answer=answer, max_len=max_len)

	def _sanitize_filename_part(self, text: str) -> str:
		return sanitize_filename_part(text)

	def _extract_think_blocks(self, text: str) -> tuple[list[str], str]:
		"""Extract <think> blocks from text and return (thoughts, clean_answer)."""
		raw = str(text or "")
		thoughts: list[str] = []
		
		def normalize_think(inner: str) -> str:
			value = str(inner or "").replace("\r\n", "\n")
			value = re.sub(r"\[\s*Empty\s+Line\s*\]", "\n", value, flags=re.IGNORECASE)
			value = re.sub(r"\n{3,}", "\n\n", value)
			return value.strip()
		
		answer = re.sub(
			r"<think>([\s\S]*?)</think>",
			lambda m: (
				thoughts.append(normalize_think(m.group(1))),
				"\n",
			)[1],
			raw,
			flags=re.IGNORECASE,
		)
		clean_answer = normalize_think(answer)
		return ([t for t in thoughts if t], clean_answer)

	def _session_file_name(self, session: dict[str, object]) -> str:
		return session_file_name(session)

	def _build_rag_session_markdown(self, session: dict[str, object]) -> str:
		return build_rag_session_markdown(session)

	def _parse_rag_session_markdown(self, text: str) -> tuple[str, str, list[tuple[str, str]]]:
		return parse_rag_session_markdown(
			text=text,
			fallback_system_message=self._get_default_rag_system_message(),
		)

	def _load_rag_sessions(self) -> None:
		self.rag_sessions = []
		for path in self._iter_rag_session_json_files():
			normalized = self._load_rag_session_json_file(path)
			if normalized is not None:
				self.rag_sessions.append(normalized)

		if self.rag_sessions:
			self.rag_sessions.sort(key=lambda s: str(s.get("updated_at", "")), reverse=True)
			return

		# Legacy compatibility: migrate old aggregate json when per-session files are absent.
		payload = self._load_legacy_web_sessions_payload()
		sessions = payload.get("sessions", []) if isinstance(payload, dict) else []
		if isinstance(sessions, list):
			for raw in sessions:
				normalized = self._normalize_session_for_gui(raw)
				if normalized is not None:
					self.rag_sessions.append(normalized)
		if self.rag_sessions:
			self.rag_sessions.sort(key=lambda s: str(s.get("updated_at", "")), reverse=True)
			self._save_all_rag_sessions()
			return

		# Legacy compatibility: import old markdown sessions once when JSON is empty.
		migrated = self._migrate_legacy_md_sessions()
		if migrated:
			self.rag_sessions = migrated
			self.rag_sessions.sort(key=lambda s: str(s.get("updated_at", "")), reverse=True)
			self._save_all_rag_sessions()

	def _persist_rag_session(self, session_idx: int) -> None:
		if session_idx < 0 or session_idx >= len(self.rag_sessions):
			return

		session = self.rag_sessions[session_idx]
		now = datetime.now().isoformat(timespec="seconds")
		session.setdefault("id", str(uuid4()))
		session.setdefault("created_at", now)
		session["updated_at"] = now
		self._save_rag_session_json_file(session)

	def _set_rag_session_title(self, session_idx: int, title: str, lock: bool) -> None:
		if session_idx < 0 or session_idx >= len(self.rag_sessions):
			return

		session = self.rag_sessions[session_idx]
		new_title = self._sanitize_session_title(title)
		if not new_title:
			return
		session["title"] = new_title
		session["title_locked"] = bool(lock)

		self._persist_rag_session(session_idx)
		self._refresh_rag_session_list()

	def _show_rag_session_context_menu(self, event: tk.Event) -> str:
		if self.rag_session_listbox is None or self.rag_session_menu is None:
			return "break"
		index = self.rag_session_listbox.nearest(event.y)
		if index < 0 or index >= len(self.rag_sessions):
			return "break"
		self.rag_session_listbox.selection_clear(0, tk.END)
		self.rag_session_listbox.selection_set(index)
		self.rag_session_listbox.activate(index)
		self.rag_current_session_idx = index
		self._render_rag_current_session()
		try:
			self.rag_session_menu.tk_popup(event.x_root, event.y_root)
		finally:
			self.rag_session_menu.grab_release()
		return "break"

	def _delete_selected_rag_session(self) -> None:
		idx = self.rag_current_session_idx
		if idx < 0 or idx >= len(self.rag_sessions):
			return

		session = self.rag_sessions[idx]
		del self.rag_sessions[idx]
		sid = str(session.get("id", "")).strip() if isinstance(session, dict) else ""
		if sid:
			self._delete_rag_session_json_file(sid)
		if not self.rag_sessions:
			self._new_rag_session()
			return

		self.rag_current_session_idx = min(idx, len(self.rag_sessions) - 1)
		self._refresh_rag_session_list()
		self._render_rag_current_session()

	def _session_json_path(self, session_id: str) -> Path:
		return self.rag_sessions_dir / f"{self.rag_session_file_prefix}{session_id}.json"

	def _iter_rag_session_json_files(self) -> list[Path]:
		if not self.rag_sessions_dir.exists():
			return []
		return sorted(self.rag_sessions_dir.glob(f"{self.rag_session_file_prefix}*.json"), key=lambda p: p.name.lower())

	def _load_legacy_web_sessions_payload(self) -> dict[str, object]:
		if not self.legacy_web_sessions_file.exists():
			return {"sessions": []}
		try:
			data = json.loads(self.legacy_web_sessions_file.read_text(encoding="utf-8"))
			if isinstance(data, dict):
				return data
		except Exception:
			pass
		return {"sessions": []}

	def _load_rag_session_json_file(self, path: Path) -> dict[str, object] | None:
		try:
			data = json.loads(path.read_text(encoding="utf-8"))
		except Exception:
			return None
		return self._normalize_session_for_gui(data)

	def _save_rag_session_json_file(self, session: object) -> None:
		item = self._serialize_session_for_json(session)
		if item is None:
			return
		sid = str(item.get("id", "")).strip()
		if not sid:
			return
		path = self._session_json_path(sid)
		try:
			path.parent.mkdir(parents=True, exist_ok=True)
			path.write_text(json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8")
		except Exception as exc:  # noqa: BLE001
			self._append_log(f"保存会话失败：{exc}")

	def _delete_rag_session_json_file(self, session_id: str) -> None:
		if not session_id:
			return
		path = self._session_json_path(session_id)
		try:
			path.unlink(missing_ok=True)
		except Exception as exc:  # noqa: BLE001
			self._append_log(f"删除会话文件失败：{exc}")

	def _load_memory_context_for_session(self, session_idx: int) -> str:
		if session_idx < 0 or session_idx >= len(self.rag_sessions):
			return ""
		session = self.rag_sessions[session_idx]
		sid = str(session.get("id", "")).strip() if isinstance(session, dict) else ""
		if not sid:
			return ""
		memory_file = self.rag_sessions_dir / "_memory" / f"memory_{sid}.json"
		if not memory_file.exists():
			return ""
		try:
			text = memory_file.read_text(encoding="utf-8")
		except Exception:
			return ""
		# Keep command line argument size bounded.
		return text[:4000]

	def _save_all_rag_sessions(self) -> None:
		serialized_ids: set[str] = set()
		for session in self.rag_sessions:
			item = self._serialize_session_for_json(session)
			if item is not None:
				sid = str(item.get("id", "")).strip()
				if sid:
					serialized_ids.add(sid)
					self._save_rag_session_json_file(item)
		for path in self._iter_rag_session_json_files():
			stem = path.stem
			if not stem.startswith(self.rag_session_file_prefix):
				continue
			sid = stem[len(self.rag_session_file_prefix):]
			if sid and sid not in serialized_ids:
				try:
					path.unlink(missing_ok=True)
				except Exception:
					pass

	def _normalize_session_for_gui(self, raw: object) -> dict[str, object] | None:
		if not isinstance(raw, dict):
			return None
		now = datetime.now().isoformat(timespec="seconds")
		title = self._sanitize_session_title(str(raw.get("title", "新会话")))
		created_at = str(raw.get("created_at", "")).strip() or now
		updated_at = str(raw.get("updated_at", "")).strip() or created_at
		messages_raw = raw.get("messages", [])
		messages = self._normalize_messages_for_gui(messages_raw)
		if not messages:
			messages = [("系统", self._get_default_rag_system_message())]
		return {
			"id": str(raw.get("id", "")).strip() or str(uuid4()),
			"title": title,
			"created_at": created_at,
			"updated_at": updated_at,
			"title_locked": bool(raw.get("title_locked", False)),
			"messages": messages,
		}

	def _normalize_messages_for_gui(self, messages_raw: object) -> list[tuple[str, str]]:
		result: list[tuple[str, str]] = []
		if not isinstance(messages_raw, list):
			return result
		for item in messages_raw:
			role = ""
			text = ""
			if isinstance(item, dict):
				role = str(item.get("role", "")).strip()
				text = str(item.get("text", "")).strip()
			elif isinstance(item, (list, tuple)) and len(item) >= 2:
				role = str(item[0]).strip()
				text = str(item[1]).strip()
			if role and text:
				result.append((role, text))
		return result

	def _serialize_session_for_json(self, session: object) -> dict[str, object] | None:
		if not isinstance(session, dict):
			return None
		now = datetime.now().isoformat(timespec="seconds")
		created_at = str(session.get("created_at", "")).strip() or now
		updated_at = str(session.get("updated_at", "")).strip() or created_at
		messages = self._normalize_messages_for_gui(session.get("messages", []))
		if not messages:
			messages = [("系统", self._get_default_rag_system_message())]
		return {
			"id": str(session.get("id", "")).strip() or str(uuid4()),
			"title": self._sanitize_session_title(str(session.get("title", "新会话"))),
			"created_at": created_at,
			"updated_at": updated_at,
			"title_locked": bool(session.get("title_locked", False)),
			"messages": [{"role": role, "text": text} for role, text in messages],
		}

	def _migrate_legacy_md_sessions(self) -> list[dict[str, object]]:
		migrated: list[dict[str, object]] = []
		for path in sorted(self.rag_sessions_dir.glob("*.md"), key=lambda p: p.name.lower(), reverse=True):
			try:
				text = path.read_text(encoding="utf-8", errors="ignore")
				title, created_at, messages = self._parse_rag_session_markdown(text)
			except Exception:
				continue
			normalized = self._normalize_session_for_gui(
				{
					"id": str(uuid4()),
					"title": title,
					"created_at": created_at,
					"updated_at": created_at,
					"title_locked": True,
					"messages": [{"role": role, "text": content} for role, content in messages],
				}
			)
			if normalized is not None:
				migrated.append(normalized)
		return migrated

	def _on_rag_input_enter(self, _event: tk.Event) -> str:
		self.run_rag_qa("hybrid")
		return "break"

	def _on_rag_input_shift_enter(self, _event: tk.Event) -> None:
		# Keep default Text behavior so Shift+Enter inserts a newline.
		return None

	def _set_rag_qa_running(self, running: bool) -> None:
		self.rag_qa_running = running
		if self.rag_local_answer_btn is not None:
			self.rag_local_answer_btn.configure(state=tk.DISABLED if running else tk.NORMAL)
		if self.rag_send_btn is not None:
			self.rag_send_btn.configure(state=tk.DISABLED if running else tk.NORMAL)
		if self.rag_abort_btn is not None:
			self.rag_abort_btn.configure(state=tk.NORMAL if running else tk.DISABLED)
		if self.rag_input_text is not None:
			self.rag_input_text.configure(state=tk.DISABLED if running else tk.NORMAL)
		if self.rag_mode_local_btn is not None:
			self.rag_mode_local_btn.configure(state=tk.DISABLED if running else tk.NORMAL)
		if self.rag_mode_deepseek_btn is not None:
			self.rag_mode_deepseek_btn.configure(state=tk.DISABLED if running else tk.NORMAL)
		if self.rag_mode_reasoner_btn is not None:
			self.rag_mode_reasoner_btn.configure(state=tk.DISABLED if running else tk.NORMAL)

	def _set_rag_answer_mode(self, mode: str) -> None:
		selected = mode if mode in {"local", "deepseek", "reasoner"} else "local"
		self.rag_answer_mode_var.set(selected)
		self._update_rag_mode_buttons()
		self._refresh_rag_model_label()

	def _update_rag_mode_buttons(self) -> None:
		selected = self.rag_answer_mode_var.get().strip().lower() or "local"

		def _apply(button: tk.Button | None, key: str) -> None:
			if button is None:
				return
			is_selected = selected == key
			button.configure(
				bg=self.colors["cyan"] if is_selected else self.colors["panel_soft"],
				fg="#11120F" if is_selected else self.colors["text"],
				activebackground="#79E6FA" if is_selected else self.colors["select"],
				activeforeground="#11120F" if is_selected else self.colors["text"],
			)

		_apply(self.rag_mode_local_btn, "local")
		_apply(self.rag_mode_deepseek_btn, "deepseek")
		_apply(self.rag_mode_reasoner_btn, "reasoner")

	def _resolve_rag_chat_model(self) -> str:
		mode = self.rag_answer_mode_var.get().strip().lower()
		if mode == "deepseek":
			return "deepseek-chat"
		if mode == "reasoner":
			return "deepseek-reasoner"
		return os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model

	def _resolve_local_llm_runtime(self) -> tuple[str, str, str]:
		# Keep GUI local-mode behavior aligned with Web UI local-mode routing.
		url = os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "http://127.0.0.1:1234").strip()
		if url and not re.search(r"/v1/?$", url):
			url = url.rstrip("/") + "/v1"
		model = os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model
		api_key = os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local").strip() or "local"
		return url, model, api_key

	def run_rag_qa(self, search_mode: str = "hybrid") -> None:
		self._refresh_rag_model_label()
		if self.rag_qa_running:
			self._append_log("RAG Q&A 正在进行中，请稍候。")
			return
		if self.is_running:
			self._append_log("已有任务在运行，请等待结束。")
			return

		if self.rag_input_text is None:
			return
		question = self.rag_input_text.get("1.0", tk.END).strip()
		if not question:
			self._append_log("RAG Q&A：请输入问题。")
			return
		# Clear the input box right after submit.
		self.rag_input_text.delete("1.0", tk.END)
		if self.rag_current_session_idx < 0 or self.rag_current_session_idx >= len(self.rag_sessions):
			self._new_rag_session()

		mode = self.rag_answer_mode_var.get().strip().lower() or "local"
		api_key = self._resolve_api_key().strip()
		base_url = self.base_url_var.get().strip()
		model = self._resolve_rag_chat_model()

		if mode == "local":
			base_url, model, api_key = self._resolve_local_llm_runtime()
		if mode in {"deepseek", "reasoner"} and not api_key:
			self._append_log("RAG Q&A：DeepSeek/深度思考模式需要 API_KEY。")
			return
		if not base_url or not model:
			if mode == "local":
				self._append_log("RAG Q&A：本地模式缺少 AI_SUMMARY_LOCAL_LLM_URL 或 AI_SUMMARY_LOCAL_LLM_MODEL。")
			else:
				self._append_log("RAG Q&A：请先填写 API_BASE_URL、MODEL、API_KEY。")
			return

		embed_model = self._resolve_embedding_model_for_rag()
		if not embed_model or not self._is_local_embedding_model(embed_model):
			self._append_log("RAG Q&A：请配置本地 embedding 模型（如 BAAI/bge-base-zh-v1.5）。")
			return

		python_cmd = self._resolve_python_for_modules(["sentence_transformers", "openai"])
		if not python_cmd:
			self._append_log("RAG Q&A：未找到同时可导入 sentence_transformers 与 openai 的 Python 解释器。")
			self._append_log("请设置 AI_SUMMARY_PYTHON 指向正确解释器，或在该解释器安装缺失依赖。")
			return

		self._set_rag_qa_running(True)
		self.rag_cancel_event.clear()
		session_idx = self.rag_current_session_idx
		self._append_rag_chat("用户", question, session_idx=session_idx)
		mode_label = {"local": "本地模型", "deepseek": "DeepSeek", "reasoner": "深度思考"}.get(mode, "本地模型")
		search_label = "本地回答" if str(search_mode).strip().lower() in {"local", "local_only", "local-only"} else "联网搜索"
		# Add initial progress message with placeholder for time tracking.
		self._append_rag_chat("系统", f"[{mode_label} | {search_label}] 正在准备... (已用时: 0s)", session_idx=session_idx)

		thread = threading.Thread(
			target=self._run_rag_qa_worker,
			args=(python_cmd, question, base_url, api_key, model, embed_model, session_idx, mode, search_mode),
			daemon=True,
		)
		thread.start()

	def run_rag_qa_local_only(self) -> None:
		self.run_rag_qa("local_only")

	def run_rag_qa_hybrid(self) -> None:
		self.run_rag_qa("hybrid")

	def _abort_rag_qa(self) -> None:
		if not self.rag_qa_running:
			self._append_log("当前没有正在运行的 Q&A 任务。")
			return
		self.rag_cancel_event.set()
		with self.rag_process_lock:
			proc = self.rag_active_process
		if proc is not None and proc.poll() is None:
			try:
				proc.terminate()
			except Exception:
				pass
		self._append_log("已请求中止当前 RAG Q&A 任务。")

	def _configure_markdown_tags(self, widget: scrolledtext.ScrolledText) -> None:
		widget.tag_configure("h1", font=("Microsoft YaHei UI", 18, "bold"), foreground=self.colors["accent"], spacing1=8, spacing3=6)
		widget.tag_configure("h2", font=("Microsoft YaHei UI", 15, "bold"), foreground=self.colors["cyan"], spacing1=6, spacing3=4)
		widget.tag_configure("h3", font=("Microsoft YaHei UI", 13, "bold"), foreground=self.colors["orange"], spacing1=4, spacing3=3)
		widget.tag_configure("h3_system", font=("Microsoft YaHei UI", 13, "bold"), foreground=self.colors["cyan"], spacing1=4, spacing3=3)
		widget.tag_configure("h3_user", font=("Microsoft YaHei UI", 15, "bold"), foreground=self.colors["cyan"], spacing1=4, spacing3=3)
		widget.tag_configure("h3_assistant", font=("Microsoft YaHei UI", 15, "bold"), foreground=self.colors["orange"], spacing1=4, spacing3=3)
		widget.tag_configure("bullet", lmargin1=14, lmargin2=28)
		widget.tag_configure("quote", foreground="#CFCFC2", lmargin1=18, lmargin2=18)
		widget.tag_configure("quote_bullet", foreground="#CFCFC2", lmargin1=28, lmargin2=40)
		widget.tag_configure("code", font=("Consolas", 10), foreground="#E6DB74", background="#2E2F2A")
		widget.tag_configure("inline_code", font=("Consolas", 10), foreground="#E6DB74", background="#2E2F2A")
		widget.tag_configure("link", foreground=self.colors["cyan"], underline=1)
		widget.tag_configure("bold", font=("Microsoft YaHei UI", 10, "bold"))
		widget.tag_configure("hr", foreground="#75715E", spacing1=4, spacing3=6)
		widget.tag_configure("highlight", background="#F6E27F", foreground="#11120F")
		widget.tag_configure("normal", font=("Microsoft YaHei UI", 10))
		widget.tag_configure("think_label", font=("Microsoft YaHei UI", 9, "bold"), foreground="#9d9d88", spacing1=2, spacing3=1)
		widget.tag_configure("think_content", font=("Microsoft YaHei UI", 9), foreground="#b7b7a4", lmargin1=12, lmargin2=12)
		# Raise priority of role heading tags so they display with correct font size
		widget.tag_raise("h3_user")
		widget.tag_raise("h3_assistant")
		widget.tag_raise("h3_system")

	def _insert_markdown_with_bold(self, widget: scrolledtext.ScrolledText, text: str, base_tags: tuple[str, ...]) -> None:
		parts = re.split(r"(\*\*[^*]+\*\*|`[^`\n]+`|\[(?:[^\[\]]|\[[^\[\]]*\])+]\([^\)]+\))", text)
		for part in parts:
			if not part:
				continue
			if part.startswith("**") and part.endswith("**") and len(part) >= 4:
				widget.insert(tk.END, part[2:-2], base_tags + ("bold",))
			elif part.startswith("`") and part.endswith("`") and len(part) >= 3:
				widget.insert(tk.END, part[1:-1], base_tags + ("inline_code",))
			elif part.startswith("[") and "](" in part and part.endswith(")"):
				match = re.match(r"^\[((?:[^\[\]]|\[[^\[\]]*\])+)]\(([^\)]+)\)$", part)
				if not match:
					widget.insert(tk.END, part, base_tags)
					continue
				label = re.sub(r"\s+", " ", match.group(1)).strip()
				url = str(match.group(2) or "").strip()
				tag_name = self._register_markdown_link(widget, url)
				widget.insert(tk.END, label, base_tags + ("link", tag_name))
			else:
				widget.insert(tk.END, part, base_tags)

	def _register_markdown_link(self, widget: scrolledtext.ScrolledText, url: str) -> str:
		tag_name = f"md_link_{self._markdown_link_seq}"
		self._markdown_link_seq += 1
		widget.tag_bind(tag_name, "<Button-1>", lambda _e, u=url: self._handle_markdown_link(u))
		widget.tag_bind(tag_name, "<Enter>", lambda _e: widget.config(cursor="hand2"))
		widget.tag_bind(tag_name, "<Leave>", lambda _e: widget.config(cursor=""))
		return tag_name

	def _handle_markdown_link(self, url: str) -> None:
		if not url:
			return
		if re.match(r"^https?://", url, flags=re.IGNORECASE):
			try:
				webbrowser.open_new_tab(url)
			except Exception:
				pass
			return
		if url.startswith("doc://"):
			rel_path = unquote(url[len("doc://") :]).strip()
			if re.match(r"^https?://", rel_path, flags=re.IGNORECASE):
				try:
					webbrowser.open_new_tab(rel_path)
				except Exception:
					pass
				return
			self._open_doc_in_preview(rel_path)

	def _open_doc_in_preview(self, relative_path: str) -> None:
		safe_rel = (relative_path or "").replace("\\", "/").strip("/")
		if not safe_rel:
			return
		target = (self.documents_dir / safe_rel).resolve()
		if not str(target).startswith(str(self.documents_dir.resolve())):
			return
		if not target.exists() or not target.is_file():
			self._show_monokai_dialog("文档不存在", f"未找到文档：{safe_rel}", level="warning")
			return

		self.preview_mode = "browse"
		self.preview_search_var.set("")
		self._refresh_documents_tree()
		self._restore_preview_selection_after_tree_refresh(target)
		if self.main_notebook is not None and self.preview_tab_frame is not None:
			self.main_notebook.select(self.preview_tab_frame)

	def _format_local_answer_with_refs(self, answer: str, used_docs: list[dict[str, object]]) -> str:
		text = (answer or "").strip()
		if not text:
			return text

		text = re.sub(r"\[资料\s*(\d+)\]", r"[\1]", text)

		ref_entries: list[tuple[str, str]] = []
		seen_keys: set[str] = set()
		for item in used_docs:
			if not isinstance(item, dict):
				continue
			path = str(item.get("path", "")).strip().replace("\\", "/")
			title = re.sub(r"\s+", " ", str(item.get("title", "")).strip())
			if not path:
				continue
			is_web = bool(re.match(r"^https?://", path, flags=re.IGNORECASE))
			label = title if (is_web and title) else re.sub(r"\s+", " ", path).strip()
			link = path if is_web else f"doc://{quote(path)}"
			key = f"{label}|{link}"
			if key in seen_keys:
				continue
			seen_keys.add(key)
			ref_entries.append((label, link))

		if not ref_entries:
			return text

		if not re.search(r"\[\d+\]", text):
			markers = "".join(f"[{idx}]" for idx in range(1, len(ref_entries) + 1))
			text = f"{text}\n\n参考标注：{markers}"

		lines = ["---", "### 参考资料"]
		for idx, (label, link) in enumerate(ref_entries, start=1):
			lines.append(f"- [{idx}] [{label}]({link})")
		return f"{text}\n\n" + "\n".join(lines)

	def _repair_wrapped_markdown_links(self, markdown_text: str) -> str:
		text = str(markdown_text or "")

		def _merge(match: re.Match[str]) -> str:
			left = re.sub(r"\s+", " ", match.group(1)).strip()
			right = re.sub(r"\s+", " ", match.group(2)).strip()
			url = str(match.group(3) or "").strip()
			return f"[{left} {right}]({url})"

		# Repair links broken by an accidental newline inside the markdown label.
		text = re.sub(
			r"\[([^\n]{1,240})\n([^\n]{1,240})\]\(((?:https?|doc)://[^)\s]+)\)",
			_merge,
			text,
		)
		return text

	def _set_markdown_text(self, widget: scrolledtext.ScrolledText, markdown_text: str) -> None:
		widget.configure(state=tk.NORMAL)
		widget.delete("1.0", tk.END)
		markdown_text = self._repair_wrapped_markdown_links(markdown_text)

		in_code_block = False
		current_msg_role = None  # Track current message block for background
		in_think_block = False
		
		for raw_line in markdown_text.splitlines():
			line = raw_line.rstrip("\n")

			# Handle think block markers
			if line == "THINK_BLOCK_START":
				in_think_block = True
				widget.insert(tk.END, "▼ 思考过程\n", ("think_label",))
				continue
			elif line == "THINK_BLOCK_END":
				in_think_block = False
				widget.insert(tk.END, "\n", ("normal",))
				continue

			# Handle message block markers
			if line.startswith("MSGBLOCK_START:"):
				current_msg_role = line[len("MSGBLOCK_START:"):].strip()
				continue
			elif line == "MSGBLOCK_END":
				current_msg_role = None
				continue

			# Apply system message styling for system role
			base_tags: tuple[str, ...] = ("normal",)
			if current_msg_role == "系统":
				base_tags = ("msg_system", "normal")
			
			# Use think content style for think blocks
			if in_think_block:
				base_tags = ("think_content",)

			if line.strip().startswith("```"):
				in_code_block = not in_code_block
				continue

			if in_code_block:
				widget.insert(tk.END, line + "\n", base_tags + ("code",))
				continue

			if re.match(r"^\s*([-*_])\1{2,}\s*$", line):
				widget.insert(tk.END, "─" * 48 + "\n", ("hr",))
				continue

			if line.startswith("### "):
				heading = line[4:].strip()
				# Use role-specific tag for user/assistant headings
				h3_tag = "h3"
				if current_msg_role == "用户" and heading == "用户":
					h3_tag = "h3_user"
				elif current_msg_role == "助手" and heading == "助手":
					h3_tag = "h3_assistant"
				elif current_msg_role == "系统" and heading == "系统":
					h3_tag = "h3_system"
				self._insert_markdown_with_bold(widget, heading, base_tags + (h3_tag,))
				widget.insert(tk.END, "\n", base_tags + (h3_tag,))
			elif line.startswith("## "):
				self._insert_markdown_with_bold(widget, line[3:], base_tags + ("h2",))
				widget.insert(tk.END, "\n", base_tags + ("h2",))
			elif line.startswith("# "):
				self._insert_markdown_with_bold(widget, line[2:], base_tags + ("h1",))
				widget.insert(tk.END, "\n", base_tags + ("h1",))
			elif line.lstrip().startswith(("- ", "* ")):
				content = line.lstrip()[2:]
				widget.insert(tk.END, "• ", base_tags + ("bullet", "normal"))
				self._insert_markdown_with_bold(widget, content, base_tags + ("bullet", "normal"))
				widget.insert(tk.END, "\n", base_tags + ("bullet", "normal"))
			elif line.startswith(">"):
				quote_content = line.lstrip("> ")
				if quote_content.lstrip().startswith(("- ", "* ")):
					item_content = quote_content.lstrip()[2:]
					widget.insert(tk.END, "• ", base_tags + ("quote_bullet", "normal"))
					self._insert_markdown_with_bold(widget, item_content, base_tags + ("quote_bullet", "normal"))
					widget.insert(tk.END, "\n", base_tags + ("quote_bullet", "normal"))
				else:
					self._insert_markdown_with_bold(widget, quote_content, base_tags + ("quote", "normal"))
					widget.insert(tk.END, "\n", base_tags + ("quote", "normal"))
			else:
				self._insert_markdown_with_bold(widget, line, base_tags + ("normal",))
				widget.insert(tk.END, "\n", base_tags + ("normal",))

		if widget is self.preview_text:
			self._highlight_preview_search_text()

		widget.configure(state=tk.DISABLED)

	def _refresh_documents_tree(self) -> None:
		# Rebuild tree + node-path map from current mode/search state.
		self.docs_tree.delete(*self.docs_tree.get_children())
		self.preview_node_paths.clear()

		if not self.documents_dir.exists():
			return

		query = self.preview_search_var.get().strip().lower()
		if self.preview_mode == "vector":
			# Vector mode renders search hits from cached result rows, not folder traversal.
			root_node = self.docs_tree.insert("", tk.END, text=f"向量搜索结果: {query}", open=True)
			self.preview_node_paths[root_node] = self.documents_dir
			for item in self.vector_search_results:
				file_path_text = str(item.get("file_path", "")).strip()
				topic = str(item.get("topic", "")).strip()
				rel = str(item.get("relative_path", "")).strip()
				score = float(item.get("score", 0.0))
				if not file_path_text and not rel:
					continue
				node_text = f"[{score:.4f}] {topic} | {rel}"
				node = self.docs_tree.insert(root_node, tk.END, text=node_text, open=False)
				# Normalize mixed path formats (absolute/file_path/relative_path) for click preview.
				resolved_path = self._resolve_vector_result_path(file_path_text, rel)
				if resolved_path is not None:
					self.preview_node_paths[node] = resolved_path
		elif query:
			root_node = self.docs_tree.insert("", tk.END, text=f"关键词搜索结果: {query}", open=True)
			self.preview_node_paths[root_node] = self.documents_dir
			for file_path in self._collect_markdown_files(self.documents_dir):
				if self._matches_preview_query(file_path, query):
					rel = file_path.relative_to(self.documents_dir)
					node = self.docs_tree.insert(root_node, tk.END, text=rel.as_posix(), open=False)
					self.preview_node_paths[node] = file_path
		else:
			root_node = self.docs_tree.insert("", tk.END, text="documents", open=True)
			self.preview_node_paths[root_node] = self.documents_dir
			self._insert_tree_nodes(root_node, self.documents_dir)

	def _refresh_documents_and_prune(self) -> None:
		# Keep original UX: refresh tree first so UI response is immediate.
		selected_path = self._get_selected_doc_path()
		self._refresh_documents_tree()
		self._restore_preview_selection_after_tree_refresh(selected_path)
		try:
			# Then prune vector entries whose backing markdown files were deleted.
			stats = prune_stale_index_entries(
				documents_dir=self.documents_dir,
				index_dir=self.vector_index_dir,
				backend="auto",
				dry_run=False,
			)
		except RAGIndexError as exc:
			# Index may be absent before first vector build; treat as non-fatal refresh.
			self._append_log(f"刷新目录：向量清理跳过（{exc}）。")
			return
		except Exception as exc:  # noqa: BLE001
			self._append_log(f"刷新目录：向量清理失败（{exc}）。")
			return

		removed = int(stats.get("removed_documents", 0))
		if removed > 0:
			self._append_log(f"刷新目录：已清理 {removed} 条失效向量项。")
		else:
			self._append_log("刷新目录：未发现失效向量项。")

	def _collect_markdown_files(self, root: Path) -> list[Path]:
		results: list[Path] = []
		for path in root.rglob("*.md"):
			if path.is_file() and path.name != ".gitkeep":
				results.append(path)
		return sorted(results, key=lambda p: p.as_posix().lower())

	def _matches_preview_query(self, file_path: Path, query: str) -> bool:
		rel = file_path.relative_to(self.documents_dir).as_posix().lower()
		if query in rel:
			return True
		try:
			content = file_path.read_text(encoding="utf-8", errors="ignore").lower()
		except Exception:
			return False
		return query in content

	def _insert_tree_nodes(self, parent_node: str, parent_path: Path) -> None:
		for child in sorted(parent_path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
			if child.name == ".gitkeep":
				continue
			if parent_path == self.documents_dir and child.is_dir() and child.name.lower() in {"example", "examples"}:
				continue
			node = self.docs_tree.insert(parent_node, tk.END, text=child.name, open=False)
			self.preview_node_paths[node] = child
			if child.is_dir():
				self._insert_tree_nodes(node, child)

	def _resolve_vector_result_path(self, file_path_text: str, relative_path_text: str) -> Path | None:
		candidates: list[Path] = []

		raw_file_path = file_path_text.strip()
		if raw_file_path:
			raw = Path(raw_file_path)
			if raw.is_absolute():
				candidates.append(raw)
			else:
				candidates.append(self.documents_dir / raw)
				candidates.append(self.workspace_root / raw)

		rel_text = relative_path_text.strip()
		if rel_text:
			rel = Path(rel_text.replace("\\", "/"))
			candidates.append(self.documents_dir / rel)

		for candidate in candidates:
			if candidate.is_file() and candidate.suffix.lower() == ".md":
				return candidate

		if candidates:
			return candidates[0]
		return None

	def _on_preview_tree_select(self, _event: tk.Event) -> None:
		selected = self.docs_tree.selection()
		if not selected:
			return

		path = self.preview_node_paths.get(selected[0])
		if not path:
			return
		if path.is_dir():
			self._set_markdown_text(self.preview_text, self._build_folder_preview_markdown(path))
			return
		if not path.is_file():
			self._set_markdown_text(self.preview_text, f"文件不存在: {path}")
			return
		if path.suffix.lower() != ".md":
			return

		try:
			text = path.read_text(encoding="utf-8")
		except Exception as exc:  # noqa: BLE001
			self._set_markdown_text(self.preview_text, f"读取失败: {exc}")
			return

		self._set_markdown_text(self.preview_text, text)

	def _extract_doc_quick_summary(self, file_path: Path, max_chars: int = 120) -> str:
		try:
			text = file_path.read_text(encoding="utf-8", errors="ignore")
		except Exception:
			return ""

		for line in text.splitlines():
			stripped = line.strip()
			if not stripped:
				continue
			if stripped.startswith("#"):
				return stripped.lstrip("#").strip()[:max_chars]
			if stripped.startswith(">"):
				continue
			return stripped[:max_chars]
		return ""

	def _folder_theme_hint(self, folder_name: str) -> str:
		hints = {
			"ai-governance": "聚焦 AI 治理、隐私风险、战略与制度分析。",
			"career-learning": "聚焦职业发展、学习方法与能力建设。",
			"cognition-method": "聚焦认知框架、方法论与思维训练。",
			"finance": "聚焦宏观经济、货币政策与金融结构分析。",
			"industry-tech": "聚焦产业与技术演进、工程实践与趋势。",
			"politics": "聚焦政治议题、制度比较与政策分析。",
			"science": "聚焦科学主题、实证推理与技术知识。",
			"humanities": "聚焦历史、人文与社会文化主题。",
		}
		return hints.get(folder_name.lower(), "")

	def _build_folder_preview_markdown(self, folder_path: Path) -> str:
		if folder_path == self.documents_dir:
			subdirs = sorted(
				[p for p in folder_path.iterdir() if p.is_dir() and p.name.lower() not in {"example", "examples"}],
				key=lambda p: p.name.lower(),
			)
			total_docs = len(self._collect_markdown_files(folder_path))
			lines = [
				"# documents 总览",
				f"- 总文档数: **{total_docs}**",
				f"- 一级分类数: **{len(subdirs)}**",
				"",
				"## 分类目录",
			]
			for sub in subdirs[:20]:
				doc_count = len(self._collect_markdown_files(sub))
				hint = self._folder_theme_hint(sub.name)
				if hint:
					lines.append(f"- **{sub.name}** ({doc_count} 篇): {hint}")
				else:
					lines.append(f"- **{sub.name}** ({doc_count} 篇)")
			lines.extend([
				"",
				"---",
				"提示: 点击左侧具体分类或文档可查看更细内容；也可用关键词/向量搜索定位资料。",
			])
			return "\n".join(lines)

		files = self._collect_markdown_files(folder_path)
		subdirs = sorted([p for p in folder_path.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
		rel = folder_path.relative_to(self.documents_dir).as_posix()
		theme_hint = self._folder_theme_hint(folder_path.name)
		lines = [
			f"# 文件夹概览: {folder_path.name}",
			f"- 相对路径: `{rel}`",
			f"- 文档数量: **{len(files)}**",
			f"- 子目录数量: **{len(subdirs)}**",
		]
		if theme_hint:
			lines.append(f"- 主题提示: {theme_hint}")

		lines.extend(["", "## 代表文档"])
		if not files:
			lines.append("- 当前目录暂无 Markdown 文档。")
		else:
			for idx, file_path in enumerate(files[:10], start=1):
				rel_file = file_path.relative_to(folder_path).as_posix()
				summary = self._extract_doc_quick_summary(file_path)
				if summary:
					lines.append(f"- {idx}. `{rel_file}`: {summary}")
				else:
					lines.append(f"- {idx}. `{rel_file}`")
			if len(files) > 10:
				lines.append(f"- ... 还有 {len(files) - 10} 篇文档")

		if subdirs:
			lines.extend(["", "## 子目录"])
			for sub in subdirs[:12]:
				sub_count = len(self._collect_markdown_files(sub))
				lines.append(f"- `{sub.name}` ({sub_count} 篇)")

		lines.extend([
			"",
			"---",
			"提示: 想快速定位主题可用上方关键词搜索；想找语义相近内容可用向量搜索。",
		])
		return "\n".join(lines)

	def _on_preview_search_changed(self, _event: tk.Event) -> None:
		self.run_keyword_search()

	def _on_preview_search_enter(self, _event: tk.Event) -> str:
		self.run_keyword_search()
		return "break"

	def run_keyword_search(self) -> None:
		self.preview_mode = "keyword"
		self.vector_search_results = []
		self._refresh_documents_tree()

	def _resolve_embedding_model_for_rag(self) -> str:
		return (
			os.getenv("LOCAL_EMBEDDING_MODEL", "").strip()
			or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()
			or (EMBEDDING_MODEL or "").strip()
			or "BAAI/bge-base-zh-v1.5"
		)

	def _refresh_rag_model_label(self) -> None:
		mode = self.rag_answer_mode_var.get().strip().lower() or "local"
		if mode == "deepseek":
			self.rag_model_label_var.set("当前模型：deepseek-chat")
			return
		if mode == "reasoner":
			self.rag_model_label_var.set("当前模型：deepseek-reasoner")
			return

		# For local mode: show local LLM model if configured, else embedding model.
		local_llm = os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model
		if local_llm:
			self.rag_model_label_var.set(f"当前模型：{local_llm}")
			return

		raw_model = self._resolve_embedding_model_for_rag().strip()
		if not raw_model:
			self.rag_model_label_var.set("当前模型：(未设置)")
			return

		def _to_readable_model_name(value: str) -> str:
			display = value.strip()
			if display.lower().startswith("local:"):
				display = display.split(":", 1)[1].strip() or display

			normalized = display.replace("\\", "/").rstrip("/")
			if not normalized:
				return display

			parts = normalized.split("/")

			# HF cache layout: .../models--ORG--MODEL/(snapshots/<hash>)
			for part in parts:
				if part.lower().startswith("models--"):
					model_full = part[len("models--") :].replace("--", "/")
					return model_full.split("/")[-1] or model_full

			if len(parts) >= 3 and parts[-2].lower() == "snapshots":
				return parts[-3]

			# HF id like BAAI/bge-base-zh-v1.5 -> bge-base-zh-v1.5
			if "/" in normalized:
				return parts[-1]

			return normalized

		display_name = _to_readable_model_name(raw_model)

		self.rag_model_label_var.set(f"当前模型：{display_name}")

	def _is_local_embedding_model(self, model: str) -> bool:
		raw = (model or "").strip()
		if not raw:
			return False
		text = raw.lower()
		if text.startswith(("http://", "https://")):
			return False
		if text.startswith("local:"):
			return True

		# Accept explicit local filesystem paths (absolute or relative).
		local_path = Path(raw)
		if local_path.exists():
			return True

		# Accept common local model layouts under shared and legacy model roots.
		local_models_roots = [
			self.core_data_dir / "local_models",
			self.workspace_root / "data" / "local_models",
		]
		leaf = raw.split("/")[-1].strip()
		for local_models_root in local_models_roots:
			if not local_models_root.exists():
				continue
			candidates = [
				local_models_root / raw.replace("\\", "/"),
				local_models_root / raw.replace("/", "--"),
			]
			if leaf:
				candidates.append(local_models_root / leaf)
			if any(path.exists() for path in candidates):
				return True

		# HF-style IDs are commonly used for local-cached embedding models.
		if "/" in raw and not raw.startswith("http"):
			return True

		local_markers = (
			"bge",
			"nomic-embed",
			"mxbai-embed",
			"jina-embeddings",
			"snowflake-arctic-embed",
			"stella",
			"bce-embedding",
			"gte-",
			"multilingual-e5",
			"intfloat/e5",
			"sentence-transformers/",
			"baai/",
			"embed",
		)
		return any(marker in text for marker in local_markers)

	def run_vector_search(self) -> None:
		self._refresh_rag_model_label()
		if self.is_running:
			self._append_log("已有任务在运行，请等待结束。")
			return

		query = self.preview_search_var.get().strip()
		if not query:
			self._append_log("向量搜索：请输入查询内容。")
			self._show_monokai_dialog("向量搜索", "请输入查询内容后再搜索。", level="info")
			return

		model = self._resolve_embedding_model_for_rag()
		if not model:
			self._append_log("向量搜索失败：请先填写 MODEL（或设置 LOCAL_EMBEDDING_MODEL）。")
			self._show_monokai_dialog("向量搜索失败", "请先填写 MODEL 或设置 LOCAL_EMBEDDING_MODEL。", level="warning")
			return
		if not self._is_local_embedding_model(model):
			self._append_log("向量搜索失败：仅支持本地 embedding 模型。")
			self._append_log("请设置环境变量 LOCAL_EMBEDDING_MODEL，例如 BAAI/bge-base-zh-v1.5。")
			self._show_monokai_dialog("向量搜索失败", "仅支持本地 embedding 模型。\n请设置 LOCAL_EMBEDDING_MODEL。", level="warning")
			return
		self._append_log(f"向量搜索：使用本地 embedding 模型 {model}")

		self._append_log("向量搜索：检查/构建索引中...")
		self._show_vector_search_progress()
		self._set_running(True)
		thread = threading.Thread(
			target=self._run_vector_search_worker,
			args=(query, model),
			daemon=True,
		)
		thread.start()

	def _run_vector_search_worker(self, query: str, model: str) -> None:
		start_ts = time.perf_counter()
		try:
			results, timings = search_vector_index_with_diagnostics(
				query=query,
				documents_dir=self.documents_dir,
				index_dir=self.vector_index_dir,
				top_k=10,
				backend="faiss",
				build_if_missing=True,
				embedding_model=model,
				timeout=TIMEOUT,
				stage_callback=lambda stage: self.log_queue.put(("vector_stage", stage)),
			)
			self._pending_vector_results = results
			elapsed = time.perf_counter() - start_ts
			self._queue_log(f"向量搜索耗时：{elapsed:.2f}s")
			if timings:
				prepare_t = timings.get("prepare_index", 0.0)
				embed_t = timings.get("embed_query", 0.0)
				faiss_t = timings.get("faiss_search", timings.get("chroma_search", 0.0))
				self._queue_log(
					f"阶段耗时：prepare_index={prepare_t:.2f}s, embed_query={embed_t:.2f}s, faiss_search={faiss_t:.2f}s"
				)
			self.log_queue.put(("vector_ready", ""))
		except RAGIndexError as exc:
			err_text = str(exc)
			if "sentence-transformers" in err_text.lower() or "sentence_transformers" in err_text.lower():
				try:
					self._queue_log("当前解释器缺少 sentence-transformers，尝试切换解释器执行向量搜索...")
					if not self.vector_fallback_warned:
						self._queue_log("提示：当前已进入子进程降级路径，后续每次搜索都可能较慢。建议用项目 .venv 启动 GUI。")
						self.vector_fallback_warned = True
					results = self._vector_search_via_subprocess(query=query, model=model)
					self._pending_vector_results = results
					elapsed = time.perf_counter() - start_ts
					self._queue_log(f"向量搜索耗时：{elapsed:.2f}s")
					self.log_queue.put(("vector_ready", ""))
				except Exception as sub_exc:  # noqa: BLE001
					self._queue_log(f"向量搜索失败：{sub_exc}")
					self.log_queue.put(("vector_error", str(sub_exc)))
			else:
				self._queue_log(f"向量搜索失败：{exc}")
				self._queue_log("请先点击“补齐向量”，将 documents 中未入库文档写入 FAISS。")
				self.log_queue.put(("vector_error", str(exc)))
		except Exception as exc:  # noqa: BLE001
			self._queue_log(f"向量搜索异常：{exc}")
			self.log_queue.put(("vector_error", str(exc)))
		finally:
			self.log_queue.put(("done", ""))

	def _vector_search_via_subprocess(self, query: str, model: str) -> list[dict[str, object]]:
		python_cmd = self._resolve_python_for_module("sentence_transformers")
		if not python_cmd:
			raise RAGIndexError("未找到可导入 sentence_transformers 的 Python 解释器。")

		tmp_path = ""
		try:
			with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
				tmp_path = tmp.name

			memory_context = self._load_memory_context_for_session(session_idx)

			command = [
				python_cmd,
				"-u",
				str(self.script_dir / "rag_vector_index.py"),
				"--documents-dir",
				str(self.documents_dir),
				"--index-dir",
				str(self.vector_index_dir),
				"--backend",
				"faiss",
				"--query",
				query,
				"--top-k",
				"10",
				"--embedding-model",
				model,
				"--output-json",
				tmp_path,
				"--timeout",
				str(TIMEOUT),
			]

			creationflags = _no_window_creationflags()
			child_env = os.environ.copy()
			child_env["PYTHONUNBUFFERED"] = "1"
			child_env["PYTHONIOENCODING"] = "utf-8"
			child_env["PYTHONUTF8"] = "1"
			result = subprocess.run(
				command,
				cwd=str(self.script_dir),
				env=child_env,
				stdout=subprocess.PIPE,
				stderr=subprocess.STDOUT,
				text=True,
				encoding="utf-8",
				errors="replace",
				check=False,
				creationflags=creationflags,
			)
			if result.stdout:
				for line in result.stdout.splitlines():
					if line.strip():
						self._queue_log(line)

			if result.returncode != 0:
				raise RAGIndexError(f"向量搜索子进程失败，退出码: {result.returncode}")

			if not tmp_path or not Path(tmp_path).exists():
				raise RAGIndexError("向量搜索结果文件未生成")

			results = json.loads(Path(tmp_path).read_text(encoding="utf-8"))
			if not isinstance(results, list):
				raise RAGIndexError("向量搜索结果格式无效")
			return results
		finally:
			if tmp_path:
				try:
					Path(tmp_path).unlink(missing_ok=True)
				except Exception:
					pass

	def _highlight_preview_search_text(self) -> None:
		self.preview_text.tag_remove("highlight", "1.0", tk.END)
		query = self.preview_search_var.get().strip()
		if not query:
			return

		start = "1.0"
		while True:
			pos = self.preview_text.search(query, start, stopindex=tk.END, nocase=True)
			if not pos:
				break
			end = f"{pos}+{len(query)}c"
			self.preview_text.tag_add("highlight", pos, end)
			start = end

	def _get_selected_doc_path(self) -> Path | None:
		selected = self.docs_tree.selection()
		if not selected:
			return None
		return self.preview_node_paths.get(selected[0])

	def _find_tree_node_by_path(self, target: Path) -> str | None:
		for node_id, path in self.preview_node_paths.items():
			if path == target:
				return node_id
		return None

	def _restore_preview_selection_after_tree_refresh(self, selected_path: Path | None) -> None:
		# Re-select previous node when possible so right preview reflects latest counts/content.
		node_id: str | None = None
		if selected_path is not None:
			node_id = self._find_tree_node_by_path(selected_path)

		if node_id is None:
			node_id = self._find_tree_node_by_path(self.documents_dir)

		if node_id is None:
			self._set_markdown_text(self.preview_text, self._build_folder_preview_markdown(self.documents_dir))
			return

		self.docs_tree.selection_set(node_id)
		self.docs_tree.focus(node_id)
		self.docs_tree.see(node_id)
		self._on_preview_tree_select(None)

	def _show_docs_context_menu(self, event: tk.Event) -> str:
		node_id = self.docs_tree.identify_row(event.y)
		if not node_id:
			return "break"

		self.docs_tree.selection_set(node_id)
		self.docs_tree.focus(node_id)
		try:
			self.docs_menu.tk_popup(event.x_root, event.y_root)
		finally:
			self.docs_menu.grab_release()
		return "break"

	def _rename_selected_doc_node(self) -> None:
		path = self._get_selected_doc_path()
		if not path or not path.exists():
			return
		if path == self.documents_dir:
			self._append_log("根目录 documents 不支持重命名。")
			return

		new_name = simpledialog.askstring("重命名", "输入新名称", initialvalue=path.name, parent=self.root)
		if not new_name:
			return
		new_name = new_name.strip()
		if not new_name:
			return

		target = path.with_name(new_name)
		if target.exists():
			self._show_monokai_dialog("重命名失败", f"目标已存在: {target.name}")
			return

		try:
			path.rename(target)
		except Exception as exc:  # noqa: BLE001
			self._show_monokai_dialog("重命名失败", str(exc))
			return

		self._append_log(f"已重命名: {path.name} -> {target.name}")
		self._refresh_documents_tree()

	def _open_selected_doc_location(self) -> None:
		path = self._get_selected_doc_path()
		if not path or not path.exists():
			return

		folder = path if path.is_dir() else path.parent
		try:
			os.startfile(str(folder))
		except Exception as exc:  # noqa: BLE001
			self._show_monokai_dialog("打开失败", str(exc))

	def _load_info_content(self) -> None:
		info_markdown = """# GUI 使用指南

本工具用于对话数据处理、文档预览检索、RAG 问答和整体流程管理。

## Tab 导航

- **预览**: 浏览 `documents` 目录、关键词搜索、向量搜索、查看文档内容。
- **RAG Q&A**: 基于本地向量库检索资料后向模型提问，支持历史会话持久化。
- **批处理**: 上传数据、格式批处理、AI 总结、输出分类、补齐向量、配置 API。
- **Info**: 当前说明页，汇总所有功能和建议工作流。

## 预览 Tab

- 左侧树展示 `documents` 内容（自动隐藏 `.gitkeep`）。
- 点击 Markdown 文件: 右侧渲染正文。
- 点击目录: 右侧显示该目录概览（文档数、子目录、代表文档摘要）。
- `关键词搜索`: 关键字匹配文件名与内容。
- `向量搜索`: 语义检索相关文档片段。
- `刷新目录`: 重扫目录并清理已删除文档对应的失效向量条目。

## RAG Q&A Tab

- 左侧会话列表支持新建、切换、右键删除。
- 发送问题后流程: 检索 -> 组装上下文 -> 调用模型生成回答。
- 会话标题由 AI 根据首次问题自动生成。
- 会话记录持久化在 `data/rag_sessions/*.md`。
- 输入规则: `Enter` 发送，`Shift+Enter` 换行。

## 批处理 Tab

- `上传源文件`: 将源文件复制到 `data/raw_dir`。
- `格式批处理`: 按来源脚本做预处理。
- `AI 总结`: 调用模型生成总结到 `data/summarize_dir`。
- `输出分类`: 三步工作流处理文档
  1. 拆分多主题文档 (`summarize_dir` → `split_dir`)
  2. 分类移动拆分后的文档 (`split_dir` → `documents`)
  3. 分类移动单主题文档 (`summarize_dir` → `documents`)
- `补齐向量`: 仅为 `documents` 中新增/缺失文档写入向量索引。
- `保存配置`: 保存 `API_BASE_URL`、`MODEL`，并更新 API key 环境变量。

## 推荐工作流

1. 在 `批处理` 完成上传、处理、总结、分类。
2. 点击 `补齐向量` 更新向量库。
3. 在 `预览` 检查文档质量并做关键词/向量搜索。
4. 在 `RAG Q&A` 发起问题并沉淀会话。

## 常见问题

- RAG 报 `Missing dependency: openai`: 在项目环境执行 `pip install openai`。
- 向量检索失败: 先执行 `补齐向量`，再重试。
- 没有检索到内容: 检查 `documents` 是否包含目标主题文档。
"""
		self._set_markdown_text(self.readme_text, info_markdown)

	def toggle_api_key_visibility(self) -> None:
		self.api_key_visible = not self.api_key_visible
		self.api_key_entry.configure(show="" if self.api_key_visible else "*")
		self.show_key_btn.configure(text="隐藏" if self.api_key_visible else "显示")

	def _resolve_api_key(self) -> str:
		return (self.api_key_var.get().strip() or os.getenv("DEEPSEEK_API_KEY", "").strip())

	def _persist_api_key_env(self, api_key: str) -> tuple[bool, str]:
		# Keep current process in sync so subsequent subprocess calls can use updated key immediately.
		os.environ["DEEPSEEK_API_KEY"] = api_key

		if sys.platform != "win32":
			return True, "已写入当前进程环境变量 DEEPSEEK_API_KEY。"

		try:
			result = subprocess.run(
				["setx", "DEEPSEEK_API_KEY", api_key],
				stdout=subprocess.PIPE,
				stderr=subprocess.PIPE,
				text=True,
				encoding="utf-8",
				errors="replace",
				check=False,
				creationflags=_no_window_creationflags(),
			)
		except Exception as exc:  # noqa: BLE001
			return False, f"环境变量写入失败：{exc}"

		if result.returncode != 0:
			detail = (result.stderr or result.stdout or "未知错误").strip()
			return False, f"环境变量写入失败：{detail}"

		return True, "已更新用户环境变量 DEEPSEEK_API_KEY（新开的终端会自动生效）。"

	def save_config(self) -> None:
		base_url = self.base_url_var.get().strip()
		model = self.model_var.get().strip()
		api_key = self._resolve_api_key().strip()

		if not base_url or not model or not api_key:
			self._append_log("保存失败：请先完整填写 API_BASE_URL、MODEL、API_KEY。")
			return

		core_payload = {
			"api": {
				"base_url": base_url,
				"api_key": api_key,
				"chat_model": model,
				"timeout": TIMEOUT,
			},
			"rag": {
				"embedding_model": (os.getenv("LOCAL_EMBEDDING_MODEL", "").strip() or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()),
			},
			"local_llm": {
				"url": os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "http://127.0.0.1:1234/v1").strip(),
				"model": os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model,
				"api_key": os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local").strip() or "local",
			},
		}

		env_ok, env_msg = self._persist_api_key_env(api_key)
		if not env_ok:
			self._append_log(f"保存失败：{env_msg}")
			return

		try:
			self.core_config_path.parent.mkdir(parents=True, exist_ok=True)
			self.core_config_path.write_text(json.dumps(core_payload, ensure_ascii=False, indent=2), encoding="utf-8")
		except Exception as exc:  # noqa: BLE001
			self._append_log(f"保存失败：{exc}")
			return

		self.raw_api_key = api_key
		self.api_key_var.set(api_key)
		if not self.api_key_visible:
			self.api_key_entry.configure(show="*")
		self._append_log(f"配置已保存：{self.core_config_path}")
		self._append_log("兼容说明：scripts/api_config.py 由 core_service 配置统一读取。")
		self._append_log(env_msg)

	def _append_log(self, message: str) -> None:
		time_prefix = datetime.now().strftime("%H:%M:%S")
		line = f"[{time_prefix}] {message}\n"
		self.log_text.configure(state=tk.NORMAL)
		self.log_text.insert(tk.END, line)
		self.log_text.see(tk.END)
		self.log_text.configure(state=tk.DISABLED)

	def _show_monokai_dialog(self, title: str, message: str, level: str = "error") -> None:
		level_color = {
			"error": self.colors["pink"],
			"warning": self.colors["orange"],
			"info": self.colors["cyan"],
		}.get(level, self.colors["pink"])

		win = tk.Toplevel(self.root)
		win.title(title)
		win.configure(bg=self.colors["bg"])
		win.resizable(True, True)
		win.transient(self.root)
		win.grab_set()
		try:
			win.attributes("-topmost", True)
		except Exception:
			pass
		win.bind("<FocusOut>", lambda _e: self._schedule_date_picker_focus_check(), add="+")

		panel = tk.Frame(win, bg=self.colors["panel"], padx=10, pady=10)
		panel.pack(fill=tk.BOTH, expand=True)

		title_label = tk.Label(
			panel,
			text=title,
			bg=self.colors["panel"],
			fg=level_color,
			font=("Microsoft YaHei UI", 10, "bold"),
			anchor="w",
		)
		title_label.pack(fill=tk.X, pady=(0, 6))

		text_box = tk.Text(
			panel,
			height=8,
			wrap=tk.WORD,
			bg="#1E1F1C",
			fg=self.colors["text"],
			insertbackground=self.colors["accent"],
			relief=tk.FLAT,
			borderwidth=0,
			font=("Microsoft YaHei UI", 10),
		)
		text_box.pack(fill=tk.BOTH, expand=True)
		text_box.insert("1.0", message)
		text_box.configure(state=tk.DISABLED)

		btn_row = tk.Frame(panel, bg=self.colors["panel"])
		btn_row.pack(fill=tk.X, pady=(8, 0))

		def _copy_text() -> None:
			content = text_box.get("1.0", tk.END).strip()
			if not content:
				return
			self.root.clipboard_clear()
			self.root.clipboard_append(content)
			self.root.update()

		copy_btn = tk.Button(
			btn_row,
			text="复制",
			command=_copy_text,
			bg=self.colors["panel_soft"],
			fg=self.colors["text"],
			activebackground=self.colors["select"],
			activeforeground=self.colors["text"],
			relief=tk.FLAT,
			padx=12,
		)
		copy_btn.pack(side=tk.LEFT)

		close_btn = tk.Button(
			btn_row,
			text="关闭",
			command=win.destroy,
			bg=level_color,
			fg="#11120F",
			activebackground=level_color,
			activeforeground="#11120F",
			relief=tk.FLAT,
			padx=12,
		)
		close_btn.pack(side=tk.RIGHT)

		text_box.focus_set()
		win.minsize(460, 240)
		self.root.update_idletasks()
		win.update_idletasks()
		x = self.root.winfo_rootx() + (self.root.winfo_width() - win.winfo_width()) // 2
		y = self.root.winfo_rooty() + (self.root.winfo_height() - win.winfo_height()) // 2
		win.geometry(f"+{max(x, 0)}+{max(y, 0)}")

	def _clear_log_text(self) -> None:
		self.log_text.configure(state=tk.NORMAL)
		self.log_text.delete("1.0", tk.END)
		self.log_text.configure(state=tk.DISABLED)

	def _copy_log_text(self) -> None:
		content = self.log_text.get("1.0", tk.END).strip()
		if not content:
			self._append_log("日志为空，未复制。")
			return
		try:
			self.root.clipboard_clear()
			self.root.clipboard_append(content)
			self.root.update()
		except Exception as exc:  # noqa: BLE001
			self._append_log(f"复制日志失败：{exc}")
			return
		self._append_log("日志已复制到剪贴板。")

	def _attach_tooltip(self, widget: tk.Widget, text: str) -> HoverToolTip:
		tip = HoverToolTip(widget, text, bg="#1F201C", fg=self.colors["text"])
		self.tooltips.append(tip)
		return tip

	def _on_source_changed(self, _event: tk.Event | None = None) -> None:
		self._update_batch_button_tooltip()

	def _update_batch_button_tooltip(self) -> None:
		if self.batch_btn_tooltip is None or self.upload_btn_tooltip is None:
			return
		source = self.source_var.get().strip().lower()
		if source == "chatgpt":
			self.upload_btn_tooltip.text = "上传 ChatGPT 导出文件到 raw_dir（支持 zip/json/html）。"
			self.batch_btn_tooltip.text = "对 raw_dir 执行 ChatGPT 批处理（支持 zip/json/html）。"
		else:
			self.upload_btn_tooltip.text = "选择并复制源文件 (如JSON) 到 data/raw_dir。"
			self.batch_btn_tooltip.text = "对 raw_dir 文件执行格式批处理。"

	def _show_log_context_menu(self, event: tk.Event) -> str:
		try:
			self.log_menu.tk_popup(event.x_root, event.y_root)
		finally:
			self.log_menu.grab_release()
		return "break"

	def _queue_log(self, message: str) -> None:
		self.log_queue.put(("log", message))

	def _drain_log_queue(self) -> None:
		# Central dispatcher for background task events.
		# Event kinds are intentionally simple strings to keep worker side lightweight.
		try:
			while True:
				kind, payload = self.log_queue.get_nowait()
				if kind == "log":
					self._append_log(payload)
				elif kind == "vector_stage":
					self.vector_progress_stage = payload
				elif kind == "vector_ready":
					self._hide_vector_search_progress()
					self.vector_search_results = self._pending_vector_results
					self.preview_mode = "vector"
					self._refresh_documents_tree()
					self._append_log(f"向量搜索完成：返回 {len(self.vector_search_results)} 条结果。")
				elif kind == "vector_error":
					self._hide_vector_search_progress()
					self._show_monokai_dialog("向量搜索失败", payload)
				elif kind == "rag_qa_answer":
					try:
						data = json.loads(payload)
						session_idx = int(data.get("session_idx", self.rag_current_session_idx))
						answer = str(data.get("answer", "")).strip()
						title = str(data.get("session_title", "")).strip()
						mode = str(data.get("mode", "local")).strip().lower() or "local"
						used_docs = data.get("used_docs", [])
					except Exception:
						session_idx = self.rag_current_session_idx
						answer = payload
						title = ""
						mode = "local"
						used_docs = []
					if mode == "local" and isinstance(used_docs, list):
						answer = self._format_local_answer_with_refs(answer, used_docs)
					if title:
						session = self.rag_sessions[session_idx] if 0 <= session_idx < len(self.rag_sessions) else None
						if session is not None and not bool(session.get("title_locked", False)):
							self._set_rag_session_title(session_idx, title, lock=True)
					self._append_rag_chat("助手", answer, session_idx=session_idx)
				elif kind == "rag_progress_update":
					# Update the last system message with progress info
					try:
						data = json.loads(payload)
						session_idx = int(data.get("session_idx", self.rag_current_session_idx))
						message = str(data.get("message", "")).strip()
					except Exception:
						session_idx = self.rag_current_session_idx
						message = payload
					self._update_last_rag_message("系统", message, session_idx=session_idx)
				elif kind == "rag_stream_chunk":
					# Update assistant message with streaming chunks
					try:
						data = json.loads(payload)
						session_idx = int(data.get("session_idx", self.rag_current_session_idx))
						full_answer = str(data.get("full_answer", "")).strip()
					except Exception:
						continue
					# Ensure there's an assistant message to update (create if needed)
					if 0 <= session_idx < len(self.rag_sessions):
						messages = self.rag_sessions[session_idx].get("messages", [])
						if not messages or messages[-1][0] != "助手":
							self._append_rag_chat("助手", "", session_idx=session_idx)
					self._update_last_rag_message("助手", full_answer, session_idx=session_idx)
				elif kind == "rag_session_title":
					# Update session title
					try:
						data = json.loads(payload)
						session_idx = int(data.get("session_idx", self.rag_current_session_idx))
						title = str(data.get("title", "")).strip()
					except Exception:
						continue
					if title and 0 <= session_idx < len(self.rag_sessions):
						session = self.rag_sessions[session_idx]
						if not bool(session.get("title_locked", False)):
							self._set_rag_session_title(session_idx, title, lock=True)
				elif kind == "rag_qa_error":
					try:
						data = json.loads(payload)
						session_idx = int(data.get("session_idx", self.rag_current_session_idx))
						err_msg = str(data.get("message", "")).strip()
					except Exception:
						session_idx = self.rag_current_session_idx
						err_msg = payload
					self._append_rag_chat("助手", f"RAG Q&A 失败：{err_msg}", session_idx=session_idx)
					self._show_monokai_dialog("RAG Q&A 失败", err_msg)
				elif kind == "rag_qa_cancelled":
					try:
						data = json.loads(payload)
						session_idx = int(data.get("session_idx", self.rag_current_session_idx))
						msg = str(data.get("message", "已中止")).strip() or "已中止"
					except Exception:
						session_idx = self.rag_current_session_idx
						msg = "已中止"
					self._append_rag_chat("系统", msg, session_idx=session_idx)
				elif kind == "rag_qa_done":
					self._set_rag_qa_running(False)
				elif kind == "done":
					self._set_running(False)
					self._refresh_extracted_count()
		except queue.Empty:
			pass
		self.root.after(100, self._drain_log_queue)

	def _parse_date_str(self, value: str) -> date | None:
		text = value.strip()
		if not text:
			return None
		try:
			return datetime.strptime(text, "%Y-%m-%d").date()
		except ValueError:
			return None

	def _extract_date_from_file_name(self, file_path: Path) -> date | None:
		name = file_path.name
		if len(name) < 10:
			return None
		prefix = name[:10]
		try:
			return datetime.strptime(prefix, "%Y-%m-%d").date()
		except ValueError:
			return None

	def _get_selected_date_range(self) -> tuple[date | None, date | None, str | None]:
		start_date = self._parse_date_str(self.start_date_var.get())
		end_date = self._parse_date_str(self.end_date_var.get())
		if start_date is None:
			return None, None, "开始日期格式无效，请使用 YYYY-MM-DD。"
		if end_date is None:
			return None, None, "结束日期格式无效，请使用 YYYY-MM-DD。"
		if start_date > end_date:
			return None, None, "开始日期不能晚于结束日期。"
		return start_date, end_date, None

	def _collect_extracted_files_in_selected_range(self) -> tuple[list[Path], str | None]:
		start_date, end_date, err = self._get_selected_date_range()
		if err:
			return [], err

		files: list[Path] = []
		for file_path in sorted(self.extracted_dir.glob("*.md")):
			file_date = self._extract_date_from_file_name(file_path)
			if file_date is None:
				continue
			if start_date <= file_date <= end_date:
				files.append(file_path)
		return files, None

	def _refresh_extracted_count(self) -> None:
		total_count = len(list(self.extracted_dir.glob("*.md")))
		files, err = self._collect_extracted_files_in_selected_range()
		if err:
			self.extracted_count_var.set(f"提取文件: 日期无效 / 总计 {total_count}")
			return
		self.extracted_count_var.set(f"提取文件: {len(files)} / 总计 {total_count}")

	def _open_date_picker(self, target_var: tk.StringVar, anchor_widget: tk.Widget | None = None) -> None:
		if self.date_picker_month is not None:
			self.date_picker_month = self._clamp_date_picker_month(self.date_picker_month)

		if self.date_picker_window is not None and self.date_picker_target_var is target_var:
			self.date_picker_anchor_widget = anchor_widget
			self._position_date_picker(anchor_widget)
			try:
				self.date_picker_window.lift()
			except Exception:
				pass
			return

		initial = self._parse_date_str(target_var.get()) or date.today()
		if initial.year < DATE_PICKER_MIN_YEAR:
			initial = date(DATE_PICKER_MIN_YEAR, 1, 1)
		elif initial.year > DATE_PICKER_MAX_YEAR:
			initial = date(DATE_PICKER_MAX_YEAR, 12, 1)
		self._close_date_picker()

		self.date_picker_target_var = target_var
		self.date_picker_month = self._clamp_date_picker_month(date(initial.year, initial.month, 1))
		self.date_picker_anchor_widget = anchor_widget

		win = tk.Toplevel(self.root)
		win.overrideredirect(True)
		win.transient(self.root)
		win.resizable(False, False)
		win.configure(bg=self.colors["panel"], padx=8, pady=8)
		try:
			win.attributes("-topmost", True)
		except Exception:
			pass

		header = tk.Frame(win, bg=self.colors["panel"])
		header.pack(fill=tk.X, pady=(0, 6))
		header.grid_columnconfigure(0, weight=1)
		header.grid_columnconfigure(1, weight=1)
		header.grid_columnconfigure(2, weight=1)

		prev_btn = tk.Button(
			header,
			text="<",
			width=3,
			relief=tk.FLAT,
			bg=self.colors["panel_soft"],
			fg=self.colors["text"],
			activebackground=self.colors["select"],
			activeforeground=self.colors["text"],
			command=lambda: self._shift_date_picker_month(-1),
		)
		prev_btn.grid(row=0, column=0, sticky="w")

		month_label = tk.Label(
			header,
			bg=self.colors["panel"],
			fg=self.colors["accent"],
			font=("Microsoft YaHei UI", 10, "bold"),
		)
		month_label.grid(row=0, column=1, sticky="n")

		next_btn = tk.Button(
			header,
			text=">",
			width=3,
			relief=tk.FLAT,
			bg=self.colors["panel_soft"],
			fg=self.colors["text"],
			activebackground=self.colors["select"],
			activeforeground=self.colors["text"],
			command=lambda: self._shift_date_picker_month(1),
		)
		next_btn.grid(row=0, column=2, sticky="e")

		grid_frame = tk.Frame(win, bg=self.colors["panel"])
		grid_frame.pack(fill=tk.BOTH)
		self.date_picker_window = win
		self.date_picker_month_label = month_label
		self.date_picker_grid_frame = grid_frame
		self._render_date_picker_grid()
		self._position_date_picker(anchor_widget)

	def _position_date_picker(self, anchor_widget: tk.Widget | None) -> None:
		if self.date_picker_window is None:
			return
		self.root.update_idletasks()
		self.date_picker_window.update_idletasks()
		if anchor_widget is not None:
			x = anchor_widget.winfo_rootx()
			y = anchor_widget.winfo_rooty() + anchor_widget.winfo_height() + 4
		else:
			x = self.root.winfo_rootx() + self.root.winfo_width() - self.date_picker_window.winfo_width() - 24
			y = self.root.winfo_rooty() + 110
		self.date_picker_window.geometry(f"+{max(x, 0)}+{max(y, 0)}")

	def _close_date_picker(self) -> None:
		if self.date_picker_window is not None:
			try:
				self.date_picker_window.destroy()
			except Exception:
				pass
		self.date_picker_window = None
		self.date_picker_month_label = None
		self.date_picker_grid_frame = None
		self.date_picker_anchor_widget = None
		if self.date_picker_focus_check_after_id is not None:
			try:
				self.root.after_cancel(self.date_picker_focus_check_after_id)
			except Exception:
				pass
		self.date_picker_focus_check_after_id = None

	def _is_widget_descendant_of(self, child: tk.Widget | None, ancestor: tk.Widget | None) -> bool:
		if child is None or ancestor is None:
			return False
		current: tk.Widget | None = child
		while current is not None:
			if current == ancestor:
				return True
			parent_name = current.winfo_parent()
			if not parent_name:
				break
			try:
				current = current._nametowidget(parent_name)
			except Exception:
				break
		return False

	def _handle_global_click_for_date_picker(self, event: tk.Event) -> None:
		if self.date_picker_window is None:
			return

		clicked_widget = event.widget if isinstance(event.widget, tk.Widget) else None
		if clicked_widget is None:
			self._close_date_picker()
			return

		if self._is_widget_descendant_of(clicked_widget, self.date_picker_window):
			return
		if self.start_date_entry is not None and self._is_widget_descendant_of(clicked_widget, self.start_date_entry):
			return
		if self.end_date_entry is not None and self._is_widget_descendant_of(clicked_widget, self.end_date_entry):
			return

		self._close_date_picker()

	def _schedule_date_picker_focus_check(self) -> None:
		if self.date_picker_window is None:
			return
		if self.date_picker_focus_check_after_id is not None:
			try:
				self.root.after_cancel(self.date_picker_focus_check_after_id)
			except Exception:
				pass
		self.date_picker_focus_check_after_id = self.root.after(20, self._handle_global_focus_for_date_picker)

	def _handle_global_focus_for_date_picker(self) -> None:
		self.date_picker_focus_check_after_id = None
		if self.date_picker_window is None:
			return

		focused_widget = self.root.focus_get()
		if focused_widget is None:
			self._close_date_picker()
			return

		if self._is_widget_descendant_of(focused_widget, self.date_picker_window):
			return
		if self.start_date_entry is not None and self._is_widget_descendant_of(focused_widget, self.start_date_entry):
			return
		if self.end_date_entry is not None and self._is_widget_descendant_of(focused_widget, self.end_date_entry):
			return

		self._close_date_picker()

	def _shift_date_picker_month(self, delta: int) -> None:
		if self.date_picker_window is None or self.date_picker_month is None:
			return
		year = self.date_picker_month.year
		month = self.date_picker_month.month + delta
		while month < 1:
			month += 12
			year -= 1
		while month > 12:
			month -= 12
			year += 1
		self.date_picker_month = self._clamp_date_picker_month(date(year, month, 1))
		self._render_date_picker_grid()

	def _clamp_date_picker_month(self, value: date) -> date:
		if value.year < DATE_PICKER_MIN_YEAR:
			return date(DATE_PICKER_MIN_YEAR, 1, 1)
		if value.year > DATE_PICKER_MAX_YEAR:
			return date(DATE_PICKER_MAX_YEAR, 12, 1)
		return date(value.year, value.month, 1)

	def _render_date_picker_grid(self) -> None:
		if self.date_picker_month is None or self.date_picker_month_label is None or self.date_picker_grid_frame is None:
			return
		self.date_picker_month_label.configure(text=self.date_picker_month.strftime("%Y-%m"))

		for child in self.date_picker_grid_frame.winfo_children():
			child.destroy()

		weekday_names = ["日", "一", "二", "三", "四", "五", "六"]
		weekend_color = "#ECECE3"
		weekday_color = self.colors["text_muted"]
		weekend_bg = "#56564D"
		weekday_bg = self.colors["panel_soft"]
		for index, day_name in enumerate(weekday_names):
			header_fg = weekend_color if index in (0, 6) else weekday_color
			header_bg = "#4A4A42" if index in (0, 6) else self.colors["panel"]
			label = tk.Label(self.date_picker_grid_frame, text=day_name, width=3, bg=header_bg, fg=header_fg, font=("Microsoft YaHei UI", 9))
			label.grid(row=0, column=index, padx=2, pady=2)

		year = self.date_picker_month.year
		month = self.date_picker_month.month
		first_day = date(year, month, 1)
		# Convert Python Monday-first weekday to Sunday-first column index.
		start_col = (first_day.weekday() + 1) % 7
		for day_num in range(1, 32):
			try:
				date(year, month, day_num)
			except ValueError:
				break
			index = start_col + day_num - 1
			row_idx = 1 + index // 7
			col_idx = index % 7
			day_fg = weekend_color if col_idx in (0, 6) else self.colors["text"]
			day_bg = weekend_bg if col_idx in (0, 6) else weekday_bg
			btn = tk.Button(
				self.date_picker_grid_frame,
				text=str(day_num),
				width=3,
				relief=tk.FLAT,
				bg=day_bg,
				fg=day_fg,
				activebackground=self.colors["select"],
				activeforeground=weekend_color if col_idx in (0, 6) else self.colors["text"],
				command=lambda d=day_num: self._pick_date(d),
			)
			btn.grid(row=row_idx, column=col_idx, padx=2, pady=2)

	def _pick_date(self, day_num: int) -> None:
		if self.date_picker_month is None or self.date_picker_target_var is None:
			return
		picked = date(self.date_picker_month.year, self.date_picker_month.month, day_num)
		self.date_picker_target_var.set(picked.strftime("%Y-%m-%d"))
		self._close_date_picker()
		self._refresh_extracted_count()

	def _show_vector_search_progress(self) -> None:
		self._hide_vector_search_progress()
		self.vector_progress_start_ts = time.perf_counter()
		self.vector_progress_stage = "prepare_index"

		win = tk.Toplevel(self.root)
		win.overrideredirect(True)
		win.resizable(False, False)
		win.transient(self.root)
		win.configure(bg=self.colors["bg"], highlightthickness=1, highlightbackground=self.colors["select"])
		try:
			win.attributes("-topmost", True)
		except Exception:
			pass
		win.lift()
		win.deiconify()

		panel = tk.Frame(win, bg=self.colors["panel"], padx=2, pady=2)
		panel.pack(fill=tk.BOTH, expand=True)

		title = tk.Label(
			panel,
			text="向量搜索中",
			bg=self.colors["panel"],
			fg=self.colors["accent"],
			font=("Microsoft YaHei UI", 10, "bold"),
			anchor="w",
			padx=10,
			pady=6,
		)
		title.pack(fill=tk.X)

		label = tk.Label(
			panel,
			text="阶段: prepare_index\n已用时: 0.0s",
			bg=self.colors["panel"],
			fg=self.colors["text"],
			font=("Microsoft YaHei UI", 10),
			padx=18,
			pady=10,
		)
		label.pack()

		self.vector_progress_window = win
		self.vector_progress_label = label
		self._center_vector_search_progress()
		self._tick_vector_search_progress()

	def _center_vector_search_progress(self) -> None:
		if self.vector_progress_window is None:
			return
		self.root.update_idletasks()
		self.vector_progress_window.update_idletasks()
		x = self.root.winfo_rootx() + (self.root.winfo_width() - self.vector_progress_window.winfo_width()) // 2
		y = self.root.winfo_rooty() + (self.root.winfo_height() - self.vector_progress_window.winfo_height()) // 2
		self.vector_progress_window.geometry(f"+{max(x, 0)}+{max(y, 0)}")

	def _tick_vector_search_progress(self) -> None:
		if self.vector_progress_window is None or self.vector_progress_start_ts is None:
			return
		elapsed = time.perf_counter() - self.vector_progress_start_ts
		if self.vector_progress_label is not None:
			self.vector_progress_label.configure(text=f"阶段: {self.vector_progress_stage}\n已用时: {elapsed:.1f}s")
		self.vector_progress_after_id = self.root.after(200, self._tick_vector_search_progress)

	def _hide_vector_search_progress(self) -> None:
		if self.vector_progress_after_id is not None:
			try:
				self.root.after_cancel(self.vector_progress_after_id)
			except Exception:
				pass
			self.vector_progress_after_id = None

		if self.vector_progress_window is not None:
			try:
				self.vector_progress_window.destroy()
			except Exception:
				pass

		self.vector_progress_window = None
		self.vector_progress_label = None
		self.vector_progress_start_ts = None
		self.vector_progress_stage = "准备中"

	def _set_running(self, running: bool) -> None:
		self.is_running = running
		if not running:
			self._hide_vector_search_progress()
		state = tk.DISABLED if running else tk.NORMAL
		self.upload_btn.configure(state=state)
		self.source_combo.configure(state="disabled" if running else "readonly")
		self.batch_btn.configure(state=state)
		self.summary_btn.configure(state=state)
		self.save_cfg_btn.configure(state=state)
		self.classify_btn.configure(state=state)
		self.sync_embed_btn.configure(state=state)
		self.show_key_btn.configure(state=state)

	def upload_json_files(self) -> None:
		file_paths = filedialog.askopenfilenames(
			title="选择源文件",
			filetypes=[("All Files", "*.*"), ("JSON Files", "*.json")],
		)
		if not file_paths:
			self._append_log("未选择文件。")
			return

		copied = 0
		for src in file_paths:
			src_path = Path(src)
			target = self._build_unique_target(self.raw_dir, src_path.name)
			shutil.copy2(src_path, target)
			copied += 1
			self._append_log(f"已上传: {src_path.name} -> data/raw_dir/{target.name}")

		self._append_log(f"上传完成，共 {copied} 个文件。")

	def _build_unique_target(self, target_dir: Path, filename: str) -> Path:
		candidate = target_dir / filename
		if not candidate.exists():
			return candidate

		stem = candidate.stem
		suffix = candidate.suffix
		index = 2
		while True:
			candidate = target_dir / f"{stem}_{index}{suffix}"
			if not candidate.exists():
				return candidate
			index += 1

	def _build_python_candidates(self) -> list[str]:
		return build_python_candidates(
			workspace_root=self.workspace_root,
			script_dir=self.script_dir,
			no_window_creationflags=_no_window_creationflags,
		)

	def _discover_python_from_where(self) -> list[str]:
		return discover_python_from_where(_no_window_creationflags)

	def _python_supports_module(self, python_cmd: str, module_name: str) -> bool:
		return python_supports_module(
			python_cmd=python_cmd,
			module_name=module_name,
			no_window_creationflags=_no_window_creationflags,
		)

	def _resolve_python_executable(self, required_module: str | None = None) -> str:
		return resolve_python_executable(
			workspace_root=self.workspace_root,
			script_dir=self.script_dir,
			no_window_creationflags=_no_window_creationflags,
			required_module=required_module,
		)

	def _resolve_python_for_module(self, required_module: str) -> str | None:
		return resolve_python_for_module(
			workspace_root=self.workspace_root,
			script_dir=self.script_dir,
			no_window_creationflags=_no_window_creationflags,
			required_module=required_module,
		)

	def _resolve_python_for_modules(self, required_modules: list[str]) -> str | None:
		for candidate in self._build_python_candidates():
			if any(sep in candidate for sep in ("\\", "/")) and not Path(candidate).exists():
				continue
			if all(self._python_supports_module(candidate, module_name) for module_name in required_modules):
				return candidate
		return None

	def _run_rag_qa_worker(
		self,
		python_cmd: str,
		question: str,
		base_url: str,
		api_key: str,
		model: str,
		embed_model: str,
		session_idx: int,
		mode: str,
		search_mode: str,
	) -> None:
		tmp_path = ""
		start_time = time.perf_counter()
		mode_label = {"local": "本地模型", "deepseek": "DeepSeek", "reasoner": "深度思考"}.get(mode, "本地模型")

		try:
			with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
				tmp_path = tmp.name

			memory_context = self._load_memory_context_for_session(session_idx)

			command = [
				python_cmd,
				"-u",
				str(self.script_dir / "ask_rag.py"),
				"--question",
				question,
				"--documents-dir",
				str(self.documents_dir),
				"--index-dir",
				str(self.vector_index_dir),
				"--backend",
				"faiss",
				"--search-mode",
				("local_only" if str(search_mode).strip().lower() in {"local", "local_only", "local-only"} else "hybrid"),
				"--top-k",
				"5",
				"--embedding-model",
				embed_model,
				"--api-url",
				base_url,
				"--api-key",
				api_key,
				"--model",
				model,
				"--timeout",
				str(TIMEOUT),
				"--call-type",
				"answer",
				"--output-json",
				tmp_path,
				"--stream",
			]
			if memory_context:
				command.extend(["--memory-context", memory_context])
			if mode == "local":
				command.append("--allow-local-fallback")

			creationflags = _no_window_creationflags()
			env = os.environ.copy()
			env["PYTHONUNBUFFERED"] = "1"
			env["PYTHONIOENCODING"] = "utf-8"
			env["PYTHONUTF8"] = "1"
			# Pass local LLM config to subprocess for local mode.
			if "AI_SUMMARY_LOCAL_LLM_URL" in os.environ:
				env["AI_SUMMARY_LOCAL_LLM_URL"] = os.environ["AI_SUMMARY_LOCAL_LLM_URL"]
			if "AI_SUMMARY_LOCAL_LLM_MODEL" in os.environ:
				env["AI_SUMMARY_LOCAL_LLM_MODEL"] = os.environ["AI_SUMMARY_LOCAL_LLM_MODEL"]
			if "AI_SUMMARY_LOCAL_LLM_API_KEY" in os.environ:
				env["AI_SUMMARY_LOCAL_LLM_API_KEY"] = os.environ["AI_SUMMARY_LOCAL_LLM_API_KEY"]
			if "AI_SUMMARY_LOCAL_LLM_MAX_CONTEXT_CHARS" in os.environ:
				env["AI_SUMMARY_LOCAL_LLM_MAX_CONTEXT_CHARS"] = os.environ["AI_SUMMARY_LOCAL_LLM_MAX_CONTEXT_CHARS"]
			if "AI_SUMMARY_LOCAL_LLM_MAX_CHARS_PER_DOC" in os.environ:
				env["AI_SUMMARY_LOCAL_LLM_MAX_CHARS_PER_DOC"] = os.environ["AI_SUMMARY_LOCAL_LLM_MAX_CHARS_PER_DOC"]
			proc = subprocess.Popen(
				command,
				cwd=str(self.script_dir),
				env=env,
				stdout=subprocess.PIPE,
				stderr=subprocess.STDOUT,
				text=True,
				encoding="utf-8",
				errors="replace",
				creationflags=creationflags,
			)
			with self.rag_process_lock:
				self.rag_active_process = proc

			streaming_answer: list[str] = []

			while proc.poll() is None:
				if self.rag_cancel_event.is_set():
					try:
						proc.terminate()
					except Exception:
						pass
					break

				if proc.stdout is None:
					time.sleep(0.05)
					continue

				line = proc.stdout.readline()
				if not line:
					continue
				line = line.rstrip()
				elapsed = time.perf_counter() - start_time

				if line.startswith("PROGRESS: "):
					progress_text = line[len("PROGRESS: "):]
					payload = json.dumps(
						{
							"session_idx": session_idx,
							"message": f"[{mode_label}] {progress_text} (已用时: {elapsed:.1f}s)",
						},
						ensure_ascii=False,
					)
					self.log_queue.put(("rag_progress_update", payload))
				elif line.startswith("STREAM_CHUNK_JSON: "):
					try:
						chunk = json.loads(line[len("STREAM_CHUNK_JSON: "):])
						streaming_answer.append(chunk)
						payload = json.dumps(
							{
								"session_idx": session_idx,
								"chunk": chunk,
								"full_answer": "".join(streaming_answer),
							},
							ensure_ascii=False,
						)
						self.log_queue.put(("rag_stream_chunk", payload))
					except (json.JSONDecodeError, ValueError):
						pass
				elif line.startswith("STREAM_CHUNK: "):
					chunk = line[len("STREAM_CHUNK: "):]
					streaming_answer.append(chunk)
					payload = json.dumps(
						{
							"session_idx": session_idx,
							"chunk": chunk,
							"full_answer": "".join(streaming_answer),
						},
						ensure_ascii=False,
					)
					self.log_queue.put(("rag_stream_chunk", payload))

			return_code = proc.wait()

			if self.rag_cancel_event.is_set():
				payload = json.dumps({"session_idx": session_idx, "message": "已中止"}, ensure_ascii=False)
				self.log_queue.put(("rag_qa_cancelled", payload))
				return

			if return_code != 0:
				remaining_output = ""
				if proc.stdout is not None:
					remaining_output = proc.stdout.read() or ""
				full_output = "".join(streaming_answer) + remaining_output
				msg = full_output.strip() or f"ask_rag.py 退出码: {return_code}"
				payload = json.dumps({"session_idx": session_idx, "message": msg}, ensure_ascii=False)
				self.log_queue.put(("rag_qa_error", payload))
				return

			if not tmp_path or not Path(tmp_path).exists():
				payload = json.dumps({"session_idx": session_idx, "message": "未生成 RAG Q&A 输出文件。"}, ensure_ascii=False)
				self.log_queue.put(("rag_qa_error", payload))
				return

			data = json.loads(Path(tmp_path).read_text(encoding="utf-8"))
			answer = str(data.get("answer", "")).strip()
			session_title = str(data.get("session_title", "")).strip()
			used_docs = data.get("used_context_docs", [])

			if streaming_answer:
				final_answer = "".join(streaming_answer).strip()
				if mode == "local" and isinstance(used_docs, list) and used_docs:
					final_answer = self._format_local_answer_with_refs(final_answer, used_docs)
					payload = json.dumps(
						{"session_idx": session_idx, "full_answer": final_answer},
						ensure_ascii=False,
					)
					self.log_queue.put(("rag_stream_chunk", payload))
			else:
				final_answer = answer

			if not final_answer:
				payload = json.dumps({"session_idx": session_idx, "message": "回答为空。"}, ensure_ascii=False)
				self.log_queue.put(("rag_qa_error", payload))
				return

			# Local-only title generation: no extra API request.
			if mode == "local":
				session_title = self._derive_local_session_title(question, final_answer)

			if session_title:
				session = self.rag_sessions[session_idx] if 0 <= session_idx < len(self.rag_sessions) else None
				if session is not None and not bool(session.get("title_locked", False)):
					payload = json.dumps(
						{"session_idx": session_idx, "title": session_title},
						ensure_ascii=False,
					)
					self.log_queue.put(("rag_session_title", payload))

			timing = data.get("elapsed_seconds")
			if timing is not None:
				self.log_queue.put(("log", f"RAG Q&A 完成，耗时 {timing:.1f}s"))
		except Exception as exc:  # noqa: BLE001
			payload = json.dumps({"session_idx": session_idx, "message": str(exc)}, ensure_ascii=False)
			self.log_queue.put(("rag_qa_error", payload))
		finally:
			with self.rag_process_lock:
				self.rag_active_process = None
			self.rag_cancel_event.clear()
			if tmp_path:
				try:
					Path(tmp_path).unlink(missing_ok=True)
				except Exception:
					pass
			self.log_queue.put(("rag_qa_done", ""))

	def run_batch_processing(self) -> None:
		if self.is_running:
			self._append_log("已有任务在运行，请等待结束。")
			return

		source = self.source_var.get().strip().lower()
		script_name_map = {
			"deepseek": "for_deepseek.py",
			"chatgpt": "for_chatgpt.py",
		}
		script_name = script_name_map.get(source)
		if not script_name:
			self._append_log(f"当前来源为 {source}，暂不支持。")
			return

		python_cmd = self._resolve_python_executable()
		script_path = self.script_dir / script_name
		if not script_path.exists():
			self._append_log(f"未找到批处理脚本: {script_path.name}")
			return

		command = [
			python_cmd,
			"-u",
			str(script_path),
			"--input-dir",
			str(self.raw_dir),
			"--output-dir",
			str(self.extracted_dir),
		]
		self._append_log(f"数据来源: {source}")
		self._append_log(f"解释器: {python_cmd}")
		self._run_command_async(command, f"执行格式批处理({script_path.name})")

	def run_ai_summary(self) -> None:
		if self.is_running:
			self._append_log("已有任务在运行，请等待结束。")
			return

		base_url = self.base_url_var.get().strip()
		model = self.model_var.get().strip()
		api_key = self._resolve_api_key().strip()
		if not base_url or not model or not api_key:
			self._append_log("请先完整填写 API_BASE_URL、MODEL、API_KEY。")
			return

		env = os.environ.copy()
		env["DEEPSEEK_BASE_URL"] = base_url
		env["DEEPSEEK_MODEL"] = model
		env["DEEPSEEK_API_KEY"] = api_key

		# Keep latest full key in memory so next masked view remains consistent.
		self.raw_api_key = api_key
		self.api_key_var.set(api_key)

		python_cmd = self._resolve_python_for_module("openai")
		if not python_cmd:
			self._append_log("未找到可导入 openai 的 Python 解释器。")
			self._append_log("请在可用环境安装 openai，或设置环境变量 AI_SUMMARY_PYTHON 指向正确 python.exe。")
			return
		selected_files, range_err = self._collect_extracted_files_in_selected_range()
		if range_err:
			self._append_log(range_err)
			return
		if not selected_files:
			self._append_log("当前日期范围内无可总结文件。")
			return

		temp_input = tempfile.TemporaryDirectory(prefix="ai_summary_filtered_")
		for file_path in selected_files:
			shutil.copy2(file_path, Path(temp_input.name) / file_path.name)

		command = [
			python_cmd,
			"-u",
			str(self.script_dir / "summarize.py"),
			"--input-dir",
			str(temp_input.name),
			"--output-dir",
			str(self.summarize_dir),
		]
		self._append_log(f"日期范围内匹配文件: {len(selected_files)}")
		self._append_log(f"解释器: {python_cmd}")
		self._run_command_async(command, "执行 AI 总结(summarize.py)", env=env, on_finish=temp_input.cleanup)

	def estimate_tokens_for_selected_range(self) -> None:
		files, range_err = self._collect_extracted_files_in_selected_range()
		if range_err:
			self._append_log(range_err)
			return
		if not files:
			self._append_log("当前日期范围内无文件，无法估计 token。")
			return

		try:
			import transformers  # type: ignore
		except Exception:
			self._append_log("Token估计失败：当前解释器缺少 transformers。")
			self._append_log("请安装：pip install transformers")
			return

		tokenizer_dir = self.script_dir / "deepseek_v3_tokenizer"
		if not tokenizer_dir.exists():
			self._append_log(f"Token估计失败：未找到目录 {tokenizer_dir}")
			return

		self._append_log("开始估计 token，请稍候...")
		try:
			tokenizer = transformers.AutoTokenizer.from_pretrained(
				str(tokenizer_dir),
				trust_remote_code=True,
			)
			total_tokens = 0
			total_chars = 0
			for file_path in files:
				text = file_path.read_text(encoding="utf-8", errors="replace")
				total_chars += len(text)
				total_tokens += len(tokenizer.encode(text, add_special_tokens=False))
		except Exception as exc:  # noqa: BLE001
			self._append_log(f"Token估计失败：{exc}")
			return

		self._append_log(f"Token估计完成：文件 {len(files)} 个，字符 {total_chars}，约 {total_tokens} tokens。")

	def run_output_classification(self) -> None:
		if self.is_running:
			self._append_log("已有任务在运行，请等待结束。")
			return

		python_cmd = self._resolve_python_executable()
		command = [
			python_cmd,
			"-u",
			str(self.script_dir / "output_classification_workflow.py"),
		]
		self._append_log(f"解释器: {python_cmd}")
		self._append_log("执行输出分类工作流：")
		self._append_log("  步骤1: 拆分多主题文档 (summarize_dir → split_dir)")
		self._append_log("  步骤2: 分类移动文档 (split_dir → documents)")
		self._append_log("  步骤3: 分类移动文档 (summarize_dir → documents)")
		self._run_command_async(command, "执行输出分类工作流(output_classification_workflow.py)")

	def run_sync_missing_embeddings(self) -> None:
		self._refresh_rag_model_label()
		if self.is_running:
			self._append_log("已有任务在运行，请等待结束。")
			return

		model = self._resolve_embedding_model_for_rag()
		if not model:
			self._append_log("补齐向量失败：请先填写 MODEL（或设置 LOCAL_EMBEDDING_MODEL）。")
			return
		if not self._is_local_embedding_model(model):
			self._append_log("补齐向量失败：仅支持本地 embedding 模型。")
			self._append_log("请设置环境变量 LOCAL_EMBEDDING_MODEL，例如 BAAI/bge-base-zh-v1.5。")
			return

		python_cmd = self._resolve_python_for_module("sentence_transformers")
		if not python_cmd:
			self._append_log("未找到可导入 sentence_transformers 的 Python 解释器。")
			self._append_log("请先安装：pip install sentence-transformers")
			return

		command = [
			python_cmd,
			"-u",
			str(self.script_dir / "rag_vector_index.py"),
			"--documents-dir",
			str(self.documents_dir),
			"--index-dir",
			str(self.vector_index_dir),
			"--backend",
			"faiss",
			"--sync-missing",
			"--embedding-model",
			model,
			"--timeout",
			str(TIMEOUT),
		]
		self._append_log(f"补齐向量：使用本地 embedding 模型 {model}")
		self._append_log(f"解释器: {python_cmd}")
		self._run_command_async(command, "执行补齐向量(rag_vector_index.py --sync-missing)")

	def _run_command_async(
		self,
		command: list[str],
		task_name: str,
		env: dict[str, str] | None = None,
		on_finish: Callable[[], None] | None = None,
	) -> None:
		self._set_running(True)
		self._append_log(task_name)
		self._append_log("命令: " + " ".join(command))

		effective_env = os.environ.copy()
		if env:
			effective_env.update(env)
		effective_env["PYTHONUNBUFFERED"] = "1"
		effective_env["PYTHONIOENCODING"] = "utf-8"

		thread = threading.Thread(
			target=self._run_command_worker,
			args=(command, effective_env, on_finish),
			daemon=True,
		)
		thread.start()

	def _run_command_worker(
		self,
		command: list[str],
		env: dict[str, str] | None = None,
		on_finish: Callable[[], None] | None = None,
	) -> None:
		try:
			creationflags = 0
			if sys.platform == "win32":
				creationflags = subprocess.CREATE_NO_WINDOW

			process = subprocess.Popen(
				command,
				cwd=str(self.script_dir),
				env=env,
				stdout=subprocess.PIPE,
				stderr=subprocess.STDOUT,
				text=True,
				encoding="utf-8",
				errors="replace",
				bufsize=1,
				creationflags=creationflags,
			)
			safe_stdout = process.stdout
			if safe_stdout is not None:
				for line in safe_stdout:
					self._queue_log(line.rstrip())
			return_code = process.wait()
			if return_code == 0:
				self._queue_log("任务完成。")
			else:
				self._queue_log(f"任务失败，退出码: {return_code}")
		except Exception as exc:  # noqa: BLE001
			self._queue_log(f"执行异常: {exc}")
		finally:
			if on_finish is not None:
				try:
					on_finish()
				except Exception as exc:  # noqa: BLE001
					self._queue_log(f"清理临时目录失败: {exc}")
			self.log_queue.put(("done", ""))


def main() -> None:
	_relaunch_with_pythonw_on_windows(Path(__file__).resolve())
	_hide_console_on_windows()
	root = tk.Tk()
	app = SummaryGuiApp(root)
	app._append_log("程序已启动。")
	app._append_log("请先上传源文件，再选择数据来源并执行操作。")
	if TkDateEntry is None:
		app._append_log("未检测到 tkcalendar，日期输入已降级到内置日历。")
		app._append_log("建议在当前虚拟环境执行: .\\.venv\\Scripts\\python.exe -m pip install tkcalendar")
	root.mainloop()


if __name__ == "__main__":
	main()
