"""
输出分类工作流wrapper脚本

执行两步操作：
1. 拆分多主题文档：从summarize_dir拆分到split_dir
2. 分类移动文档：从split_dir移动到documents/

用法：
    python output_classification_workflow.py [--dry-run]
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List


def configure_stdio_utf8() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def run_step(description: str, command: List[str]) -> bool:
    """执行一个步骤，返回是否成功"""
    print(f"\n{'='*60}")
    print(f"步骤: {description}")
    print(f"{'='*60}\n")
    print(f"命令: {' '.join(str(c) for c in command)}\n")
    
    try:
        result = subprocess.run(
            command,
            check=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        print(result.stdout)
        print(f"\n✓ {description} - 成功")
        return True
    except subprocess.CalledProcessError as e:
        print(e.stdout)
        print(f"\n✗ {description} - 失败 (退出码: {e.returncode})")
        return False
    except Exception as e:
        print(f"\n✗ {description} - 异常: {e}")
        return False


def main() -> int:
    configure_stdio_utf8()
    
    parser = argparse.ArgumentParser(
        description='输出分类工作流：拆分多主题文档 → 分类移动'
    )
    parser.add_argument('--dry-run', action='store_true', help='预览模式，不实际创建/移动文件')
    args = parser.parse_args()
    
    # 确定路径
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent
    summarize_dir = root_dir / "data" / "summarize_dir"
    split_dir = root_dir / "data" / "split_dir"
    documents_dir = root_dir / "documents"
    
    python_exe = sys.executable
    
    print("输出分类工作流")
    print(f"Python: {python_exe}")
    print(f"源目录: {summarize_dir}")
    print(f"拆分目录: {split_dir}")
    print(f"文档目录: {documents_dir}")
    print(f"预览模式: {'是' if args.dry_run else '否'}")
    
    # 确保目录存在
    split_dir.mkdir(parents=True, exist_ok=True)
    documents_dir.mkdir(parents=True, exist_ok=True)
    
    # 步骤1: 拆分多主题文档
    step1_command = [
        python_exe,
        "-u",
        str(script_dir / "batch_split_documents.py"),
        str(summarize_dir),
        "--output-dir",
        str(split_dir),
        "--move-originals",  # 拆分成功后将原文件移到archived_multi_topic
        "--no-recursive",  # 只处理summarize_dir根目录
    ]
    
    if args.dry_run:
        step1_command.append("--dry-run")
    
    success_step1 = run_step("拆分多主题文档", step1_command)
    
    if not success_step1:
        print("\n警告：拆分步骤失败，但将继续执行分类步骤（处理未拆分和已有的文件）")
    
    # 步骤2: 分类移动文档
    # 处理split_dir中的所有文件
    step2_command = [
        python_exe,
        "-u",
        str(script_dir / "move_summaries_by_category.py"),
        "--input-dir",
        str(split_dir),
        "--documents-dir",
        str(documents_dir),
    ]
    
    if args.dry_run:
        step2_command.append("--dry-run")
    
    success_step2 = run_step("分类移动文档 (split_dir → documents)", step2_command)
    
    # 步骤3: 同时处理summarize_dir中剩余的单主题文档
    step3_command = [
        python_exe,
        "-u",
        str(script_dir / "move_summaries_by_category.py"),
        "--input-dir",
        str(summarize_dir),
        "--documents-dir",
        str(documents_dir),
    ]
    
    if args.dry_run:
        step3_command.append("--dry-run")
    
    success_step3 = run_step("分类移动文档 (summarize_dir → documents)", step3_command)
    
    # 汇总结果
    print(f"\n{'='*60}")
    print("工作流完成")
    print(f"{'='*60}")
    print(f"步骤1 (拆分): {'成功' if success_step1 else '失败'}")
    print(f"步骤2 (分类split_dir): {'成功' if success_step2 else '失败'}")
    print(f"步骤3 (分类summarize_dir): {'成功' if success_step3 else '失败'}")
    
    if args.dry_run:
        print("\n这是预览模式，未实际创建/移动文件")
        print("移除 --dry-run 参数以执行实际操作")
    
    # 如果任何步骤失败，返回1
    if not (success_step2 and success_step3):
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
