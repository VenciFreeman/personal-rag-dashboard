"""nav_dashboard/web/api/benchmark.py
性能基准测试 API 路由（/api/benchmark）

职责：
  POST /api/benchmark/run      — 运行基准测试，通过 SSE（Server-Sent Events）实时推送进度
                                  请求体：{ modules, query_count, run_id }
  DELETE /api/benchmark/run    — 中止当前进行中的测试（通过 run_id 标记取消标志位）
  GET  /api/benchmark/history  — 获取最近 N 次测试结果（存储于 data/benchmark_results.json）
  DELETE /api/benchmark/history — 清空历史

测试流程：
  1. 按 query_count 从 SHORT/MEDIUM/LONG 三个中文 Query 池各随机采样
  2. 对每条 query 顺序调用 RAG 或 Agent 接口，记录端到端时延及各阶段指标
  3. 每批（短/中/长）完成后聚合：avg/p50/p95 时延、成功率
  4. 全部完成后生成 result 事件并写入历史文件（中止时丢弃，不写入）

每次测试通过 run_id 绑定一个取消标志，DELETE 端点设置标志位后，
生成器循环在下条 query 开始前检查并提前退出。
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from web.config import AI_SUMMARY_URL_OVERRIDE

router = APIRouter(prefix="/api/benchmark", tags=["benchmark"])

APP_DIR = Path(__file__).resolve().parent.parent  # nav_dashboard/web/
# Store benchmark data in its own subfolder, fully isolated from dashboard data
BENCHMARK_FILE = APP_DIR.parent / "data" / "benchmark" / "results.json"
BENCHMARK_HISTORY_MAX = 5

# ─── Query pools (20 per length) ─────────────────────────────────────────────

SHORT_QUERIES: list[str] = [
    "机器学习是什么",
    "量子计算原理",
    "通货膨胀的影响",
    "区块链技术",
    "人工智能伦理",
    "气候变化原因",
    "深度学习框架",
    "大数据处理",
    "微服务架构",
    "认知偏差类型",
    "股市波动因素",
    "自然语言处理",
    "操作系统调度",
    "碳中和概念",
    "全球化影响",
    "基因编辑技术",
    "密码学基础",
    "云计算服务",
    "数字货币监管",
    "历史唯物主义",
]

MEDIUM_QUERIES: list[str] = [
    "深度学习中的注意力机制是如何工作的，有哪些主要应用场景",
    "比较分析传统关系型数据库与NoSQL数据库的优缺点",
    "量子纠缠现象对未来量子通信技术的潜在影响是什么",
    "全球供应链断裂对新兴市场国家经济发展的具体影响",
    "人工智能在医疗保健领域的主要应用案例和挑战",
    "现代微服务架构中容器化与服务网格的关系是什么",
    "碳排放交易市场的运作机制及其减排效果评估",
    "认知行为疗法与正念冥想在治疗焦虑症方面的对比",
    "推荐系统中协同过滤与内容过滤算法的优缺点对比",
    "区块链技术在供应链追溯中的实际应用案例分析",
    "联邦学习如何在保护数据隐私的同时实现模型训练",
    "多模态大语言模型的训练方式与单模态模型的主要区别",
    "分析当前全球半导体短缺危机的原因与长期解决路径",
    "生物信息学中基因组序列比对的主要算法思路",
    "现代城市规划如何在经济发展和生态保护之间取得平衡",
    "网络安全中零信任架构的核心理念和实施步骤",
    "强化学习在机器人控制领域的典型算法和训练策略",
    "阐述全球数字货币监管框架的现状和主要挑战",
    "电影叙事结构中非线性叙事的艺术价值和典型案例",
    "精益生产方法论的核心要素与适用的行业场景",
]

LONG_QUERIES: list[str] = [
    "请详细解释Transformer架构中多头注意力机制的数学原理，与传统RNN相比有哪些计算效率上的优势，以及为什么这种架构特别适合并行训练，并给出一些近年来基于此架构的重要变体",
    "从经济学和社会学两个角度深入分析人工智能自动化对未来劳动力市场的影响，包括哪些职业最容易被替代、哪些技能会变得更有价值，以及政府和教育机构应如何应对这一趋势",
    "请系统地比较PostgreSQL、MongoDB和Redis三种数据库在数据模型、一致性保证、水平扩展能力和典型使用场景上的差异，并给出在设计分布式系统时如何选择合适数据库的建议",
    "详细分析气候变化对全球粮食安全的多维影响，包括对不同地区农业产量的影响、水资源变化、极端天气增加对物流的干扰，以及国际社会应如何协作建立更具弹性的全球粮食系统",
    "解释CRISPR-Cas9基因编辑技术的分子机制，重点讲解gRNA的设计原则、HDR与NHEJ修复路径的区别，讨论其在遗传病治疗中的临床试验现状，并分析目前面临的技术壁垒和伦理争论",
    "请从架构设计、性能调优和运维实践三个角度，系统讲解在高并发场景下如何设计一个支持百万级QPS的分布式缓存系统，包括数据分片策略、一致性哈希、热点问题处理和故障恢复机制",
    "深入分析量子计算对现有密码学体系的威胁，特别是Shor算法对RSA和ECC算法的破坏能力，讨论后量子密码学的主要候选方案（如NTRU、CRYSTALS-Kyber等），以及当前迁移计划的挑战",
    "请综合分析近十年来中美关系的结构性变化，包括贸易摩擦的深层经济逻辑、科技脱钩的战略背景、台湾问题的地缘政治敏感性，以及这种竞争格局对全球多边主义体系的长远影响",
    "从神经科学和计算认知科学两个视角，详细解释人类工作记忆和长期记忆的编码、存储和提取机制，类比讨论大型语言模型的记忆与人类记忆的根本差异，以及这对AI系统设计的启示",
    "请详细说明微服务架构下的服务治理体系，涵盖服务注册与发现、负载均衡策略、熔断器模式、限流算法（令牌桶vs漏桶）、分布式追踪和日志聚合，并讨论Service Mesh在简化复杂性方面的作用",
    "比较分析凯恩斯主义经济学和货币主义在应对经济衰退时的政策主张差异，结合2008年金融危机和2020年新冠疫情的政策实践，评估两种理论框架在现代经济环境中的适用性和局限性",
    "请系统梳理大规模语言模型的对齐技术发展历程，包括早期InstructGPT中的RLHF方法、Constitutional AI的核心思路、直接偏好优化DPO的原理，以及当前对齐研究面临的主要挑战",
    "分析数字平台经济下网络效应、数据垄断和算法推荐如何共同形成强大的市场壁垒，探讨欧盟数字市场法和中国平台反垄断监管的不同思路及对全球互联网产业格局的影响",
    "请详细讲解强化学习中的策略梯度方法，包括REINFORCE算法、Actor-Critic架构、PPO裁剪目标函数的数学推导，并解释为什么PPO成为当前大模型RLHF实践中的主流算法选择",
    "从城市规划、交通工程和行为经济学三个角度深入分析如何设计更高效的公共交通系统，包括BRT与地铁的适用条件、需求响应式交通的新兴实践，以及数字化技术如何改变城市出行模式",
    "请综述表观遗传学的主要调控机制（DNA甲基化、组蛋白修饰、非编码RNA等），解释这些机制如何介导基因-环境相互作用，并讨论表观遗传修饰在癌症发生发展中的功能及靶向治疗前景",
    "详细分析供应链金融的主要模式（应收账款融资、预付款融资、存货融资），探讨区块链和物联网技术如何提升供应链金融的信息透明度，以及在中小企业融资难背景下的实践价值",
    "请从哲学、伦理学和法律三个维度，深入探讨自动驾驶汽车在面临不可避免事故时的决策伦理，分析不同国家的立法思路，以及这些讨论如何推动AI系统责任归属理论的发展",
    "系统讲解现代编译器的前端、中端和后端架构，重点介绍LLVM IR的设计哲学、各类优化Pass的工作原理（常量折叠、循环展开、内联优化），以及AOT与JIT编译策略各自的性能取舍",
    "请综合分析Web3和去中心化金融（DeFi）生态系统的核心组件（智能合约、AMM、流动性挖矿、Layer2扩展方案），评估其技术可行性与经济模型的可持续性，以及监管框架缺失带来的系统性风险",
]

QUERY_POOL: dict[str, list[str]] = {
    "short": SHORT_QUERIES,
    "medium": MEDIUM_QUERIES,
    "long": LONG_QUERIES,
}

# ─── URL resolution ───────────────────────────────────────────────────────────

def _ai_summary_base() -> str:
    raw = (os.getenv("NAV_DASHBOARD_AI_SUMMARY_INTERNAL_URL", "") or "").strip().rstrip("/")
    if raw:
        return raw
    if AI_SUMMARY_URL_OVERRIDE:
        parsed = urlparse.urlparse(AI_SUMMARY_URL_OVERRIDE)
        if parsed.scheme and parsed.hostname:
            port = parsed.port or 8000
            return f"{parsed.scheme}://{parsed.hostname}:{port}"
    return "http://127.0.0.1:8000"


# ─── HTTP helper ──────────────────────────────────────────────────────────────

def _post_json(url: str, body: dict[str, Any], timeout: int = 180) -> dict[str, Any]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw.strip() else {}
    except urlerror.HTTPError as exc:
        snippet = ""
        try:
            snippet = exc.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return {"_error": f"HTTP {exc.code}: {snippet}"}
    except Exception as exc:
        return {"_error": str(exc)[:200]}


# ─── Per-query runners ────────────────────────────────────────────────────────

def _run_rag_query(ai_base: str, question: str) -> dict[str, Any]:
    url = f"{ai_base}/api/rag/ask"
    t0 = time.perf_counter()
    resp = _post_json(url, {"question": question, "mode": "local", "search_mode": "local_only", "no_embed_cache": True, "benchmark_mode": True})
    wall_clock = round(time.perf_counter() - t0, 3)
    error = resp.get("_error") or None
    timings = resp.get("timings") if isinstance(resp.get("timings"), dict) else {}
    elapsed = float(resp.get("elapsed_seconds") or 0.0)
    no_context = 1 if float(timings.get("local_after_threshold", -1) or -1) == 0 else 0
    return {"wall_clock_s": wall_clock, "elapsed_s": elapsed, "error": error, "timings": timings, "no_context": no_context}


def _run_agent_query(self_base: str, question: str) -> dict[str, Any]:
    url = f"{self_base}/api/agent/chat"
    t0 = time.perf_counter()
    resp = _post_json(
        url,
        {"question": question, "backend": "local", "search_mode": "local_only", "deny_over_quota": True, "benchmark_mode": True},
        timeout=240,
    )
    wall_clock = round(time.perf_counter() - t0, 3)
    error = resp.get("_error") or None
    timings = resp.get("timings") if isinstance(resp.get("timings"), dict) else {}
    no_context = int(timings.get("no_context", 0) or 0)
    return {
        "wall_clock_s": wall_clock,
        "error": error,
        "timings": timings,
        "no_context": no_context,
    }


# ─── Aggregation ──────────────────────────────────────────────────────────────

def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * max(0.0, min(1.0, p / 100.0))))
    return ordered[idx]


def _aggregate(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {"count": 0, "errors": 0}
    errors = sum(1 for r in records if r.get("error"))
    valid = [r for r in records if not r.get("error")]
    out: dict[str, Any] = {"count": len(records), "errors": errors}
    if not valid:
        return out
    n = len(valid)
    out["avg_wall_clock_s"] = round(sum(r["wall_clock_s"] for r in valid) / n, 3)
    # Wall-clock percentiles
    wall_vals = [r["wall_clock_s"] for r in valid]
    out["p50_wall_clock_s"] = round(_percentile(wall_vals, 50), 3)
    out["p95_wall_clock_s"] = round(_percentile(wall_vals, 95), 3)
    out["p99_wall_clock_s"] = round(_percentile(wall_vals, 99), 3)

    if any("elapsed_s" in r for r in valid):
        vals = [float(r.get("elapsed_s") or 0) for r in valid]
        out["avg_elapsed_s"] = round(sum(vals) / n, 3)

    # No-context rate
    no_ctx = sum(1 for r in valid if int(r.get("no_context", 0) or 0))
    out["no_context_rate"] = round(no_ctx / n, 3) if n else 0.0

    # Per-stage averages and percentiles from timings dicts
    sums: dict[str, float] = {}
    cnts: dict[str, int] = {}
    vals_by_stage: dict[str, list[float]] = {}
    for r in valid:
        for k, v in (r.get("timings") or {}).items():
            try:
                fv = float(v)
            except Exception:
                continue
            if fv < 0:
                continue
            sums[k] = sums.get(k, 0.0) + fv
            cnts[k] = cnts.get(k, 0) + 1
            vals_by_stage.setdefault(k, []).append(fv)
    for k, s in sums.items():
        c = cnts[k]
        out[f"avg_{k}_s"] = round(s / c, 3) if c else 0.0
    # Percentiles for key timing stages
    for k, vlist in vals_by_stage.items():
        if len(vlist) >= 1:
            out[f"p50_{k}_s"] = round(_percentile(vlist, 50), 3)
            out[f"p95_{k}_s"] = round(_percentile(vlist, 95), 3)
            out[f"p99_{k}_s"] = round(_percentile(vlist, 99), 3)
    return out


# ─── History storage ──────────────────────────────────────────────────────────

def _load_history() -> list[dict[str, Any]]:
    if not BENCHMARK_FILE.exists():
        return []
    try:
        raw = json.loads(BENCHMARK_FILE.read_text(encoding="utf-8"))
        return list(raw.get("results", [])) if isinstance(raw, dict) else []
    except Exception:
        return []


def _save_result(result: dict[str, Any]) -> None:
    history = _load_history()
    history.append(result)
    history = history[-BENCHMARK_HISTORY_MAX:]
    BENCHMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    BENCHMARK_FILE.write_text(
        json.dumps({"results": history}, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ─── Benchmark generator (SSE source) ────────────────────────────────────────

def _run_benchmark(modules: list[str], query_count: int, ai_base: str, self_base: str):
    lengths = ["short", "medium", "long"]
    total = len(modules) * 3 * query_count
    done = 0
    rag_recs: dict[str, list[dict[str, Any]]] = {l: [] for l in lengths}
    agent_recs: dict[str, list[dict[str, Any]]] = {l: [] for l in lengths}

    yield {"type": "progress", "message": f"准备运行 {total} 项测试...", "current": 0, "total": total}

    for length in lengths:
        pool = QUERY_POOL[length]
        queries = pool[: min(query_count, len(pool))]

        if "rag" in modules:
            for i, q in enumerate(queries):
                label = q[:22] + ("..." if len(q) > 22 else "")
                yield {
                    "type": "progress",
                    "message": f"[RAG / {length}] {i + 1}/{len(queries)}: {label}",
                    "current": done,
                    "total": total,
                }
                rec = _run_rag_query(ai_base, q)
                rag_recs[length].append(rec)
                done += 1

        if "agent" in modules:
            for i, q in enumerate(queries):
                label = q[:22] + ("..." if len(q) > 22 else "")
                yield {
                    "type": "progress",
                    "message": f"[Agent / {length}] {i + 1}/{len(queries)}: {label}",
                    "current": done,
                    "total": total,
                }
                rec = _run_agent_query(self_base, q)
                agent_recs[length].append(rec)
                done += 1

    result: dict[str, Any] = {
        "id": str(uuid4())[:8],
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "config": {"modules": modules, "query_count_per_type": query_count},
    }
    if "rag" in modules:
        all_rag = [r for l in lengths for r in rag_recs[l]]
        result["rag"] = {
            "by_length": {l: _aggregate(rag_recs[l]) for l in lengths},
            "global": _aggregate(all_rag),
        }
    if "agent" in modules:
        all_agent = [r for l in lengths for r in agent_recs[l]]
        result["agent"] = {
            "by_length": {l: _aggregate(agent_recs[l]) for l in lengths},
            "global": _aggregate(all_agent),
        }

    _save_result(result)
    yield {"type": "result", "data": result, "current": total, "total": total}


# ─── Pydantic models ──────────────────────────────────────────────────────────

class RunPayload(BaseModel):
    modules: list[str] = Field(default_factory=lambda: ["rag"])
    query_count_per_type: int = Field(default=3, ge=1, le=20)


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/run")
def post_run(payload: RunPayload, request: Request) -> StreamingResponse:
    modules = [m for m in payload.modules if m in ("rag", "agent")]
    if not modules:
        raise HTTPException(status_code=400, detail="请至少选择一个测试模块（rag 或 agent）")
    query_count = max(1, min(20, payload.query_count_per_type))
    ai_base = _ai_summary_base()
    self_base = str(request.base_url).rstrip("/")

    def event_stream():
        try:
            for event in _run_benchmark(modules, query_count, ai_base, self_base):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/history")
def get_history() -> dict[str, Any]:
    return {"results": _load_history()}


@router.delete("/history")
def clear_history() -> dict[str, Any]:
    if BENCHMARK_FILE.exists():
        BENCHMARK_FILE.write_text(
            json.dumps({"results": []}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return {"ok": True}
