"""Platform entrypoint for shared personal-ai-stack capabilities.

This package is the stable, high-level boundary that app code should prefer.
Low-level implementation modules still exist under ``core_service.*``, but the
intended public platform surface is grouped around explicit service domains:

- ``core_service.auth``: application auth/bootstrap helpers
- ``core_service.llm``: model request helpers and streaming adapters
- ``core_service.reporting``: report state, persistence, and report-engine IO
- ``core_service.observability``: traces, usage, and export utilities
- ``core_service.feedback``: shared chat feedback persistence helpers
- ``core_service.tickets``: shared ticket persistence and aggregation helpers
- ``core_service.runtime_data``: shared runtime-data path resolution and migration helpers

Configuration access is re-exported here because it is effectively part of the
platform contract used by every app entrypoint.
"""

from importlib import import_module

from core_service.config import CoreSettings, display_model_name, get_settings


_LAZY_EXPORTS = {
	"auth",
	"feedback",
	"llm",
	"observability",
	"report_engine",
	"reporting",
	"runtime_data",
	"tickets",
	"trace_store",
}


def __getattr__(name: str):
	if name in _LAZY_EXPORTS:
		module = import_module(f"core_service.{name}")
		globals()[name] = module
		return module
	raise AttributeError(f"module 'core_service' has no attribute {name!r}")


def __dir__() -> list[str]:
	return sorted(set(globals()) | _LAZY_EXPORTS)

__all__ = [
	"CoreSettings",
	"auth",
	"feedback",
	"display_model_name",
	"get_settings",
	"llm",
	"observability",
	"report_engine",
	"reporting",
	"runtime_data",
	"tickets",
	"trace_store",
]
