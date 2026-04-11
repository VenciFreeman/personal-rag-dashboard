from __future__ import annotations

"""Temporary support compatibility barrel.

This module exists only to preserve the historical import surface while owner
logic lives in the split modules below. Guardrail: no new owner logic belongs
here.
"""

from nav_dashboard.web.services.ontologies.ontology_loader import get_load_statuses as _get_ontology_load_statuses
from nav_dashboard.web.services.tooling.tool_executor import build_confirmation_payload, execute_tool_phase, resolve_allowed_plan
from nav_dashboard.web.services.tooling.tool_option_assembly import assemble_execution_tool_options as assemble_execution_tool_options_layer
from nav_dashboard.web.services.tooling.tool_planning_pipeline import run_tool_planning_pipeline

from .support_common import *
from .support_answering import *
from .support_observability import *
from .support_retrieval import *
