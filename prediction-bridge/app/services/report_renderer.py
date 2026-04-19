"""Render prediction JSON to Markdown via a Jinja2 template."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ..core.config import ReportSection
from ..core.logging import logger


SEGMENTS = ("T1", "T2", "T3", "T4", "T5")
METRICS = ("调频容量需求", "边际排序价格", "市场出清价格_预测均价")


class ReportRenderer:
    def __init__(self, cfg: ReportSection) -> None:
        self._cfg = cfg
        template_path = Path(cfg.template_path)
        if not template_path.is_absolute():
            template_path = Path.cwd() / template_path
        if not template_path.is_file():
            raise FileNotFoundError(
                f"report template not found: {template_path}"
            )
        self._template_dir = template_path.parent
        self._template_name = template_path.name
        self._env = Environment(
            loader=FileSystemLoader(str(self._template_dir)),
            autoescape=select_autoescape(enabled_extensions=()),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        Path(cfg.output_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #

    def render(
        self,
        *,
        predict_date: str,
        data_date: str,
        trace_id: str,
        prediction: Dict[str, Any],
        output_dir: Optional[Path] = None,
    ) -> Path:
        out_dir = Path(output_dir) if output_dir else Path(self._cfg.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"prediction_{predict_date}.md"

        segments_for_template = self._normalise_segments(prediction.get("segments", {}))
        context = {
            "predict_date": predict_date,
            "data_date": data_date,
            "trace_id": trace_id,
            "model_version": prediction.get("model_version", "unknown"),
            "generated_at": prediction.get(
                "generated_at", datetime.now(timezone.utc).isoformat()
            ),
            "rendered_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "segments": segments_for_template,
            "segment_keys": list(SEGMENTS),
            "metric_keys": list(METRICS),
            "raw_json": json.dumps(prediction, ensure_ascii=False, indent=2),
        }

        tpl = self._env.get_template(self._template_name)
        rendered = tpl.render(**context)
        out_path.write_text(rendered, encoding="utf-8")
        logger.info(
            "report rendered",
            extra={"path": str(out_path), "predict_date": predict_date},
        )
        return out_path

    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalise_segments(segments: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Fill in missing segments/metrics with ``None`` for stable rendering.

        Also tolerates the ``市场出清价格(预测均价)`` variant seen in ``/results/{file}``.
        """
        normalised: Dict[str, Dict[str, Any]] = {}
        for seg in SEGMENTS:
            raw = segments.get(seg, {}) or {}
            row: Dict[str, Any] = {}
            for metric in METRICS:
                if metric in raw:
                    row[metric] = raw[metric]
                elif metric == "市场出清价格_预测均价" and "市场出清价格(预测均价)" in raw:
                    row[metric] = raw["市场出清价格(预测均价)"]
                else:
                    row[metric] = None
            normalised[seg] = row
        return normalised
