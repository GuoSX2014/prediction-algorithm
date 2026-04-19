"""Report rendering snapshot tests."""

from __future__ import annotations

from pathlib import Path

from app.core.config import ReportSection
from app.services.report_renderer import ReportRenderer


def _sample_prediction(date: str = "2026-03-27") -> dict:
    return {
        "date": date,
        "segments": {
            "T1": {"调频容量需求": 3521.234, "边际排序价格": 7.856, "市场出清价格_预测均价": 8.123},
            "T2": {"调频容量需求": 3412.567, "边际排序价格": 6.789, "市场出清价格_预测均价": 7.234},
            "T3": {"调频容量需求": 3298.901, "边际排序价格": 12.345, "市场出清价格_预测均价": 11.678},
            "T4": {"调频容量需求": 3156.789, "边际排序价格": 13.456, "市场出清价格_预测均价": 12.890},
            "T5": {"调频容量需求": 3089.012, "边际排序价格": 8.901, "市场出清价格_预测均价": 9.012},
        },
        "model_version": "hidden_dim=128",
        "generated_at": "2026-03-04T18:30:00.123456",
    }


def test_render_contains_heading_and_table(tmp_path: Path) -> None:
    cfg = ReportSection(
        output_dir=str(tmp_path),
        template_path="app/templates/prediction.md.j2",
    )
    renderer = ReportRenderer(cfg)

    path = renderer.render(
        predict_date="2026-03-27",
        data_date="2026-03-26",
        trace_id="trace-abc",
        prediction=_sample_prediction(),
    )
    assert path.name == "prediction_2026-03-27.md"
    body = path.read_text(encoding="utf-8")
    assert "# 2026-03-27 预测结果" in body
    assert "数据日期：2026-03-26" in body
    assert "hidden_dim=128" in body
    # table rows
    assert "| T1 |" in body
    assert "| T5 |" in body
    # raw json block
    assert "```json" in body and "segments" in body


def test_render_tolerates_alt_metric_key(tmp_path: Path) -> None:
    pred = _sample_prediction()
    pred["segments"]["T1"] = {
        "调频容量需求": 1.0,
        "边际排序价格": 2.0,
        "市场出清价格(预测均价)": 3.0,  # alt key from /results endpoint
    }
    cfg = ReportSection(
        output_dir=str(tmp_path),
        template_path="app/templates/prediction.md.j2",
    )
    renderer = ReportRenderer(cfg)
    path = renderer.render(
        predict_date="2026-03-27",
        data_date="2026-03-26",
        trace_id="t1",
        prediction=pred,
    )
    body = path.read_text(encoding="utf-8")
    assert "3.000" in body
