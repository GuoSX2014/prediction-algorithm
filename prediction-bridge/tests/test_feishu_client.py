"""FeishuClient tests."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from app.core.config import FeishuSection, FeishuTarget
from app.core.errors import FeishuError
from app.services.feishu_client import BASE_URL, FeishuClient, build_mention_text


def _cfg(**overrides) -> FeishuSection:
    base = dict(
        enabled=True,
        app_id="cli_test",
        app_secret="secret",
        http_timeout_sec=5,
        targets=[
            FeishuTarget(
                chat_id="oc_test",
                name="group-1",
                mention_all=True,
                message="请查阅 {predict_date} 预测结果。",
            )
        ],
    )
    base.update(overrides)
    return FeishuSection(**base)


# ----- build_mention_text ----- #

def test_build_mention_all_ignores_individual_ids() -> None:
    text = build_mention_text(
        message="hi",
        mention_all=True,
        mention_ids=["ou_a", "ou_b"],
        mention_names=["A", "B"],
    )
    assert text.startswith('<at user_id="all">所有人</at>')
    assert "ou_a" not in text


def test_build_mention_all_false_uses_ids() -> None:
    text = build_mention_text(
        message="hi",
        mention_all=False,
        mention_ids=["ou_a", "ou_b"],
        mention_names=["张三", "李四"],
    )
    assert '<at user_id="ou_a">张三</at>' in text
    assert '<at user_id="ou_b">李四</at>' in text


def test_build_mention_mismatched_lengths() -> None:
    with pytest.raises(ValueError):
        build_mention_text(
            message="hi",
            mention_all=False,
            mention_ids=["ou_a", "ou_b"],
            mention_names=["only-one"],
        )


# ----- HTTP-backed ----- #

@respx.mock
def test_get_token() -> None:
    respx.post(f"{BASE_URL}/auth/v3/tenant_access_token/internal").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "msg": "ok", "tenant_access_token": "t-123", "expire": 7200}
        )
    )
    client = FeishuClient(_cfg())
    assert client.get_tenant_access_token() == "t-123"


@respx.mock
def test_token_error_raises() -> None:
    respx.post(f"{BASE_URL}/auth/v3/tenant_access_token/internal").mock(
        return_value=httpx.Response(200, json={"code": 99991663, "msg": "bad"})
    )
    with pytest.raises(FeishuError):
        FeishuClient(_cfg()).get_tenant_access_token(force=True)


@respx.mock
def test_send_report_happy_path(tmp_path: Path) -> None:
    report = tmp_path / "prediction_2026-03-27.md"
    report.write_text("# hello")

    respx.post(f"{BASE_URL}/auth/v3/tenant_access_token/internal").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "tenant_access_token": "tok", "expire": 7200}
        )
    )
    respx.post(f"{BASE_URL}/im/v1/files").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "data": {"file_key": "file_v3_xxx"}}
        )
    )
    msg_route = respx.post(f"{BASE_URL}/im/v1/messages").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "data": {"message_id": "om_1"}}
        )
    )

    client = FeishuClient(_cfg())
    results = client.send_report(
        report_path=report,
        predict_date="2026-03-27",
        data_date="2026-03-26",
        trace_id="trace-1",
    )
    assert len(results) == 1
    assert results[0]["ok"] is True
    assert results[0]["file_key"] == "file_v3_xxx"
    # called twice: text message then file message
    assert msg_route.call_count == 2


@respx.mock
def test_send_report_per_target_failure_does_not_abort(tmp_path: Path) -> None:
    report = tmp_path / "r.md"
    report.write_text("x")

    respx.post(f"{BASE_URL}/auth/v3/tenant_access_token/internal").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "tenant_access_token": "t", "expire": 7200}
        )
    )
    respx.post(f"{BASE_URL}/im/v1/files").mock(
        return_value=httpx.Response(
            200, json={"code": 0, "data": {"file_key": "k"}}
        )
    )
    # First target POST fails (code != 0); second succeeds.
    responses = iter(
        [
            httpx.Response(200, json={"code": 230001, "msg": "no perm"}),
            httpx.Response(200, json={"code": 0, "data": {"message_id": "om_ok_text"}}),
            httpx.Response(200, json={"code": 0, "data": {"message_id": "om_ok_file"}}),
        ]
    )
    respx.post(f"{BASE_URL}/im/v1/messages").mock(side_effect=lambda r: next(responses))

    cfg = _cfg(
        targets=[
            FeishuTarget(chat_id="oc_bad", mention_all=True, message="x"),
            FeishuTarget(chat_id="oc_ok", mention_all=True, message="x"),
        ]
    )
    results = FeishuClient(cfg).send_report(
        report_path=report,
        predict_date="2026-03-27",
        data_date="2026-03-26",
        trace_id="t",
    )
    assert results[0]["ok"] is False
    assert results[1]["ok"] is True
