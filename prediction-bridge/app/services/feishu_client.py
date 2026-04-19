"""Feishu bot client.

Factored from ``demo/feishu_file_bot_demo.py`` into a reusable module covering:
- ``get_tenant_access_token``
- ``upload_file``
- ``send_text_message`` (with ``@all`` or per-user mentions)
- ``send_file_message``

High-level :meth:`send_report` handles mentions + file delivery per target config.
"""

from __future__ import annotations

import json
import mimetypes
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..core.config import FeishuSection, FeishuTarget
from ..core.errors import FeishuError
from ..core.logging import logger, mask_secret


BASE_URL = "https://open.feishu.cn/open-apis"


class FeishuClient:
    def __init__(self, cfg: FeishuSection) -> None:
        self._cfg = cfg
        self._token: Optional[str] = None
        self._token_exp: float = 0.0

    # ------------------------------------------------------------------ #
    # Auth
    # ------------------------------------------------------------------ #

    def get_tenant_access_token(self, *, force: bool = False) -> str:
        if not force and self._token and time.time() < self._token_exp - 60:
            return self._token

        logger.info(
            "fetching tenant_access_token",
            extra={"app_id": self._cfg.app_id, "app_secret": mask_secret(self._cfg.app_secret)},
        )
        resp = httpx.post(
            f"{BASE_URL}/auth/v3/tenant_access_token/internal",
            json={"app_id": self._cfg.app_id, "app_secret": self._cfg.app_secret},
            timeout=self._cfg.http_timeout_sec,
        )
        self._raise_for(resp, "/auth/v3/tenant_access_token/internal")
        payload = resp.json()
        self._token = payload["tenant_access_token"]
        self._token_exp = time.time() + int(payload.get("expire", 7200))
        return self._token

    # ------------------------------------------------------------------ #
    # File + messages
    # ------------------------------------------------------------------ #

    def upload_file(self, file_path: Path) -> str:
        token = self.get_tenant_access_token()
        content_type, _ = mimetypes.guess_type(file_path.name)
        content_type = content_type or "application/octet-stream"
        logger.info(
            "uploading file",
            extra={
                "path": str(file_path),
                "file_name": file_path.name,
                "size": file_path.stat().st_size,
                "content_type": content_type,
            },
        )
        with file_path.open("rb") as fp:
            resp = httpx.post(
                f"{BASE_URL}/im/v1/files",
                headers={"Authorization": f"Bearer {token}"},
                data={"file_type": "stream", "file_name": file_path.name},
                files={"file": (file_path.name, fp, content_type)},
                timeout=self._cfg.http_timeout_sec,
            )
        self._raise_for(resp, "/im/v1/files")
        return resp.json()["data"]["file_key"]

    def send_text_message(self, chat_id: str, text: str) -> Dict[str, Any]:
        token = self.get_tenant_access_token()
        payload = {
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        }
        resp = httpx.post(
            f"{BASE_URL}/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=self._cfg.http_timeout_sec,
        )
        self._raise_for(resp, "/im/v1/messages(text)")
        return resp.json()

    def send_file_message(self, chat_id: str, file_key: str) -> Dict[str, Any]:
        token = self.get_tenant_access_token()
        payload = {
            "receive_id": chat_id,
            "msg_type": "file",
            "content": json.dumps({"file_key": file_key}, ensure_ascii=False),
        }
        resp = httpx.post(
            f"{BASE_URL}/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=self._cfg.http_timeout_sec,
        )
        self._raise_for(resp, "/im/v1/messages(file)")
        return resp.json()

    # ------------------------------------------------------------------ #
    # High level
    # ------------------------------------------------------------------ #

    def send_report(
        self,
        *,
        report_path: Path,
        predict_date: str,
        data_date: str,
        trace_id: str,
        targets: Optional[List[FeishuTarget]] = None,
    ) -> List[Dict[str, Any]]:
        """Send report to each configured target.

        Returns a list of per-target results suitable for task status inspection.
        A failure on one target does not abort the rest.
        """
        if not self._cfg.enabled:
            logger.info("feishu disabled; skipping notification")
            return []

        effective_targets = targets if targets is not None else self._cfg.targets
        results: List[Dict[str, Any]] = []

        try:
            file_key = self.upload_file(report_path)
        except Exception as exc:
            logger.opt(exception=True).error("feishu upload_file failed")
            for t in effective_targets:
                results.append(
                    {"chat_id": t.chat_id, "name": t.name, "ok": False, "error": str(exc)}
                )
            return results

        for target in effective_targets:
            entry: Dict[str, Any] = {
                "chat_id": target.chat_id,
                "name": target.name,
                "ok": True,
                "file_key": file_key,
            }
            try:
                text = build_mention_text(
                    message=target.message.format(
                        predict_date=predict_date,
                        data_date=data_date,
                        trace_id=trace_id,
                    ),
                    mention_all=target.mention_all,
                    mention_ids=target.mention_ids,
                    mention_names=target.mention_names,
                )
                text_resp = self.send_text_message(target.chat_id, text)
                file_resp = self.send_file_message(target.chat_id, file_key)
                entry["text_message_id"] = text_resp.get("data", {}).get("message_id")
                entry["file_message_id"] = file_resp.get("data", {}).get("message_id")
                logger.info(
                    "feishu delivery ok",
                    extra={
                        "chat_id": target.chat_id,
                        "text_message_id": entry["text_message_id"],
                        "file_message_id": entry["file_message_id"],
                    },
                )
            except Exception as exc:
                entry["ok"] = False
                entry["error"] = str(exc)
                logger.opt(exception=True).error(
                    "feishu delivery failed",
                    extra={"chat_id": target.chat_id, "name": target.name},
                )
            results.append(entry)
        return results

    def send_alert(self, message: str) -> None:
        """Send a plain-text alert to the first configured target (no file)."""
        if not self._cfg.enabled or not self._cfg.alert_on_failure:
            return
        if not self._cfg.targets:
            return
        target = self._cfg.targets[0]
        try:
            text = build_mention_text(
                message=message,
                mention_all=target.mention_all,
                mention_ids=target.mention_ids,
                mention_names=target.mention_names,
            )
            self.send_text_message(target.chat_id, text)
        except Exception:
            logger.opt(exception=True).error("feishu alert failed")

    def ping(self) -> bool:
        """Try to obtain a token to prove credentials work."""
        try:
            self.get_tenant_access_token(force=True)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ #

    @staticmethod
    def _raise_for(resp: httpx.Response, path: str) -> None:
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise FeishuError(
                f"{path} HTTP {resp.status_code}: {resp.text[:500]}"
            ) from exc
        try:
            payload = resp.json()
        except ValueError as exc:
            raise FeishuError(f"{path}: non-JSON response: {resp.text[:500]}") from exc
        if payload.get("code", -1) != 0:
            raise FeishuError(
                f"{path} feishu error code={payload.get('code')} msg={payload.get('msg')}"
            )


def build_mention_text(
    *,
    message: str,
    mention_all: bool = True,
    mention_ids: Iterable[str] = (),
    mention_names: Iterable[str] = (),
) -> str:
    """Prepend @ mentions to the message.

    When ``mention_all=True`` (default) we always use ``<at user_id="all">所有人</at>``
    and ignore ``mention_ids`` / ``mention_names``. Otherwise we build per-user tags.
    """
    if mention_all:
        return f'<at user_id="all">所有人</at>\n{message}'.strip()

    ids = list(mention_ids)
    names = list(mention_names)
    if names and len(names) != len(ids):
        raise ValueError("mention_names count must match mention_ids count")

    parts: List[str] = []
    for idx, user_id in enumerate(ids):
        display = names[idx] if idx < len(names) and names[idx] else user_id
        parts.append(f'<at user_id="{user_id}">{display}</at>')
    if parts:
        return f"{' '.join(parts)}\n{message}".strip()
    return message
