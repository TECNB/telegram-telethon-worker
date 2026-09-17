from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import logging
import os
import secrets
import re
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError, version
from math import ceil, floor
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web
from telethon import TelegramClient, errors, utils
from telethon.tl.functions.messages import GetPeerDialogsRequest
from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo, InputDialogPeer

from .contracts import (
    BackfillRequest,
    ForwardRequest,
    ReconcileTargetRequest,
    ResolveSourceRequest,
)
from .config import Config, configure_logging, load_dotenv
from .state import DeliveryLedger, StateStore

logger = logging.getLogger("telegram_worker")
HASHTAG_RE = re.compile(r"#[\w\u4e00-\u9fff]+")
BASELINE_PAGE_SIZE = 200
BASELINE_SOFT_MESSAGE_LIMIT = 2000
BASELINE_HARD_MESSAGE_LIMIT = 5000
BASELINE_MIN_SAMPLES = 50
BASELINE_CACHE_TTL = timedelta(hours=24)

try:
    WORKER_VERSION = version("telegram-telethon-worker")
except PackageNotFoundError:
    WORKER_VERSION = "development"


def video_duration(message: Any) -> Optional[float]:
    document = getattr(message, "document", None)
    if document is None:
        return None
    for attribute in document.attributes:
        if isinstance(attribute, DocumentAttributeVideo):
            return attribute.duration
    return None


def media_filename(message: Any) -> Optional[str]:
    document = getattr(message, "document", None)
    if document is None:
        return None
    for attribute in document.attributes:
        if isinstance(attribute, DocumentAttributeFilename):
            return attribute.file_name
    return None


def readable_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "无"
    total_seconds = round(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}小时{minutes}分{remaining_seconds}秒"
    if minutes:
        return f"{minutes}分{remaining_seconds}秒"
    return f"{remaining_seconds}秒"


def readable_media_type(message: Any, duration: Optional[float]) -> str:
    if duration is not None:
        return "视频"
    media = getattr(message, "media", None)
    if media is None:
        return "文字"
    media_name = type(media).__name__
    return {
        "MessageMediaPhoto": "图片",
        "MessageMediaDocument": "文件",
        "MessageMediaWebPage": "网页链接",
        "MessageMediaContact": "联系人",
        "MessageMediaGeo": "位置",
        "MessageMediaPoll": "投票",
    }.get(media_name, media_name)


def readable_message_text(message: Any, limit: int = 160) -> str:
    return readable_text(getattr(message, "raw_text", "") or "", limit)


def readable_text(text: str, limit: int = 160) -> str:
    text = " ".join(text.split())
    if not text:
        return "无"
    if len(text) > limit:
        return text[: limit - 1] + "…"
    return text


@dataclass
class MessageUnit:
    messages: list = field(default_factory=list)
    caption: str = ""

    @property
    def first_id(self) -> int:
        return min(message.id for message in self.messages)

    @property
    def last_id(self) -> int:
        return max(message.id for message in self.messages)


@dataclass
class ResourceBlock:
    body_units: list
    anchor: Optional[MessageUnit] = None

    @property
    def messages(self) -> list:
        return [message for unit in self.body_units for message in unit.messages]

    @property
    def last_id(self) -> int:
        units = self.body_units or ([self.anchor] if self.anchor else [])
        return max(unit.last_id for unit in units)

    @property
    def caption(self) -> str:
        if self.anchor:
            return self.anchor.caption
        return next((unit.caption for unit in self.body_units if unit.caption), "")


@dataclass(frozen=True)
class BaselineResult:
    threshold: float
    sample_count: int
    scanned_messages: int
    confidence: str
    cache_hit: bool = False


@dataclass
class PreparedSelection:
    kind: str
    signature: tuple
    source: Any
    target: Any
    source_id: int
    target_id: int
    resources: list[ResourceBlock]
    base_result: dict
    next_cursor: int


@dataclass(frozen=True)
class ResourceScore:
    message_id: int
    views: int
    forwards: int
    rate: float


def message_tags(text: str) -> list[str]:
    return list(dict.fromkeys(tag.lower() for tag in HASHTAG_RE.findall(text or "")))


def make_units(messages: list) -> list[MessageUnit]:
    grouped = {}
    ordered = []
    for message in sorted(messages, key=lambda item: item.id):
        key = ("group", message.grouped_id) if getattr(message, "grouped_id", None) else ("message", message.id)
        if key not in grouped:
            grouped[key] = MessageUnit()
            ordered.append(grouped[key])
        unit = grouped[key]
        unit.messages.append(message)
        text = (getattr(message, "raw_text", "") or "").strip()
        if text and not unit.caption:
            unit.caption = text
    return ordered


def make_resources(messages: list, mode: str) -> list[ResourceBlock]:
    units = make_units(messages)
    if mode == "group":
        return [ResourceBlock([unit]) for unit in units]

    resources = []
    anchor = None
    body = []
    for unit in units:
        if message_tags(unit.caption):
            if anchor is not None and body:
                resources.append(ResourceBlock(body, anchor))
            anchor = unit
            body = []
        elif anchor is not None:
            body.append(unit)
    if anchor is not None and body:
        resources.append(ResourceBlock(body, anchor))
    return resources


def resource_score(resource: ResourceBlock, request: ForwardRequest) -> Optional[ResourceScore]:
    scores = []
    messages = (
        resource.anchor.messages
        if request.resource_mode == "hashtag_resource" and resource.anchor
        else resource.messages
    )
    for message in messages:
        duration = video_duration(message)
        views = getattr(message, "views", None) or 0
        forwards = getattr(message, "forwards", None) or 0
        if request.resource_mode != "hashtag_resource" and (
            duration is None or duration <= request.min_video_duration
        ):
            continue
        if views < request.min_views or forwards < request.min_forwards:
            continue
        scores.append(ResourceScore(message.id, views, forwards, forwards / views))
    return max(scores, key=lambda score: score.rate) if scores else None


def resource_rate(resource: ResourceBlock, request: ForwardRequest) -> Optional[float]:
    score = resource_score(resource, request)
    return score.rate if score else None


def resource_has_long_video(resource: ResourceBlock, request: ForwardRequest) -> bool:
    return any(
        (video_duration(message) or 0) > request.min_video_duration
        for message in resource.messages
    )


def resource_is_mature(resource: ResourceBlock, request: ForwardRequest, now: datetime) -> bool:
    if not resource.messages:
        return False
    return all(
        getattr(message, "date", None) is not None
        and (now - message.date).total_seconds() >= request.min_age_hours * 3600
        for message in resource.messages
    )


def percentile(values: list[float], rank: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * rank
    lower, upper = floor(position), ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


async def resolve_message_caption(
    client: TelegramClient,
    source: Any,
    message: Any,
    album_text_cache: Dict[int, tuple],
) -> tuple:
    text = (getattr(message, "raw_text", "") or "").strip()
    grouped_id = getattr(message, "grouped_id", None)
    if text:
        if grouped_id:
            album_text_cache[grouped_id] = (text, message.id)
        return text, message.id
    if not grouped_id:
        return "", None

    cached = album_text_cache.get(grouped_id)
    if cached:
        return cached

    first_id = max(1, message.id - 9)
    nearby_messages = await client.get_messages(
        source,
        ids=list(range(first_id, message.id + 10)),
    )
    for nearby_message in nearby_messages:
        if nearby_message is None or nearby_message.grouped_id != grouped_id:
            continue
        nearby_text = (getattr(nearby_message, "raw_text", "") or "").strip()
        if nearby_text:
            album_text_cache[grouped_id] = (nearby_text, nearby_message.id)
            return nearby_text, nearby_message.id

    return "", None


async def readable_message_text_with_album(
    client: TelegramClient,
    source: Any,
    message: Any,
    album_text_cache: Dict[int, tuple],
) -> str:
    text, source_message_id = await resolve_message_caption(
        client,
        source,
        message,
        album_text_cache,
    )
    display_text = readable_text(text)
    if source_message_id is not None and source_message_id != message.id:
        return f"{display_text}（来自相册消息 {source_message_id}）"
    if not text and getattr(message, "grouped_id", None):
        return "无（已检查同一相册）"
    return display_text


class TelegramForwarder:
    def __init__(
        self,
        client: TelegramClient,
        state: StateStore,
        ledger: Optional[DeliveryLedger] = None,
    ):
        self.client = client
        self.state = state
        self.ledger = ledger or DeliveryLedger(state.database_path)
        self.baseline_cache = {}
        self.prepared_selections: Dict[str, PreparedSelection] = {}
        self.operation_progress: Dict[str, dict] = {}
        # ponytail: one account is serialized; add per-source locks only if throughput needs it.
        self.lock = asyncio.Lock()

    @staticmethod
    def _request_signature(request: Any) -> tuple:
        ignored = {"dry_run", "request_id", "reuse_request_id", "force_resend"}
        return tuple(
            (name, value)
            for name, value in vars(request).items()
            if name not in ignored
        )

    def _remember_prepared(self, request: Any, prepared: PreparedSelection) -> None:
        if not request.request_id:
            return
        self.prepared_selections[request.request_id] = prepared
        while len(self.prepared_selections) > 100:
            self.prepared_selections.pop(next(iter(self.prepared_selections)))

    def _reusable_selection(self, request: Any, kind: str) -> Optional[PreparedSelection]:
        prepared = self.prepared_selections.get(request.reuse_request_id or "")
        if prepared is None or prepared.kind != kind:
            return None
        return prepared if prepared.signature == self._request_signature(request) else None

    def _update_progress(self, request_id: Optional[str], **values: Any) -> None:
        if not request_id:
            return
        progress = self.operation_progress.setdefault(request_id, {"phase": "QUEUED"})
        progress.update(values)
        while len(self.operation_progress) > 200:
            self.operation_progress.pop(next(iter(self.operation_progress)))

    def progress(self, request_id: str) -> dict:
        return self.operation_progress.get(request_id, {"phase": "UNKNOWN"})

    def _cached_result(self, request_id: Optional[str], operation: str) -> Optional[dict]:
        if not request_id:
            return None
        result = self.ledger.command_result(request_id, operation)
        return {**result, "replayedResponse": True} if result else None

    def _remember_result(
        self, request_id: Optional[str], operation: str, result: dict
    ) -> dict:
        result = {**result, "replayedResponse": False}
        if request_id:
            self.ledger.record_command_result(request_id, operation, result)
        return result

    async def _with_anchor_context(
        self, source: Any, cursor: int, messages: list, mode: str
    ) -> list:
        if mode != "hashtag_resource" or not messages:
            return messages
        history = []
        max_id = cursor + 1
        while len(history) < BASELINE_HARD_MESSAGE_LIMIT:
            page = []
            async for message in self.client.iter_messages(
                source,
                max_id=max_id,
                limit=min(BASELINE_PAGE_SIZE, BASELINE_HARD_MESSAGE_LIMIT - len(history)),
            ):
                page.append(message)
            if not page:
                break
            history.extend(page)
            anchors = [unit for unit in make_units(history) if message_tags(unit.caption)]
            if anchors:
                anchor = max(anchors, key=lambda unit: unit.last_id)
                combined = {
                    message.id: message
                    for message in history + messages
                    if message.id >= anchor.first_id
                }
                return sorted(combined.values(), key=lambda message: message.id)
            if len(page) < BASELINE_PAGE_SIZE:
                break
            max_id = min(message.id for message in page)
        raise RuntimeError("无法在 Worker 游标之前找到 hashtag 资源锚点")

    @staticmethod
    def _group_key(unit: MessageUnit) -> str:
        return ",".join(str(message.id) for message in unit.messages)

    @staticmethod
    def _resource_id(resource: ResourceBlock) -> int:
        return resource.anchor.first_id if resource.anchor else resource.body_units[0].first_id

    def _delivery_state(self, target_id: int, source_id: int, resource: ResourceBlock) -> dict:
        delivered = 0
        target_message_ids = []
        for unit in resource.body_units:
            delivery = self.ledger.get(target_id, source_id, self._group_key(unit))
            if delivery:
                delivered += 1
                target_message_ids.extend(delivery.target_message_ids)
        total = len(resource.body_units)
        return {
            "duplicateGroupCount": delivered,
            "pendingGroupCount": total - delivered,
            "targetMessageIds": target_message_ids,
            "fullyDuplicate": total > 0 and delivered == total,
        }

    def _resource_result(
        self,
        resource: ResourceBlock,
        request: ForwardRequest,
        *,
        rank: int,
        status: str,
        threshold: Optional[float] = None,
        send_result: Optional[dict] = None,
    ) -> dict:
        score = resource_score(resource, request)
        durations = [
            duration
            for message in resource.messages
            for duration in [video_duration(message)]
            if duration is not None
        ]
        dates = [
            message.date
            for message in resource.messages
            if getattr(message, "date", None) is not None
        ]
        result = {
            "resourceId": str(self._resource_id(resource)),
            "rank": rank,
            "status": status,
            "caption": " ".join(resource.caption.split()),
            "hashtags": message_tags(resource.caption),
            "publishedAt": min(dates).isoformat() if dates else None,
            "firstMessageId": resource.body_units[0].first_id,
            "lastMessageId": resource.last_id,
            "sourceMessageIds": [message.id for message in resource.messages],
            "bodyGroupCount": len(resource.body_units),
            "sourceMessageCount": len(resource.messages),
            "mediaCount": sum(1 for message in resource.messages if getattr(message, "media", None)),
            "videoCount": len(durations),
            "longVideoCount": sum(duration > request.min_video_duration for duration in durations),
            "maxVideoDurationSeconds": max(durations) if durations else None,
            "scoreMessageId": score.message_id if score else None,
            "views": score.views if score else None,
            "forwards": score.forwards if score else None,
            "forwardRate": score.rate if score else None,
            "forwardRatePercent": score.rate * 100 if score else None,
            "threshold": threshold,
            "thresholdPercent": threshold * 100 if threshold is not None else None,
        }
        if send_result:
            result.update(
                {
                    "targetMessageIds": send_result["targetMessageIds"],
                    "sentMessageCount": send_result["sentMessageCount"],
                    "forwardedGroupCount": send_result["forwardedGroupCount"],
                    "duplicateGroupCount": send_result["duplicateGroupCount"],
                    "groups": send_result["groups"],
                }
            )
        return result

    async def _send_resource_groups(
        self,
        target: Any,
        source: Any,
        source_id: int,
        resource: ResourceBlock,
        request: ForwardRequest,
        delivery_mode: str = "FOLLOW",
        force_resend: bool = False,
    ) -> dict:
        resource_id = self._resource_id(resource)
        legacy_completed = self.state.completed_groups(source_id, resource_id)
        target_id = utils.get_peer_id(target)
        target_message_ids = []
        group_results = []
        duplicate_groups = 0
        forwarded_groups = 0
        for index, unit in enumerate(resource.body_units):
            group_key = self._group_key(unit)
            source_message_ids = [message.id for message in unit.messages]
            delivery = self.ledger.get(target_id, source_id, group_key)
            if not force_resend and (delivery or group_key in legacy_completed):
                if delivery is None:
                    self.ledger.record(
                        target_id=target_id,
                        source_id=source_id,
                        group_key=group_key,
                        resource_key=str(resource_id),
                        source_message_ids=source_message_ids,
                        target_message_ids=[],
                        delivery_mode="LEGACY_PROGRESS",
                        request_id=request.request_id,
                    )
                    delivery = self.ledger.get(target_id, source_id, group_key)
                duplicate_groups += 1
                existing_target_ids = delivery.target_message_ids if delivery else []
                target_message_ids.extend(existing_target_ids)
                group_results.append(
                    {
                        "groupKey": group_key,
                        "status": "DUPLICATE",
                        "sourceMessageIds": source_message_ids,
                        "targetMessageIds": existing_target_ids,
                        "deliveredAt": delivery.delivered_at if delivery else None,
                    }
                )
                continue
            if request.resource_mode == "hashtag_resource":
                media = [message.media for message in unit.messages if message.media]
                if not media:
                    continue
                sent_messages = await self.client.send_file(
                    target,
                    media,
                    caption=[resource.caption] + [""] * (len(media) - 1),
                    parse_mode=None,
                )
            else:
                sent_messages = await self.client.forward_messages(
                    target,
                    [message.id for message in unit.messages],
                    from_peer=source,
                )
            if sent_messages is None:
                sent_messages = []
            elif not isinstance(sent_messages, (list, tuple)):
                sent_messages = [sent_messages]
            sent_ids = [sent.id for sent in sent_messages if sent is not None]
            self.ledger.record(
                target_id=target_id,
                source_id=source_id,
                group_key=group_key,
                resource_key=str(resource_id),
                source_message_ids=source_message_ids,
                target_message_ids=sent_ids,
                delivery_mode=delivery_mode,
                request_id=request.request_id,
            )
            # Keep writing the legacy file until all existing deployments have upgraded once.
            self.state.save_completed_group(source_id, resource_id, group_key)
            forwarded_groups += 1
            target_message_ids.extend(sent_ids)
            group_results.append(
                {
                    "groupKey": group_key,
                    "status": "FORWARDED",
                    "sourceMessageIds": source_message_ids,
                    "targetMessageIds": sent_ids,
                    "deliveredAt": datetime.now(timezone.utc).isoformat(),
                }
            )
            if index + 1 < len(resource.body_units):
                await asyncio.sleep(request.group_interval_seconds)
        return {
            "targetMessageIds": target_message_ids,
            "forwardedGroupCount": forwarded_groups,
            "duplicateGroupCount": duplicate_groups,
            "sentMessageCount": sum(
                len(group["targetMessageIds"])
                for group in group_results
                if group["status"] == "FORWARDED"
            ),
            "groups": group_results,
        }

    async def _send_prepared_selection(
        self,
        request: Any,
        prepared: PreparedSelection,
    ) -> dict:
        is_backfill = isinstance(request, BackfillRequest)
        rate_request = request if not is_backfill else ForwardRequest(
            source=request.source,
            target=request.target,
            limit=None,
            min_video_duration=request.min_video_duration,
            min_views=request.min_views,
            min_forwards=request.min_forwards,
            resource_mode=request.resource_mode,
            group_interval_seconds=request.group_interval_seconds,
            resource_interval_seconds=request.resource_interval_seconds,
            request_id=request.request_id,
        )
        force_resend = is_backfill and request.force_resend
        delivery_mode = "BACKFILL_FORCE" if force_resend else prepared.kind
        threshold = prepared.base_result.get("threshold")
        results = []
        forwarded_resource_count = 0
        duplicate_resource_count = 0
        forwarded_group_count = 0
        duplicate_group_count = 0
        forwarded_message_count = 0
        forwarded_message_ids = []
        self._update_progress(
            request.request_id,
            phase="REUSING_DRY_RUN",
            selectedResourceCount=len(prepared.resources),
            processedResourceCount=0,
        )
        for index, resource in enumerate(prepared.resources, 1):
            send_result = await self._send_resource_groups(
                prepared.target,
                prepared.source,
                prepared.source_id,
                resource,
                rate_request,
                delivery_mode,
                force_resend,
            )
            forwarded_group_count += send_result["forwardedGroupCount"]
            duplicate_group_count += send_result["duplicateGroupCount"]
            forwarded_message_count += send_result["sentMessageCount"]
            forwarded_message_ids.extend(
                message_id
                for group in send_result["groups"]
                if group["status"] == "FORWARDED"
                for message_id in group["sourceMessageIds"]
            )
            if send_result["forwardedGroupCount"]:
                forwarded_resource_count += 1
                status = "FORWARDED"
            else:
                duplicate_resource_count += 1
                status = "DUPLICATE"
            results.append(self._resource_result(
                resource,
                rate_request,
                rank=index,
                status=status,
                threshold=threshold,
                send_result=send_result,
            ))
            self._update_progress(
                request.request_id,
                phase="FORWARDING_RESOURCES",
                processedResourceCount=index,
                forwardedResourceCount=forwarded_resource_count,
                forwardedMessageCount=forwarded_message_count,
            )
            if send_result["forwardedGroupCount"]:
                await asyncio.sleep(request.resource_interval_seconds)

        if is_backfill:
            if request.start_mode == "continue":
                self.state.save_backfill_before(prepared.source_id, prepared.next_cursor)
        else:
            self.state.save_cursor(prepared.source_id, prepared.next_cursor)
            if request.mark_read and prepared.resources:
                await self.client.send_read_acknowledge(
                    prepared.source, max_id=prepared.next_cursor
                )
        result = {
            **prepared.base_result,
            "requestId": request.request_id,
            "dryRun": False,
            "reusedDryRun": True,
            "reusedDryRunRequestId": request.reuse_request_id,
            "selectedResources": results,
            "forwardResults": results,
            "duplicateResourceCount": duplicate_resource_count,
            "forwardedResourceCount": forwarded_resource_count,
            "forwardedGroupCount": forwarded_group_count,
            "duplicateGroupCount": duplicate_group_count,
            "forwardedMessageCount": forwarded_message_count,
        }
        if is_backfill:
            result["nextBackfillBeforeMessageId"] = prepared.next_cursor
        else:
            result["matchedMessageIds"] = [
                message.id for resource in prepared.resources for message in resource.messages
            ]
            result["forwardedMessageIds"] = forwarded_message_ids
            result["nextCursor"] = prepared.next_cursor
        self._update_progress(request.request_id, phase="COMPLETED")
        operation = "BACKFILL" if is_backfill else "FORWARD"
        return self._remember_result(request.request_id, operation, result)

    async def backfill(self, request: BackfillRequest) -> Dict[str, Any]:
        self._update_progress(request.request_id, phase="QUEUED")
        async with self.lock:
            operation = "BACKFILL_DRY_RUN" if request.dry_run else "BACKFILL"
            cached = self._cached_result(request.request_id, operation)
            if cached:
                return cached
            prepared = self._reusable_selection(request, "BACKFILL")
            if prepared is not None and not request.dry_run:
                return await self._send_prepared_selection(request, prepared)
            self._update_progress(request.request_id, phase="RESOLVING_CHANNEL")
            source = await self.client.get_entity(request.source)
            if getattr(source, "noforwards", False) and not request.dry_run:
                raise RuntimeError("来源频道禁止转发，Worker 不会绕过此限制")
            target = await self.client.get_entity(request.target)
            source_id = utils.get_peer_id(source)
            target_id = utils.get_peer_id(target)
            reconciliation = (
                None
                if request.dry_run
                else await self._reconcile_target_entity(target, 10000)
            )
            before_id = (
                None
                if request.start_mode == "latest"
                else self.state.backfill_before(source_id)
            )
            initialized = before_id is None
            if before_id is None:
                newest = []
                async for message in self.client.iter_messages(source, limit=1):
                    newest.append(message)
                if not newest:
                    raise RuntimeError("来源频道没有可回溯的消息")
                before_id = newest[0].id + 1

            messages = []
            window_end = None
            cutoff = None
            async for message in self.client.iter_messages(
                source,
                max_id=before_id,
                limit=request.max_messages,
            ):
                if window_end is None:
                    window_end = message.date
                    cutoff = window_end - timedelta(days=request.lookback_days)
                if message.date < cutoff:
                    break
                messages.append(message)
                if len(messages) % 50 == 0:
                    self._update_progress(
                        request.request_id,
                        phase="SCANNING_MESSAGES",
                        scannedMessages=len(messages),
                    )
            if not messages:
                return self._remember_result(request.request_id, operation, {
                    "sourceId": source_id,
                    "sourceTitle": getattr(source, "title", None),
                    "sourceUsername": getattr(source, "username", None),
                    "targetId": target_id,
                    "targetTitle": getattr(target, "title", None)
                    or getattr(target, "first_name", None),
                    "targetUsername": getattr(target, "username", None),
                    "requestId": request.request_id,
                    "dryRun": request.dry_run,
                    "startMode": request.start_mode,
                    "initializedFromLatest": initialized,
                    "beforeMessageId": before_id,
                    "scanned": 0,
                    "resourceCount": 0,
                    "eligibleResourceCount": 0,
                    "selectedResourceCount": 0,
                    "duplicateResourceCount": 0,
                    "forwardedResourceCount": 0,
                    "forwardedGroupCount": 0,
                    "duplicateGroupCount": 0,
                    "forwardedMessageCount": 0,
                    "selectedResources": [],
                    "forwardResults": [],
                    "nextBackfillBeforeMessageId": before_id,
                    "telegramDataAsOf": datetime.now(timezone.utc).isoformat(),
                    "targetReconciliation": reconciliation,
                    "appliedRules": {
                        "selection": "TOP_N",
                        "topResources": request.top_resources,
                        "lookbackDays": request.lookback_days,
                        "maxMessages": request.max_messages,
                        "minVideoDuration": request.min_video_duration,
                        "minViews": request.min_views,
                        "minForwards": request.min_forwards,
                        "resourceMode": request.resource_mode,
                        "scoreMethod": (
                            "HASHTAG_ANCHOR_FORWARD_RATE"
                            if request.resource_mode == "hashtag_resource"
                            else "MAX_QUALIFYING_VIDEO_RATE"
                        ),
                    },
                })

            resources = make_resources(messages, request.resource_mode)
            self._update_progress(
                request.request_id,
                phase="RANKING_RESOURCES",
                scannedMessages=len(messages),
                discoveredResourceCount=len(resources),
            )
            rate_request = ForwardRequest(
                source=request.source,
                target=request.target,
                limit=None,
                min_video_duration=request.min_video_duration,
                min_views=request.min_views,
                min_forwards=request.min_forwards,
                resource_mode=request.resource_mode,
                group_interval_seconds=request.group_interval_seconds,
                resource_interval_seconds=request.resource_interval_seconds,
                dry_run=request.dry_run,
                request_id=request.request_id,
            )
            ranked = sorted(
                (
                    (resource, rate)
                    for resource in resources
                    if resource_has_long_video(resource, rate_request)
                    for rate in [resource_rate(resource, rate_request)]
                    if rate is not None
                ),
                key=lambda item: item[1],
                reverse=True,
            )
            selected = ranked[:request.top_resources]
            self._update_progress(
                request.request_id,
                eligibleResourceCount=len(ranked),
                selectedResourceCount=len(selected),
            )
            next_before_id = min(message.id for message in messages)
            preview = []
            for index, (resource, _) in enumerate(selected, 1):
                delivery = self._delivery_state(target_id, source_id, resource)
                status = "DUPLICATE" if delivery["fullyDuplicate"] else "DRY_RUN_SELECTED"
                detail = self._resource_result(
                    resource,
                    rate_request,
                    rank=index,
                    status=status,
                )
                detail.update(
                    {
                        "duplicateGroupCount": delivery["duplicateGroupCount"],
                        "pendingGroupCount": delivery["pendingGroupCount"],
                        "targetMessageIds": delivery["targetMessageIds"],
                    }
                )
                preview.append(detail)
            duplicate_resource_count = sum(
                self._delivery_state(target_id, source_id, resource)["fullyDuplicate"]
                for resource, _ in selected
            )
            base_result = {
                "sourceId": source_id,
                "sourceTitle": getattr(source, "title", None),
                "sourceUsername": getattr(source, "username", None),
                "targetId": target_id,
                "targetTitle": getattr(target, "title", None) or getattr(target, "first_name", None),
                "targetUsername": getattr(target, "username", None),
                "requestId": request.request_id,
                "dryRun": request.dry_run,
                "startMode": request.start_mode,
                "initializedFromLatest": initialized,
                "beforeMessageId": before_id,
                "windowStart": min(message.date for message in messages).isoformat(),
                "windowEnd": max(message.date for message in messages).isoformat(),
                "scanned": len(messages),
                "resourceCount": len(resources),
                "eligibleResourceCount": len(ranked),
                "selectedResourceCount": len(selected),
                "duplicateResourceCount": duplicate_resource_count,
                "selectedResources": preview,
                "nextBackfillBeforeMessageId": next_before_id,
                "telegramDataAsOf": datetime.now(timezone.utc).isoformat(),
                "targetReconciliation": reconciliation,
                "appliedRules": {
                    "selection": "TOP_N",
                    "topResources": request.top_resources,
                    "lookbackDays": request.lookback_days,
                    "maxMessages": request.max_messages,
                    "minVideoDuration": request.min_video_duration,
                    "minViews": request.min_views,
                    "minForwards": request.min_forwards,
                    "resourceMode": request.resource_mode,
                    "scoreMethod": (
                        "HASHTAG_ANCHOR_FORWARD_RATE"
                        if request.resource_mode == "hashtag_resource"
                        else "MAX_QUALIFYING_VIDEO_RATE"
                    ),
                },
            }
            if request.dry_run:
                self._remember_prepared(request, PreparedSelection(
                    kind="BACKFILL",
                    signature=self._request_signature(request),
                    source=source,
                    target=target,
                    source_id=source_id,
                    target_id=target_id,
                    resources=[resource for resource, _ in selected],
                    base_result=base_result,
                    next_cursor=next_before_id,
                ))
                self._update_progress(request.request_id, phase="COMPLETED")
                return self._remember_result(request.request_id, operation, base_result)

            forward_results = []
            forwarded_resource_count = 0
            duplicate_resource_count = 0
            forwarded_group_count = 0
            duplicate_group_count = 0
            forwarded_message_count = 0
            for index, (resource, _) in enumerate(selected, 1):
                send_result = await self._send_resource_groups(
                    target,
                    source,
                    source_id,
                    resource,
                    rate_request,
                    "BACKFILL_FORCE" if request.force_resend else "BACKFILL",
                    request.force_resend,
                )
                forwarded_group_count += send_result["forwardedGroupCount"]
                duplicate_group_count += send_result["duplicateGroupCount"]
                forwarded_message_count += send_result["sentMessageCount"]
                if send_result["forwardedGroupCount"]:
                    forwarded_resource_count += 1
                    status = "FORWARDED"
                else:
                    duplicate_resource_count += 1
                    status = "DUPLICATE"
                forward_results.append(
                    self._resource_result(
                        resource,
                        rate_request,
                        rank=index,
                        status=status,
                        send_result=send_result,
                    )
                )
                self._update_progress(
                    request.request_id,
                    phase="FORWARDING_RESOURCES",
                    processedResourceCount=index,
                    forwardedResourceCount=forwarded_resource_count,
                    forwardedMessageCount=forwarded_message_count,
                )
                if send_result["forwardedGroupCount"]:
                    await asyncio.sleep(request.resource_interval_seconds)
            if request.start_mode == "continue":
                self.state.save_backfill_before(source_id, next_before_id)
            for resource, _ in selected:
                self.state.clear_progress(source_id, self._resource_id(resource))
            self._update_progress(request.request_id, phase="COMPLETED")
            return self._remember_result(request.request_id, "BACKFILL", {
                **base_result,
                "selectedResources": forward_results,
                "forwardResults": forward_results,
                "duplicateResourceCount": duplicate_resource_count,
                "forwardedResourceCount": forwarded_resource_count,
                "forwardedGroupCount": forwarded_group_count,
                "duplicateGroupCount": duplicate_group_count,
                "forwardedMessageCount": forwarded_message_count,
            })

    async def _estimate_baseline(
        self,
        source: Any,
        source_id: int,
        cursor: int,
        request: ForwardRequest,
        now: datetime,
    ) -> BaselineResult:
        cache_key = (
            source_id,
            request.min_video_duration,
            request.percentile,
            request.baseline_size,
            request.min_views,
            request.min_forwards,
            request.min_age_hours,
            request.resource_mode,
        )
        cached = self.baseline_cache.get(cache_key)
        if cached and now - cached[0] < BASELINE_CACHE_TTL:
            result = cached[1]
            return BaselineResult(
                result.threshold,
                result.sample_count,
                result.scanned_messages,
                result.confidence,
                True,
            )

        messages = []
        max_id = cursor
        rates = []
        minimum_samples = min(BASELINE_MIN_SAMPLES, request.baseline_size)
        while len(messages) < BASELINE_HARD_MESSAGE_LIMIT:
            page_limit = min(
                BASELINE_PAGE_SIZE,
                BASELINE_HARD_MESSAGE_LIMIT - len(messages),
            )
            page = []
            async for message in self.client.iter_messages(
                source,
                max_id=max_id,
                limit=page_limit,
            ):
                page.append(message)
            if not page:
                break
            messages.extend(page)
            # ponytail: at most 25 small recomputations; incremental album parsing is not worth its seam yet.
            resources = make_resources(messages, request.resource_mode)
            rates = [
                rate
                for resource in resources
                if resource_has_long_video(resource, request)
                and resource_is_mature(resource, request, now)
                for rate in [resource_rate(resource, request)]
                if rate is not None
            ]
            self._update_progress(
                request.request_id,
                phase="BUILDING_BASELINE",
                baselineScannedMessages=len(messages),
                eligibleResourceCount=len(rates),
            )
            if len(rates) >= request.baseline_size:
                break
            if len(messages) >= BASELINE_SOFT_MESSAGE_LIMIT and len(rates) >= minimum_samples:
                break
            if len(page) < page_limit:
                break
            max_id = min(message.id for message in page)

        if len(rates) < minimum_samples:
            raise RuntimeError(
                "历史有效资源不足："
                f"扫描 {len(messages)} 条消息，仅找到 {len(rates)} 个；"
                f"至少需要 {minimum_samples} 个"
            )
        selected_rates = rates[-request.baseline_size:]
        threshold = percentile(selected_rates, request.percentile)
        if threshold is None:
            raise RuntimeError("历史样本不足，无法计算频道转发率基准")
        result = BaselineResult(
            threshold=threshold,
            sample_count=len(selected_rates),
            scanned_messages=len(messages),
            confidence="ideal" if len(selected_rates) >= request.baseline_size else "acceptable",
        )
        self.baseline_cache[cache_key] = (now, result)
        return result

    async def forward_unread(self, request: ForwardRequest) -> Dict[str, Any]:
        self._update_progress(request.request_id, phase="QUEUED")
        async with self.lock:
            operation = "FORWARD_DRY_RUN" if request.dry_run else "FORWARD"
            cached = self._cached_result(request.request_id, operation)
            if cached:
                return cached
            prepared = self._reusable_selection(request, "FOLLOW")
            if prepared is not None and not request.dry_run:
                return await self._send_prepared_selection(request, prepared)
            self._update_progress(request.request_id, phase="RESOLVING_CHANNEL")
            source = await self.client.get_entity(request.source)
            if getattr(source, "noforwards", False) and not request.dry_run:
                raise RuntimeError("来源频道禁止转发，Worker 不会绕过此限制")
            target = await self.client.get_entity(request.target)
            source_id = utils.get_peer_id(source)
            target_id = utils.get_peer_id(target)
            reconciliation = (
                None
                if request.dry_run
                else await self._reconcile_target_entity(target, 10000)
            )
            cursor = self.state.cursor(source_id)
            initialized_from_unread = cursor is None
            if cursor is None:
                cursor = await self._read_inbox_max_id(source)

            now = datetime.now(timezone.utc)
            baseline = await self._estimate_baseline(
                source, source_id, cursor, request, now
            )
            threshold = baseline.threshold

            messages = []
            async for message in self.client.iter_messages(
                source, min_id=cursor, limit=request.limit, reverse=True
            ):
                messages.append(message)
                if len(messages) % 20 == 0:
                    self._update_progress(
                        request.request_id,
                        phase="SCANNING_MESSAGES",
                        scannedMessages=len(messages),
                    )
            messages = await self._with_anchor_context(
                source, cursor, messages, request.resource_mode
            )
            scanned_resources = make_resources(messages, request.resource_mode)
            scanned_resources = [
                resource for resource in scanned_resources if resource.last_id > cursor
            ]
            resources = []
            for resource in scanned_resources:
                if not resource_is_mature(resource, request, now):
                    break
                resources.append(resource)
            deferred_resources = scanned_resources[len(resources):]
            scored = []
            for resource in resources:
                rate = resource_rate(resource, request)
                if (
                    resource_has_long_video(resource, request)
                    and resource_is_mature(resource, request, now)
                    and rate is not None
                ):
                    scored.append((resource, rate))
            candidates = sorted(
                [(resource, rate) for resource, rate in scored if rate >= threshold],
                key=lambda item: item[1],
                reverse=True,
            )
            if request.max_resources is not None:
                candidates = candidates[:request.max_resources]
            self._update_progress(
                request.request_id,
                phase="RANKING_RESOURCES",
                scannedMessages=sum(len(resource.messages) for resource in scanned_resources),
                discoveredResourceCount=len(scanned_resources),
                eligibleResourceCount=len(scored),
                selectedResourceCount=len(candidates),
            )
            selected = {id(resource) for resource, _ in candidates}
            ranks = {id(resource): rank for rank, (resource, _) in enumerate(candidates, 1)}

            selected_details = []
            for rank, (resource, _) in enumerate(candidates, 1):
                delivery = self._delivery_state(target_id, source_id, resource)
                status = "DUPLICATE" if delivery["fullyDuplicate"] else "DRY_RUN_SELECTED"
                detail = self._resource_result(
                    resource,
                    request,
                    rank=rank,
                    status=status,
                    threshold=threshold,
                )
                detail.update(
                    {
                        "duplicateGroupCount": delivery["duplicateGroupCount"],
                        "pendingGroupCount": delivery["pendingGroupCount"],
                        "targetMessageIds": delivery["targetMessageIds"],
                    }
                )
                selected_details.append(detail)
            duplicate_resource_count = sum(
                detail["status"] == "DUPLICATE" for detail in selected_details
            )

            if request.dry_run:
                dry_result = {
                    "sourceId": source_id,
                    "sourceTitle": getattr(source, "title", None),
                    "sourceUsername": getattr(source, "username", None),
                    "targetId": target_id,
                    "targetTitle": getattr(target, "title", None) or getattr(target, "first_name", None),
                    "targetUsername": getattr(target, "username", None),
                    "requestId": request.request_id,
                    "dryRun": True,
                    "initializedFromUnread": initialized_from_unread,
                    "cursor": cursor,
                    "scanned": sum(len(resource.messages) for resource in scanned_resources),
                    "scannedResourceCount": len(scanned_resources),
                    "processedResourceCount": len(resources),
                    "deferredResourceCount": len(deferred_resources),
                    "eligibleResourceCount": len(scored),
                    "threshold": threshold,
                    "baselineSampleSize": baseline.sample_count,
                    "baselineScannedMessages": baseline.scanned_messages,
                    "baselineConfidence": baseline.confidence,
                    "baselineCacheHit": baseline.cache_hit,
                    "selectedResourceCount": len(candidates),
                    "duplicateResourceCount": duplicate_resource_count,
                    "selectedResources": selected_details,
                    "nextCursor": cursor,
                    "markedReadThrough": None,
                    "telegramDataAsOf": now.isoformat(),
                    "targetReconciliation": reconciliation,
                    "appliedRules": self._applied_rules(request),
                }
                self._remember_prepared(request, PreparedSelection(
                    kind="FOLLOW",
                    signature=self._request_signature(request),
                    source=source,
                    target=target,
                    source_id=source_id,
                    target_id=target_id,
                    resources=[resource for resource, _ in candidates],
                    base_result=dry_result,
                    next_cursor=resources[-1].last_id if resources else cursor,
                ))
                self._update_progress(request.request_id, phase="COMPLETED")
                return self._remember_result(request.request_id, operation, dry_result)

            matched = []
            forwarded = []
            forward_results = []
            forwarded_resource_count = 0
            duplicate_resource_count = 0
            forwarded_group_count = 0
            duplicate_group_count = 0
            forwarded_message_count = 0
            for resource_index, resource in enumerate(resources, 1):
                resource_id = self._resource_id(resource)
                completed_groups = self.state.completed_groups(source_id, resource_id)
                delivery_state = self._delivery_state(target_id, source_id, resource)
                partially_delivered = (
                    delivery_state["duplicateGroupCount"] > 0
                    and not delivery_state["fullyDuplicate"]
                )
                if id(resource) not in selected and not completed_groups and not partially_delivered:
                    self.state.save_cursor(source_id, resource.last_id)
                    continue
                rate = resource_rate(resource, request)
                logger.info(
                    "正在转发资源 %d-%d：转发率 %.4f%%，Group 数量 %d。",
                    resource.body_units[0].first_id,
                    resource.last_id,
                    (rate or 0) * 100,
                    len(resource.body_units),
                )
                send_result = await self._send_resource_groups(
                    target, source, source_id, resource, request, "FOLLOW"
                )
                self.state.save_cursor(source_id, resource.last_id)
                self.state.clear_progress(source_id, resource_id)
                source_ids = [message.id for message in resource.messages]
                matched.extend(source_ids)
                forwarded.extend(
                    message_id
                    for group in send_result["groups"]
                    if group["status"] == "FORWARDED"
                    for message_id in group["sourceMessageIds"]
                )
                forwarded_group_count += send_result["forwardedGroupCount"]
                duplicate_group_count += send_result["duplicateGroupCount"]
                forwarded_message_count += send_result["sentMessageCount"]
                if send_result["forwardedGroupCount"]:
                    forwarded_resource_count += 1
                    status = "FORWARDED"
                else:
                    duplicate_resource_count += 1
                    status = "DUPLICATE"
                forward_results.append(
                    self._resource_result(
                        resource,
                        request,
                        rank=ranks.get(id(resource), 0),
                        status=status,
                        threshold=threshold,
                        send_result=send_result,
                    )
                )
                self._update_progress(
                    request.request_id,
                    phase="FORWARDING_RESOURCES",
                    processedResourceCount=resource_index,
                    forwardedResourceCount=forwarded_resource_count,
                    forwardedMessageCount=forwarded_message_count,
                )
                if send_result["forwardedGroupCount"]:
                    await asyncio.sleep(request.resource_interval_seconds)

            marked_read_through = None
            if request.mark_read and resources:
                await self.client.send_read_acknowledge(source, max_id=resources[-1].last_id)
                marked_read_through = resources[-1].last_id
            next_cursor = resources[-1].last_id if resources else cursor
            self._update_progress(request.request_id, phase="COMPLETED")
            return self._remember_result(request.request_id, operation, {
                "sourceId": source_id,
                "sourceTitle": getattr(source, "title", None),
                "sourceUsername": getattr(source, "username", None),
                "targetId": target_id,
                "targetTitle": getattr(target, "title", None) or getattr(target, "first_name", None),
                "targetUsername": getattr(target, "username", None),
                "requestId": request.request_id,
                "dryRun": False,
                "initializedFromUnread": initialized_from_unread,
                "scanned": sum(len(resource.messages) for resource in scanned_resources),
                "scannedResourceCount": len(scanned_resources),
                "processedResourceCount": len(resources),
                "deferredResourceCount": len(deferred_resources),
                "matchedMessageIds": matched,
                "forwardedMessageIds": forwarded,
                "selectedResources": forward_results,
                "forwardResults": forward_results,
                "threshold": threshold,
                "baselineSampleSize": baseline.sample_count,
                "baselineScannedMessages": baseline.scanned_messages,
                "baselineConfidence": baseline.confidence,
                "baselineCacheHit": baseline.cache_hit,
                "selectedResourceCount": len(candidates),
                "duplicateResourceCount": duplicate_resource_count,
                "forwardedResourceCount": forwarded_resource_count,
                "forwardedGroupCount": forwarded_group_count,
                "duplicateGroupCount": duplicate_group_count,
                "forwardedMessageCount": forwarded_message_count,
                "nextCursor": next_cursor,
                "markedReadThrough": marked_read_through,
                "telegramDataAsOf": now.isoformat(),
                "targetReconciliation": reconciliation,
                "appliedRules": self._applied_rules(request),
            })

    @staticmethod
    def _applied_rules(request: ForwardRequest) -> dict:
        return {
            "percentile": request.percentile,
            "baselineSize": request.baseline_size,
            "minVideoDuration": request.min_video_duration,
            "minViews": request.min_views,
            "minForwards": request.min_forwards,
            "minAgeHours": request.min_age_hours,
            "resourceMode": request.resource_mode,
            "scoreMethod": (
                "HASHTAG_ANCHOR_FORWARD_RATE"
                if request.resource_mode == "hashtag_resource"
                else "MAX_QUALIFYING_VIDEO_RATE"
            ),
        }

    async def resolve_source(self, request: ResolveSourceRequest) -> dict:
        async with self.lock:
            try:
                source = await self.client.get_entity(request.source)
            except ValueError:
                # A user may have joined a private channel in another Telegram client
                # after this worker session was last active. Refresh dialogs once so
                # Telethon can resolve the channel ID from its local entity cache.
                await self.client.get_dialogs()
                source = await self.client.get_entity(request.source)
            return {
                "sourceId": utils.get_peer_id(source),
                "title": getattr(source, "title", None)
                or getattr(source, "first_name", None),
                "username": getattr(source, "username", None),
                "accessible": True,
                "forwardsRestricted": bool(getattr(source, "noforwards", False)),
            }

    async def reconcile_target(self, request: ReconcileTargetRequest) -> dict:
        async with self.lock:
            target = await self.client.get_entity(request.target)
            return await self._reconcile_target_entity(target, request.max_messages)

    async def _reconcile_target_entity(self, target: Any, max_messages: int) -> dict:
        target_id = utils.get_peer_id(target)
        cursor = self.state.reconciliation_cursor(target_id) or 0
        messages = []
        async for message in self.client.iter_messages(
            target,
            min_id=cursor,
            reverse=True,
            limit=max_messages,
        ):
            messages.append(message)

        grouped = {}
        for message in messages:
            forward = getattr(message, "fwd_from", None)
            if forward is None:
                continue
            source_peer = getattr(forward, "from_id", None) or getattr(
                forward, "saved_from_peer", None
            )
            source_message_id = getattr(forward, "channel_post", None) or getattr(
                forward, "saved_from_msg_id", None
            )
            if source_peer is None or source_message_id is None:
                continue
            source_id = utils.get_peer_id(source_peer)
            album_key = getattr(message, "grouped_id", None) or message.id
            key = (source_id, album_key)
            group = grouped.setdefault(
                key,
                {"sourceMessageIds": [], "targetMessageIds": []},
            )
            group["sourceMessageIds"].append(source_message_id)
            group["targetMessageIds"].append(message.id)

        reconciled_groups = 0
        reconciled_messages = 0
        for (source_id, _), group in grouped.items():
            source_ids = sorted(set(group["sourceMessageIds"]))
            target_ids = sorted(set(group["targetMessageIds"]))
            group_key = ",".join(str(message_id) for message_id in source_ids)
            if self.ledger.get(target_id, source_id, group_key) is None:
                self.ledger.record(
                    target_id=target_id,
                    source_id=source_id,
                    group_key=group_key,
                    resource_key=str(source_ids[0]),
                    source_message_ids=source_ids,
                    target_message_ids=target_ids,
                    delivery_mode="MANUAL_RECONCILED",
                )
                reconciled_groups += 1
                reconciled_messages += len(source_ids)

        if messages:
            self.state.save_reconciliation_cursor(
                target_id, max(message.id for message in messages)
            )
        return {
            "targetId": target_id,
            "targetTitle": getattr(target, "title", None)
            or getattr(target, "first_name", None),
            "targetUsername": getattr(target, "username", None),
            "cursorBefore": cursor,
            "cursorAfter": max((message.id for message in messages), default=cursor),
            "scannedMessageCount": len(messages),
            "reconciledGroupCount": reconciled_groups,
            "reconciledMessageCount": reconciled_messages,
            "deliveryCount": self.ledger.count(),
        }

    async def _read_inbox_max_id(self, source: Any) -> int:
        input_peer = await self.client.get_input_entity(source)
        result = await self.client(
            GetPeerDialogsRequest(peers=[InputDialogPeer(input_peer)])
        )
        if not result.dialogs:
            raise RuntimeError("Source chat has no dialog/read state")
        return result.dialogs[0].read_inbox_max_id


def build_app(config: Config, client: TelegramClient) -> web.Application:
    def error_response(
        error: Exception,
        *,
        status: int = 500,
        code: str = "TELEGRAM_ERROR",
        retryable: bool = False,
        retry_after_seconds: Optional[int] = None,
    ) -> web.Response:
        return web.json_response(
            {
                "success": False,
                "errorType": type(error).__name__,
                "message": str(error),
                "error": {
                    "code": code,
                    "message": str(error),
                    "retryable": retryable,
                    "retryAfterSeconds": retry_after_seconds,
                },
            },
            status=status,
        )

    @web.middleware
    async def log_failures(request: web.Request, handler):
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except Exception as error:
            logger.exception("请求处理失败：%s %s", request.method, request.path)
            if isinstance(error, errors.FloodWaitError):
                return error_response(
                    error,
                    status=429,
                    code="FLOOD_WAIT",
                    retryable=True,
                    retry_after_seconds=error.seconds,
                )
            if isinstance(error, (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
                return error_response(
                    error,
                    status=503,
                    code="TELEGRAM_TIMEOUT",
                    retryable=True,
                )
            message = str(error)
            if "禁止转发" in message:
                return error_response(error, code="FORWARDS_RESTRICTED")
            if "历史有效资源不足" in message:
                return error_response(error, code="INSUFFICIENT_BASELINE")
            return error_response(error)

    @web.middleware
    async def authenticate(request: web.Request, handler):
        if request.path == "/health":
            return await handler(request)
        supplied = request.headers.get("Authorization", "")
        expected = f"Bearer {config.api_token}"
        if not hmac.compare_digest(supplied, expected):
            raise web.HTTPUnauthorized(text="missing or invalid bearer token")
        return await handler(request)

    app = web.Application(middlewares=[log_failures, authenticate])
    forwarder = TelegramForwarder(client, StateStore(config.state_path))

    async def health(_: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "ok",
                "authorized": await client.is_user_authorized(),
                "workerVersion": WORKER_VERSION,
                "stateWritable": os.access(config.state_path.parent, os.W_OK),
            }
        )

    async def resolve_source(request: web.Request) -> web.Response:
        try:
            command = ResolveSourceRequest.parse(await request.json())
        except (json.JSONDecodeError, ValueError) as error:
            return error_response(error, status=400, code="INVALID_REQUEST")
        try:
            return web.json_response(await forwarder.resolve_source(command))
        except (ValueError, errors.ChannelPrivateError):
            return error_response(
                ValueError("无法访问来源频道，请确认当前 Telegram 账号已加入该频道"),
                status=400,
                code="SOURCE_NOT_ACCESSIBLE",
            )

    async def reconcile_target(request: web.Request) -> web.Response:
        try:
            command = ReconcileTargetRequest.parse(await request.json())
        except (json.JSONDecodeError, ValueError) as error:
            return error_response(error, status=400, code="INVALID_REQUEST")
        return web.json_response(await forwarder.reconcile_target(command))

    async def operation_progress(request: web.Request) -> web.Response:
        return web.json_response(forwarder.progress(request.match_info["request_id"]))

    async def forward_unread(request: web.Request) -> web.Response:
        try:
            command = ForwardRequest.parse(await request.json())
        except (json.JSONDecodeError, ValueError) as error:
            return error_response(error, status=400, code="INVALID_REQUEST")
        return web.json_response(await forwarder.forward_unread(command))

    async def backfill(request: web.Request) -> web.Response:
        try:
            command = BackfillRequest.parse(await request.json())
        except (json.JSONDecodeError, ValueError) as error:
            return error_response(error, status=400, code="INVALID_REQUEST")
        return web.json_response(await forwarder.backfill(command))

    app.router.add_get("/health", health)
    app.router.add_post("/resolve-source", resolve_source)
    app.router.add_post("/reconcile-target", reconcile_target)
    app.router.add_get("/operations/{request_id}", operation_progress)
    app.router.add_post("/forward-unread", forward_unread)
    app.router.add_post("/backfill", backfill)
    return app


async def login(config: Config) -> None:
    config.session_path.parent.mkdir(parents=True, exist_ok=True)
    client = create_client(config)
    await client.start()
    me = await client.get_me()
    print(f"Session saved for Telegram user {me.id}")
    await client.disconnect()


def serve(config: Config) -> None:
    config.session_path.parent.mkdir(parents=True, exist_ok=True)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = create_client(config)

    async def client_lifecycle(_: web.Application):
        logger.info(
            "正在连接 Telegram，连接方式：%s。",
            f"代理 {config.proxy_host}:{config.proxy_port}" if config.proxy_host else "直接连接",
        )
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("Telegram session is not authorized; run: telegram-worker login")
        me = await client.get_me()
        logger.info("Telegram 连接成功，当前用户 ID：%d。", me.id)
        yield
        logger.info("正在断开 Telegram 连接……")
        await client.disconnect()

    app = build_app(config, client)
    app.cleanup_ctx.append(client_lifecycle)
    logger.info("Worker 正在启动，监听地址：http://%s:%d。", config.host, config.port)
    web.run_app(
        app,
        host=config.host,
        port=config.port,
        loop=loop,
        access_log=None,
        print=lambda _: logger.info("HTTP 服务已就绪，可开始发送测试请求；按 Ctrl+C 停止。"),
    )


def create_client(config: Config) -> TelegramClient:
    proxy = None
    if config.proxy_host and config.proxy_port:
        proxy = ("socks5", config.proxy_host, config.proxy_port)
    return TelegramClient(
        str(config.session_path),
        config.api_id,
        config.api_hash,
        proxy=proxy,
    )


def setup() -> None:
    env_path = Path(".env")
    if env_path.exists():
        raise RuntimeError(".env already exists; remove it manually if you want to recreate it")
    print("Opening Telegram API application page...")
    webbrowser.open("https://my.telegram.org/apps")
    print("Create an application there, then paste the two values below.")
    api_id = input("api_id: ").strip()
    api_hash = input("api_hash: ").strip()
    if not api_id.isdigit() or not api_hash:
        raise ValueError("api_id must be numeric and api_hash must be non-empty")
    proxy_host = input("SOCKS5 proxy host (blank for direct connection): ").strip()
    proxy_port = input("SOCKS5 proxy port: ").strip() if proxy_host else ""
    if proxy_host and not proxy_port.isdigit():
        raise ValueError("proxy port must be numeric")
    env_path.write_text(
        "\n".join(
            [
                f"TG_API_ID={api_id}",
                f"TG_API_HASH={api_hash}",
                "TG_SESSION_PATH=./data/telegram",
                f"TG_PROXY_HOST={proxy_host}",
                f"TG_PROXY_PORT={proxy_port}",
                f"WORKER_API_TOKEN={secrets.token_urlsafe(32)}",
                "WORKER_HOST=127.0.0.1",
                "WORKER_PORT=8081",
                "WORKER_LOG_LEVEL=INFO",
                "WORKER_STATE_PATH=./data/state.json",
                "",
            ]
        ),
        encoding="utf-8",
    )
    env_path.chmod(0o600)
    print("Created .env with a random WORKER_API_TOKEN.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Telegram forwarding worker")
    parser.add_argument("command", choices=("setup", "login", "serve"))
    args = parser.parse_args()
    if args.command == "setup":
        setup()
        return
    load_dotenv()
    configure_logging()
    config = Config.from_env()
    if args.command == "login":
        asyncio.run(login(config))
    else:
        serve(config)


if __name__ == "__main__":
    main()
