from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Union


SourceRef = Union[int, str]
PRIVATE_CHANNEL_LINK = re.compile(
    r"^(?:https?://)?(?:www\.)?t\.me/c/([1-9]\d*)(?:/\d+)?/?(?:\?.*)?$",
    re.IGNORECASE,
)


def _source(body: dict) -> SourceRef:
    value = body.get("source")
    if isinstance(value, bool) or not (
        isinstance(value, int) or isinstance(value, str) and value.strip()
    ):
        raise ValueError("source must be a channel ID or non-empty string")
    if not isinstance(value, str):
        return value
    normalized = value.strip()
    private_link = PRIVATE_CHANNEL_LINK.fullmatch(normalized)
    if private_link:
        return -int(f"100{private_link.group(1)}")
    return normalized


def _target(body: dict) -> str:
    value = body.get("target")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("target must be a non-empty string")
    return value.strip()


def _body(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ValueError("JSON body must be an object")
    return value


@dataclass(frozen=True)
class ForwardRequest:
    source: SourceRef
    target: str
    limit: Optional[int]
    min_video_duration: int
    mark_read: bool = False
    percentile: float = 0.90
    baseline_size: int = 100
    min_views: int = 5000
    min_forwards: int = 10
    min_age_hours: int = 24
    max_resources: Optional[int] = None
    resource_mode: str = "group"
    group_interval_seconds: float = 1.0
    resource_interval_seconds: float = 3.0
    dry_run: bool = False
    request_id: Optional[str] = None

    @classmethod
    def parse(cls, value: Any) -> "ForwardRequest":
        body = _body(value)
        limit = body.get("maxMessages", body.get("limit"))
        duration = body.get("minVideoDuration", 300)
        mark_read = body.get("markRead", False)
        percentile = body.get("percentile", 0.90)
        baseline_size = body.get("baselineSize", 100)
        min_views = body.get("minViews", 5000)
        min_forwards = body.get("minForwards", 10)
        min_age_hours = body.get("minAgeHours", 24)
        max_resources = body.get("maxResources")
        resource_mode = body.get("resourceMode", "group")
        group_interval = body.get("groupIntervalSeconds", 1.0)
        resource_interval = body.get("resourceIntervalSeconds", 3.0)
        dry_run = body.get("dryRun", False)
        request_id = body.get("requestId")
        if not isinstance(mark_read, bool):
            raise ValueError("markRead must be a boolean")
        if not isinstance(dry_run, bool):
            raise ValueError("dryRun must be a boolean")
        if request_id is not None and (not isinstance(request_id, str) or not request_id.strip()):
            raise ValueError("requestId must be null or a non-empty string")
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000
        ):
            raise ValueError("maxMessages must be null or an integer between 1 and 5000")
        if isinstance(duration, bool) or not isinstance(duration, int) or duration < 0:
            raise ValueError("minVideoDuration must be a non-negative integer")
        if isinstance(percentile, bool) or not isinstance(percentile, (int, float)) or not 0 < percentile < 1:
            raise ValueError("percentile must be a number between 0 and 1")
        if isinstance(baseline_size, bool) or not isinstance(baseline_size, int) or not 30 <= baseline_size <= 1000:
            raise ValueError("baselineSize must be an integer between 30 and 1000")
        for name, item in (("minViews", min_views), ("minForwards", min_forwards), ("minAgeHours", min_age_hours)):
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if max_resources is not None and (
            isinstance(max_resources, bool) or not isinstance(max_resources, int) or max_resources < 1
        ):
            raise ValueError("maxResources must be null or a positive integer")
        if resource_mode not in ("group", "hashtag_resource"):
            raise ValueError("resourceMode must be group or hashtag_resource")
        for name, item in (("groupIntervalSeconds", group_interval), ("resourceIntervalSeconds", resource_interval)):
            if isinstance(item, bool) or not isinstance(item, (int, float)) or item < 0:
                raise ValueError(f"{name} must be a non-negative number")
        return cls(
            source=_source(body),
            target=_target(body),
            limit=limit,
            min_video_duration=duration,
            mark_read=mark_read,
            percentile=float(percentile),
            baseline_size=baseline_size,
            min_views=min_views,
            min_forwards=min_forwards,
            min_age_hours=min_age_hours,
            max_resources=max_resources,
            resource_mode=resource_mode,
            group_interval_seconds=float(group_interval),
            resource_interval_seconds=float(resource_interval),
            dry_run=dry_run,
            request_id=request_id.strip() if request_id else None,
        )


@dataclass(frozen=True)
class BackfillRequest:
    source: SourceRef
    target: str
    lookback_days: int = 180
    top_resources: int = 10
    max_messages: int = 5000
    min_video_duration: int = 300
    min_views: int = 5000
    min_forwards: int = 10
    resource_mode: str = "group"
    group_interval_seconds: float = 1.0
    resource_interval_seconds: float = 3.0
    dry_run: bool = False
    start_mode: str = "continue"
    request_id: Optional[str] = None

    @classmethod
    def parse(cls, value: Any) -> "BackfillRequest":
        body = _body(value)
        values = {
            "lookbackDays": body.get("lookbackDays", 180),
            "topResources": body.get("topResources", 10),
            "maxMessages": body.get("maxMessages", 5000),
            "minVideoDuration": body.get("minVideoDuration", 300),
            "minViews": body.get("minViews", 5000),
            "minForwards": body.get("minForwards", 10),
        }
        bounds = {
            "lookbackDays": (1, 3650),
            "topResources": (1, 100),
            "maxMessages": (200, 10000),
            "minVideoDuration": (0, 86400),
            "minViews": (0, 1_000_000_000),
            "minForwards": (0, 1_000_000_000),
        }
        for name, item in values.items():
            lower, upper = bounds[name]
            if isinstance(item, bool) or not isinstance(item, int) or not lower <= item <= upper:
                raise ValueError(f"{name} must be an integer between {lower} and {upper}")
        mode = body.get("resourceMode", "group")
        if mode not in ("group", "hashtag_resource"):
            raise ValueError("resourceMode must be group or hashtag_resource")
        start_mode = body.get("startMode", "continue")
        if start_mode not in ("continue", "latest"):
            raise ValueError("startMode must be continue or latest")
        dry_run = body.get("dryRun", False)
        if not isinstance(dry_run, bool):
            raise ValueError("dryRun must be a boolean")
        request_id = body.get("requestId")
        if request_id is not None and (not isinstance(request_id, str) or not request_id.strip()):
            raise ValueError("requestId must be null or a non-empty string")
        intervals = (
            body.get("groupIntervalSeconds", 1.0),
            body.get("resourceIntervalSeconds", 3.0),
        )
        if any(isinstance(item, bool) or not isinstance(item, (int, float)) or item < 0 for item in intervals):
            raise ValueError("intervals must be non-negative numbers")
        return cls(
            source=_source(body),
            target=_target(body),
            lookback_days=values["lookbackDays"],
            top_resources=values["topResources"],
            max_messages=values["maxMessages"],
            min_video_duration=values["minVideoDuration"],
            min_views=values["minViews"],
            min_forwards=values["minForwards"],
            resource_mode=mode,
            group_interval_seconds=float(intervals[0]),
            resource_interval_seconds=float(intervals[1]),
            dry_run=dry_run,
            start_mode=start_mode,
            request_id=request_id.strip() if request_id else None,
        )


@dataclass(frozen=True)
class ResolveSourceRequest:
    source: SourceRef

    @classmethod
    def parse(cls, value: Any) -> "ResolveSourceRequest":
        return cls(_source(_body(value)))


@dataclass(frozen=True)
class ReconcileTargetRequest:
    target: str
    max_messages: int = 10000

    @classmethod
    def parse(cls, value: Any) -> "ReconcileTargetRequest":
        body = _body(value)
        max_messages = body.get("maxMessages", 10000)
        if isinstance(max_messages, bool) or not isinstance(max_messages, int) or not 1 <= max_messages <= 50000:
            raise ValueError("maxMessages must be an integer between 1 and 50000")
        return cls(_target(body), max_messages)
