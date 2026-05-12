"""
JobScheduler — register repeatable job factories.

Port of `src/classes/job-scheduler.ts`. A scheduler is a Redis-side
record (`<prefix>:<queue>:repeat:<id>` hash + entry in the `repeat`
sorted set) that owns a single in-flight delayed job representing its
next iteration. When that job completes, the worker calls
`updateJobSchedulerNextMillis` to materialize the following one.

Two scheduling strategies are supported:

- `every`: fire every N milliseconds. Iteration math happens entirely
  in Lua, so no Python-side computation is needed.
- `pattern`: cron expression evaluated in Python via `croniter` and
  passed to Lua as an absolute `nextMillis`. The timezone (`tz`) is
  honoured when supplied.

The class itself is intentionally lightweight: it reuses the parent
`Queue`'s `Scripts` instance and connection rather than opening a new
one, mirroring how the Node side composes `JobScheduler` onto its
`QueueBase`.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from bullmq.job import Job
from bullmq.types import RepeatOptions

if TYPE_CHECKING:
    from bullmq.queue import Queue


def _to_millis(value) -> Optional[int]:
    """Coerce a date-ish input (epoch millis int/float or ISO 8601 string)
    into integer epoch milliseconds. Returns None for empty input."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
        # Accept ISO 8601 (datetime.fromisoformat handles common forms).
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    return None


def default_repeat_strategy(millis: int, opts: RepeatOptions) -> Optional[int]:
    """
    Resolve the next iteration timestamp (in epoch millis) for a cron
    `pattern`. Returns None if no pattern is set or parsing fails.

    Mirrors the Node `defaultRepeatStrategy`: when `immediately` is true
    the current wall-clock millis are returned. `startDate` clamps the
    base date forward. `tz` is honoured by evaluating the cron in that
    timezone via `pytz`.
    """
    pattern = opts.get("pattern")
    if not pattern:
        return None

    if opts.get("immediately"):
        return int(time.time() * 1000)

    base_ms = millis
    start_ms = _to_millis(opts.get("startDate"))
    if start_ms is not None and start_ms > base_ms:
        base_ms = start_ms

    tz_name = opts.get("tz")
    base_dt: datetime
    if tz_name:
        try:
            import pytz  # croniter installs pytz transitively
            tzinfo = pytz.timezone(tz_name)
            base_dt = datetime.fromtimestamp(base_ms / 1000.0, tz=tzinfo)
        except Exception:
            base_dt = datetime.fromtimestamp(base_ms / 1000.0, tz=timezone.utc)
    else:
        base_dt = datetime.fromtimestamp(base_ms / 1000.0, tz=timezone.utc)

    try:
        from croniter import croniter
        itr = croniter(pattern, base_dt)
        next_dt = itr.get_next(datetime)
        return int(next_dt.timestamp() * 1000)
    except Exception:
        return None


class JobScheduler:
    """
    Manage repeatable job factories ("job schedulers") on a Queue.

    A `JobScheduler` instance is bound to a single `Queue`; it does not
    own a connection of its own. Use `Queue.upsertJobScheduler(...)` as
    the user-facing entry point; this class exists to keep the
    scheduler-related logic isolated from `Queue`.
    """

    def __init__(self, queue: "Queue", repeat_strategy=None):
        self.queue = queue
        self.scripts = queue.scripts
        self.repeat_strategy = repeat_strategy or default_repeat_strategy

    async def upsertJobScheduler(
        self,
        job_scheduler_id: str,
        repeat_opts: RepeatOptions,
        job_name: str,
        job_data: Any = None,
        opts: Optional[dict] = None,
        override: bool = True,
        producer_id: Optional[str] = None,
    ) -> Optional[Job]:
        """
        Create or update a job scheduler. Returns the `Job` representing
        the next iteration, or `None` if no iteration was produced
        (limit reached, end date exceeded, pattern unparseable).

        Validation mirrors Node:
        - exactly one of `pattern` / `every` must be given;
        - `immediately` and `startDate` are mutually exclusive;
        - `immediately` + `every` is allowed but has no effect.
        """
        opts = dict(opts or {})
        repeat_opts = dict(repeat_opts or {})

        every = repeat_opts.get("every")
        pattern = repeat_opts.get("pattern")
        limit = repeat_opts.get("limit")
        offset = repeat_opts.get("offset")

        if pattern and every:
            raise ValueError(
                "Both .pattern and .every options are defined for this repeatable job"
            )
        if not pattern and not every:
            raise ValueError(
                "Either .pattern or .every options must be defined for this repeatable job"
            )
        if repeat_opts.get("immediately") and repeat_opts.get("startDate"):
            raise ValueError(
                "Both .immediately and .startDate options are defined for this repeatable job"
            )

        # Iteration count + limit guard.
        iteration_count = (repeat_opts.get("count") or 0) + 1
        if limit is not None and iteration_count > limit:
            return None

        now = int(time.time() * 1000)
        end_ms = _to_millis(repeat_opts.get("endDate"))
        if end_ms is not None and now > end_ms:
            return None

        prev_millis = opts.get("prevMillis") or 0
        if prev_millis > now:
            now = prev_millis

        # `immediately` is a Python-side hint; never persist it to Redis.
        filtered_repeat_opts = {
            k: v for k, v in repeat_opts.items() if k != "immediately"
        }

        new_offset = offset if (every and offset) else None

        next_millis: Optional[int] = None
        if pattern:
            next_millis = self.repeat_strategy(now, repeat_opts)
            if next_millis is None:
                return None
            if next_millis < now:
                next_millis = now

        if not next_millis and not every:
            return None

        # Compose the opts the next iteration's job will be stored with.
        merged_opts = self._build_next_job_opts(
            next_millis,
            job_scheduler_id,
            {**opts, "repeat": filtered_repeat_opts},
            iteration_count,
            new_offset,
        )

        template_data_str = json.dumps(
            job_data if job_data is not None else {},
            separators=(",", ":"),
            allow_nan=False,
        )

        if override:
            clamped_next = max(next_millis or now, now)
            scheduler_opts = {
                "name": job_name,
                "tz": repeat_opts.get("tz"),
                "pattern": pattern,
                "every": every,
                "limit": limit,
                "offset": new_offset,
                "startDate": _to_millis(repeat_opts.get("startDate")),
                "endDate": end_ms,
            }
            scheduler_opts = {
                k: v for k, v in scheduler_opts.items() if v is not None
            }

            result = await self.scripts.addJobScheduler(
                job_scheduler_id,
                clamped_next,
                template_data_str,
                opts,
                scheduler_opts,
                merged_opts,
                producer_id,
            )
            if not result:
                return None
            job_id, delay = result[0], result[1]
            try:
                delay = int(delay)
            except (TypeError, ValueError):
                delay = 0
            job = Job(
                self.queue,
                job_name,
                job_data,
                {**merged_opts, "delay": delay},
                job_id,
            )
            job.id = job_id
            return job

        # Non-override path: only advance the next-millis pointer.
        job_id = await self.scripts.updateJobSchedulerNextMillis(
            job_scheduler_id,
            next_millis or now,
            template_data_str,
            merged_opts,
            producer_id,
        )
        if not job_id:
            return None
        job = Job(self.queue, job_name, job_data, merged_opts, job_id)
        job.id = job_id
        return job

    def _build_next_job_opts(
        self,
        next_millis: Optional[int],
        job_scheduler_id: str,
        opts: dict,
        iteration_count: int,
        offset: Optional[int],
    ) -> dict:
        """Compose the opts that the next iteration's Job is created
        with. Matches Node's `getNextJobOpts`."""
        now = int(time.time() * 1000)
        base_next = next_millis or now
        delay = base_next + (offset or 0) - now
        if delay < 0:
            delay = 0

        repeat_in = opts.get("repeat") or {}
        merged_repeat = {
            **repeat_in,
            "offset": offset,
            "count": iteration_count,
            "startDate": _to_millis(repeat_in.get("startDate")),
            "endDate": _to_millis(repeat_in.get("endDate")),
        }
        # Drop None values so they don't leak into the encoded opts.
        merged_repeat = {k: v for k, v in merged_repeat.items() if v is not None}

        merged = {
            **opts,
            "jobId": f"repeat:{job_scheduler_id}:{base_next}",
            "delay": delay,
            "timestamp": now,
            "prevMillis": base_next,
            "repeatJobKey": job_scheduler_id,
            "repeat": merged_repeat,
        }
        return merged

    async def removeJobScheduler(self, job_scheduler_id: str) -> int:
        """Remove a scheduler. Returns 0 on success, 1 if absent."""
        return await self.scripts.removeJobScheduler(job_scheduler_id)

    async def isJobScheduler(self, job_scheduler_id: str) -> bool:
        """
        Return True if `job_scheduler_id` corresponds to a registered
        scheduler. Probes the `ic` field on the per-id hash so that
        legacy repeatable-job ids stored in the same sorted set are not
        misclassified as schedulers. Mirrors Node's `isJobScheduler`.
        """
        scheduler_hash_key = f"{self.queue.keys['repeat']}:{job_scheduler_id}"
        exists = await self.queue.client.hexists(scheduler_hash_key, "ic")
        return exists == 1

    async def getScheduler(self, job_scheduler_id: str) -> Optional[dict]:
        """Return the JSON-shaped scheduler record, or None."""
        raw, score = await self.scripts.getJobScheduler(job_scheduler_id)
        next_millis = int(score) if score is not None else None
        if not raw:
            return None
        fields = _array_to_dict(raw)
        return _transform_scheduler_data(job_scheduler_id, fields, next_millis)

    async def getJobSchedulers(
        self, start: int = 0, end: int = -1, asc: bool = False
    ) -> list:
        """Page through registered schedulers. `asc=True` returns
        earliest-next-fire first."""
        repeat_key = self.queue.keys["repeat"]
        if asc:
            raw = await self.queue.client.zrange(
                repeat_key, start, end, withscores=True
            )
        else:
            raw = await self.queue.client.zrevrange(
                repeat_key, start, end, withscores=True
            )

        out = []
        for member, score in raw:
            fields_raw = await self.queue.client.hgetall(
                f"{repeat_key}:{member}"
            )
            try:
                next_millis = int(score)
            except (TypeError, ValueError):
                next_millis = None
            data = _transform_scheduler_data(member, fields_raw, next_millis)
            if data is not None:
                out.append(data)
        return out

    async def getSchedulersCount(self) -> int:
        """Total number of registered schedulers."""
        return await self.queue.client.zcard(self.queue.keys["repeat"])


def _array_to_dict(arr) -> dict:
    """Turn the Lua-flat `[k1, v1, k2, v2, ...]` shape into a dict.
    `redis-py` may also already hand us a dict (depending on the
    response policy); pass it through unchanged in that case."""
    if isinstance(arr, dict):
        return arr
    if not arr:
        return {}
    out = {}
    for i in range(0, len(arr), 2):
        out[arr[i]] = arr[i + 1]
    return out


def _transform_scheduler_data(
    key: str, fields: dict, next_millis: Optional[int]
) -> Optional[dict]:
    """Mirror `JobScheduler.transformSchedulerData` from the Node port."""
    if not fields:
        return None

    out: dict = {"key": key, "name": fields.get("name"), "next": next_millis}

    def _int(field):
        try:
            return int(fields[field])
        except (KeyError, TypeError, ValueError):
            return None

    ic = _int("ic")
    if ic is not None:
        out["iterationCount"] = ic
    limit = _int("limit")
    if limit is not None:
        out["limit"] = limit
    start_date = _int("startDate")
    if start_date is not None:
        out["startDate"] = start_date
    end_date = _int("endDate")
    if end_date is not None:
        out["endDate"] = end_date

    tz = fields.get("tz")
    if tz:
        out["tz"] = tz
    pattern = fields.get("pattern")
    if pattern:
        out["pattern"] = pattern
    every = _int("every")
    if every is not None:
        out["every"] = every
    offset = _int("offset")
    if offset is not None:
        out["offset"] = offset

    raw_data = fields.get("data")
    raw_opts = fields.get("opts")
    if raw_data or raw_opts:
        template = {}
        if raw_data:
            try:
                template["data"] = json.loads(raw_data)
            except (TypeError, ValueError):
                template["data"] = raw_data
        if raw_opts:
            try:
                template["opts"] = json.loads(raw_opts)
            except (TypeError, ValueError):
                template["opts"] = raw_opts
        out["template"] = template

    return out
