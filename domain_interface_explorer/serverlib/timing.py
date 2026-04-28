from __future__ import annotations

import time
from typing import Any


def _format_elapsed(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.1f} ms"
    if seconds < 60:
        return f"{seconds:.2f} s"
    return f"{seconds / 60:.2f} min"


def _format_context(context: dict[str, Any]) -> str:
    parts: list[str] = []
    for key, value in context.items():
        if value is None:
            continue
        text = str(value)
        if len(text) > 180:
            text = text[:177] + "..."
        parts.append(f"{key}={text}")
    return (" " + " ".join(parts)) if parts else ""


def log_event(scope: str, action: str, **context: Any) -> None:
    print(f"[{scope}] {action}{_format_context(context)}", flush=True)


class TimedStep:
    def __init__(self, scope: str, action: str, **context: Any) -> None:
        self.scope = scope
        self.action = action
        self.context = dict(context)
        self.started_at = 0.0

    def __enter__(self) -> "TimedStep":
        self.started_at = time.perf_counter()
        log_event(self.scope, f"{self.action} started", **self.context)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        elapsed = time.perf_counter() - self.started_at
        if exc is None:
            log_event(
                self.scope,
                f"{self.action} finished in {_format_elapsed(elapsed)}",
                **self.context,
            )
        else:
            log_event(
                self.scope,
                f"{self.action} failed after {_format_elapsed(elapsed)}",
                **self.context,
                error=exc,
            )
        return False

    def set(self, **context: Any) -> None:
        self.context.update(context)


def timed_step(scope: str, action: str, **context: Any) -> TimedStep:
    return TimedStep(scope, action, **context)
