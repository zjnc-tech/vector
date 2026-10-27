"""Synchronous embedded Vector pipelines backed by private Tokio workers."""
from ._native import (
    Pipeline, Runtime, SourceHandle, VectorRuntimeError, ClosedError,
    DeliveryError, QueueFullError, MAX_BATCH_EVENTS, __version__,
)

__all__ = [
    "Pipeline", "Runtime", "SourceHandle", "VectorRuntimeError", "ClosedError",
    "DeliveryError", "QueueFullError", "MAX_BATCH_EVENTS", "__version__",
]
from .events import external_event
__all__.append("external_event")
