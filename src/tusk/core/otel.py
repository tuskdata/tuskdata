"""OpenTelemetry opt-in initialization.

Reads `TUSK_OTEL_ENDPOINT` to decide whether to wire up the SDK. If the
env var is unset, this module is a no-op — TuskData runs fine without
the OTEL libraries installed (`tuskdata[studio]` does NOT pull them).

To enable, install the optional extra and set the env var:

    pip install tuskdata[otel]
    export TUSK_OTEL_ENDPOINT=http://collector:4318
    export TUSK_OTEL_SERVICE_NAME=tusk

All `opentelemetry` imports happen INSIDE `init_otel()` so a missing
`opentelemetry-api` doesn't break Tusk Studio at import time.
"""

from __future__ import annotations

import os

from tusk.core.logging import get_logger

log = get_logger("otel")

_initialized: bool = False


def init_otel() -> bool:
    """Initialize the OpenTelemetry SDK if `TUSK_OTEL_ENDPOINT` is set.

    Returns True if OTEL was initialized, False otherwise (env var
    missing, libraries not installed, or already initialized).
    Failures are logged at WARNING level — they never raise.
    """
    global _initialized
    if _initialized:
        return False

    endpoint = os.environ.get("TUSK_OTEL_ENDPOINT", "").strip()
    if not endpoint:
        return False

    service_name = os.environ.get("TUSK_OTEL_SERVICE_NAME", "tusk").strip() or "tusk"

    try:
        # All imports inside the function — none of these are guaranteed
        # to be installed. `tuskdata[otel]` brings them in.
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    except ImportError as e:
        log.warning(
            "OTEL libraries not installed; install with `pip install tuskdata[otel]`",
            error=str(e),
        )
        return False

    try:
        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)

        # OTLP HTTP collector — append /v1/traces if the user gave us
        # the bare collector URL.
        traces_endpoint = endpoint.rstrip("/")
        if not traces_endpoint.endswith("/v1/traces"):
            traces_endpoint = f"{traces_endpoint}/v1/traces"

        exporter = OTLPSpanExporter(endpoint=traces_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        # Litestar instrumentation — optional, best-effort.
        try:
            from opentelemetry.instrumentation.litestar import LitestarInstrumentor
            LitestarInstrumentor().instrument()
        except ImportError:
            log.info("Litestar OTEL instrumentation not installed; HTTP spans will be missing")
        except Exception as e:
            log.warning("Failed to instrument Litestar", error=str(e))

        _initialized = True
        log.info("OTEL initialized", endpoint=traces_endpoint, service_name=service_name)
        return True
    except Exception as e:
        log.warning("Failed to initialize OTEL", error=str(e))
        return False
