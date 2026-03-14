from typing import Any, Callable, Collection, TypeVar

from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.flux_table import TableList
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import WriteApi
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_COLLECTION_NAME,
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_RESPONSE_RETURNED_ROWS,
    DB_RESPONSE_STATUS_CODE,
    DB_SYSTEM,
)
from opentelemetry.semconv._incubating.attributes.net_attributes import (
    NET_PEER_NAME,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.trace import Tracer, TracerProvider, get_tracer, StatusCode
from wrapt import wrap_function_wrapper

from .package import _instruments
from .version import __version__

R = TypeVar("R")


def _instrument(tracer: Tracer):
    def _traced_query_comment(
        func: Callable[..., R],
        instance: QueryApi,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ):
        org = (
            str(kwargs.get("org"))
            if "org" in kwargs
            else (
                args[1]
                if args and len(args) > 1 and isinstance(args[1], str)
                else (instance._influxdb_client.org or "$$UNKNOWN ORG$$")
            )
        )
        with tracer.start_as_current_span(
            f"QUERY {org}", kind=trace.SpanKind.CLIENT
        ) as span:
            span.set_attribute(DB_SYSTEM, "influxdb")
            span.set_attribute(DB_OPERATION_NAME, "WRITE")
            url = instance._influxdb_client.url
            span.set_attribute(NET_PEER_NAME, url)
            try:
                resp: TableList = func(*args, **kwargs)
                span.set_attribute(
                    DB_RESPONSE_RETURNED_ROWS, sum(len(x.records) for x in resp)
                )
                span.set_status(StatusCode.OK)
                return resp
            except InfluxDBError as err:
                span.set_attribute(ERROR_TYPE, str(err.message))
                if err.response:
                    span.set_attribute(DB_RESPONSE_STATUS_CODE, err.response.status)
                raise
        return func(*args, **kwargs)

    def _traced_write_command(
        func: Callable[..., R],
        instance: WriteApi,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        bucket = (
            str(kwargs.get("bucket"))
            if "bucket" in kwargs
            else (
                args[0] if args and isinstance(args[0], str) else "$$UNKNOWN BUCKET$$"
            )
        )
        org = (
            str(kwargs.get("org"))
            if "org" in kwargs
            else (
                args[1]
                if args and len(args) > 1 and isinstance(args[1], str)
                else (instance._influxdb_client.org or "$$UNKNOWN ORG$$")
            )
        )
        with tracer.start_as_current_span(
            f"WRITE {org}.{bucket}", kind=trace.SpanKind.CLIENT
        ) as span:
            span.set_attribute(DB_SYSTEM, "influxdb")
            span.set_attribute(DB_COLLECTION_NAME, f"{org}.{bucket}")
            span.set_attribute(DB_NAMESPACE, f"{bucket}")
            span.set_attribute(DB_OPERATION_NAME, "WRITE")
            url = instance._influxdb_client.url
            span.set_attribute(NET_PEER_NAME, url)
            try:
                resp = func(*args, **kwargs)
                span.set_status(StatusCode.OK)
                return resp
            except InfluxDBError as err:
                span.set_attribute(ERROR_TYPE, str(err.message))
                if err.response:
                    span.set_attribute(DB_RESPONSE_STATUS_CODE, err.response.status)
                raise
    wrap_function_wrapper(
        "influxdb_client.client.write_api", "WriteApi.write", _traced_write_command
    )
    wrap_function_wrapper(
        "influxdb_client.client.query_api", "QueryApi.query", _traced_query_comment
    )


class InfluxDBClientInstrumentor(BaseInstrumentor):
    @staticmethod
    def _get_tracer(**kwargs):
        tracer_provider = kwargs.get("tracer_provider")
        return get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
        )

    def _instrument(self, **kwargs: Any):
        _instrument(
            self._get_tracer(**kwargs),
        )

    def _uninstrument(self, **kwargs: Any):
        unwrap(WriteApi, "write")
        unwrap(QueryApi, "query")

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments
