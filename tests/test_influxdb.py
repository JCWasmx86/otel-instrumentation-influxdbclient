import os
import random

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_RESPONSE_STATUS_CODE,
    DB_SYSTEM,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import SpanKind, StatusCode

from otel_instrumentation_influxdbclient import InfluxDBClientInstrumentor


class TestInfluxDB(TestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.org = os.getenv("INFLUXDB_ORG", "my-org")

    def setUp(self):
        super().setUp()
        InfluxDBClientInstrumentor().instrument(tracer_provider=self.tracer_provider)
        self.bucketname = "bucket-" + random.randbytes(7).hex()
        self.bucket = (
            self.make_client().buckets_api().create_bucket(bucket_name=self.bucketname)
        )

    def tearDown(self):
        super().tearDown()
        self.make_client().buckets_api().delete_bucket(bucket=self.bucket.id)
        InfluxDBClientInstrumentor().uninstrument()

    def make_client(self):
        return InfluxDBClient(
            os.getenv("INFLUXDB_URL", "http://localhost:8086"),
            token=os.getenv("INFLUXDB_TOKEN"),
            org=self.org,
        )

    def test_basic_write(self):
        client = self.make_client()
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            write_api.write(
                bucket=self.bucketname,
                record=Point("otel").tag("key", "value").field("field", 1),
            )
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.name, f"WRITE {self.org}.{self.bucketname}")
        self.assertEqual(span.kind, SpanKind.CLIENT)
        self.assertEqual(span.status.status_code, StatusCode.OK)
        self.assertEqual(span.attributes[DB_SYSTEM], "influxdb")
        self.assertEqual(span.attributes[DB_NAMESPACE], self.bucketname)
        self.assertEqual(span.attributes[DB_OPERATION_NAME], "WRITE")

    def test_failed_write(self):
        client = self.make_client()
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            try:
                write_api.write(
                    bucket=self.bucketname,
                    record=Point("").tag("key", "value").field("field", 1),
                )
                self.assertTrue(False, "Should have raised an exception")
            except ApiException:
                pass
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.name, f"WRITE {self.org}.{self.bucketname}")
        self.assertEqual(span.kind, SpanKind.CLIENT)
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertIn(ERROR_TYPE, span.attributes)
        self.assertIn(DB_RESPONSE_STATUS_CODE, span.attributes)

    def test_query(self):
        client = self.make_client()
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            write_api.write(
                bucket=self.bucketname,
                record=Point("test").tag("key", "value").field("field", 1),
            )
        query_api = client.query_api()
        res = query_api.query(f'from(bucket:"{self.bucketname}") |> range(start: -1h)')
        self.assertEqual(len(res), 1)
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 2)
        span = spans[1]
        self.assertEqual(span.name, f"QUERY {self.org}")
        self.assertEqual(span.kind, SpanKind.CLIENT)
        self.assertEqual(span.status.status_code, StatusCode.OK)
        self.assertEqual(span.attributes[DB_OPERATION_NAME], "QUERY")

    def test_query_failed(self):
        client = self.make_client()
        query_api = client.query_api()
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            write_api.write(
                bucket=self.bucketname,
                record=Point("test").tag("key", "value").field("field", 1),
            )
        query_api = client.query_api()
        with self.assertRaises(ApiException):
            _ = query_api.query(
                f'from(bucket:"{self.bucketname}ffff") |> range(start: -1h)'
            )
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 2)
        span = spans[1]
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertIn(ERROR_TYPE, span.attributes)
        self.assertIn(DB_RESPONSE_STATUS_CODE, span.attributes)
