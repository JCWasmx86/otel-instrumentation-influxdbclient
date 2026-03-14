# otel-instrumentation-influxdbclient

**This package is just some small utility package I wrote. It's neither associated with OpenTelemetry, nor with InfluxData.**

This package provides a simple OpenTelemetry instrumentation for the [influxdb-client](https://pypi.org/project/influxdb-client/) package. As
it's written for my needs (Contributions welcome!), it only covers following:

- Traces: Only traces are emitted, no metrics etc.
- Sync only: No async methods are instrumented, as I currently don't need this
- Instrumented features:
  - Querying (In a limited manner)
  - Writing

