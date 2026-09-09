package com.example.jobs.pull.runtime;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;

public final class Telemetry {
  private Telemetry() {}
  public static VertxOptions options() {
    String endpoint=System.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    if(endpoint!=null && !endpoint.isBlank()) {
      var provider=SdkTracerProvider.builder().addSpanProcessor(BatchSpanProcessor.builder(
          OtlpHttpSpanExporter.builder().setEndpoint(endpoint).build()).build()).build();
      var sdk=OpenTelemetrySdk.builder().setTracerProvider(provider).build();
      GlobalOpenTelemetry.set(sdk);
      Runtime.getRuntime().addShutdownHook(new Thread(sdk::close,"trace-shutdown"));
    }
    return new VertxOptions().setMetricsOptions(new MicrometerMetricsOptions().setEnabled(true).setJvmMetricsEnabled(true)
        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true).setStartEmbeddedServer(false)))
        .setTracingOptions(new OpenTelemetryOptions());
  }
  public static String scrape() {
    var registry=BackendRegistries.getDefaultNow();
    return registry instanceof PrometheusMeterRegistry prometheus ? prometheus.scrape():"";
  }
}
