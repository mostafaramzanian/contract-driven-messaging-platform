/**
 * otel-bootstrap.ts
 *
 * OpenTelemetry Node SDK bootstrap. MUST be imported/required as the very
 * first statement of every process entrypoint (apps/gateway/src/main.ts,
 * apps/messaging/src/main.ts) — before any other import — so that
 * auto-instrumentation can monkey-patch `pg`, `amqplib`, `http`, `express`
 * before those modules are first required by NestJS.
 *
 * Usage (top of main.ts, before all other imports):
 *
 *   import '@app/common/tracing/otel-bootstrap';
 *
 * Configuration (env vars):
 *   OTEL_SERVICE_NAME            service.name resource attribute
 *   OTEL_EXPORTER_OTLP_ENDPOINT  OTLP/HTTP collector endpoint
 *                                 (default: http://otel-collector:4318)
 *   OTEL_TRACES_SAMPLER_RATIO    0..1 ratio for ParentBasedSampler+TraceIdRatio
 *   OTEL_SDK_DISABLED            'true' to fully disable (used in unit tests)
 */
import { NodeSDK } from '@opentelemetry/sdk-node';
import { Resource } from '@opentelemetry/resources';
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import {
  ParentBasedSampler,
  TraceIdRatioBasedSampler,
} from '@opentelemetry/sdk-trace-base';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { AmqplibInstrumentation } from '@opentelemetry/instrumentation-amqplib';
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg';
import { diag, DiagConsoleLogger, DiagLogLevel } from '@opentelemetry/api';

if (process.env.OTEL_DEBUG === 'true') {
  diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);
}

const serviceName = process.env.OTEL_SERVICE_NAME ?? process.env.SERVICE_NAME ?? 'messaging-showcase';
const otlpEndpoint =
  process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'http://otel-collector:4318';
const samplerRatio = Number.parseFloat(
  process.env.OTEL_TRACES_SAMPLER_RATIO ?? '1.0',
);

let sdk: NodeSDK | undefined;

if (process.env.OTEL_SDK_DISABLED !== 'true') {
  const resource = new Resource({
    [ATTR_SERVICE_NAME]: serviceName,
    [ATTR_SERVICE_VERSION]: process.env.SERVICE_VERSION ?? '0.0.2',
    'deployment.environment': process.env.NODE_ENV ?? 'development',
  });

  const traceExporter = new OTLPTraceExporter({
    url: `${otlpEndpoint}/v1/traces`,
  });

  const metricExporter = new OTLPMetricExporter({
    url: `${otlpEndpoint}/v1/metrics`,
  });

  sdk = new NodeSDK({
    resource,
    traceExporter,
    sampler: new ParentBasedSampler({
      root: new TraceIdRatioBasedSampler(samplerRatio),
    }),
    metricReader: new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 15_000,
    }),
    instrumentations: [
      getNodeAutoInstrumentations({
        // Disable fs instrumentation — extremely noisy, no operational value here.
        '@opentelemetry/instrumentation-fs': { enabled: false },
        '@opentelemetry/instrumentation-net': { enabled: false },
      }),
      new AmqplibInstrumentation({
        // Capture message payload size as a span attribute, not the body
        // itself (avoid leaking PII / large payloads into trace backends).
        consumeHook: (span, info) => {
          span.setAttribute(
            'messaging.message_payload_size_bytes',
            info.msg?.content?.length ?? 0,
          );
        },
        publishHook: (span, info) => {
          span.setAttribute(
            'messaging.message_payload_size_bytes',
            info.msg?.length ?? 0,
          );
        },
      }),
      new PgInstrumentation({
        enhancedDatabaseReporting: true,
      }),
    ],
  });

  try {
    sdk.start();
    // eslint-disable-next-line no-console
    console.log(
      `[otel] tracing initialized service=${serviceName} endpoint=${otlpEndpoint} sampler=${samplerRatio}`,
    );
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[otel] failed to initialize SDK', err);
  }

  const shutdown = () => {
    sdk
      ?.shutdown()
      .then(() => console.log('[otel] SDK shut down cleanly'))
      .catch((err) => console.error('[otel] error shutting down SDK', err))
      .finally(() => process.exit(0));
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
} else {
  // eslint-disable-next-line no-console
  console.log('[otel] SDK disabled via OTEL_SDK_DISABLED=true');
}

export { sdk as otelSdk };
