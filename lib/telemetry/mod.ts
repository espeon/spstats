import { NodeSDK } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { trace } from "@opentelemetry/api";
import pino from "pino";

const OTEL_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
  "http://localhost:4318/v1/traces";
const LOG_LEVEL = process.env.LOG_LEVEL || "info";

export function createLogger(name?: string) {
  return pino({
    name,
    level: LOG_LEVEL,
  });
}

export function initTelemetry(serviceName: string) {
  const logger = createLogger(serviceName);

  const sdk = new NodeSDK({
    traceExporter: new OTLPTraceExporter({
      url: OTEL_ENDPOINT,
    }),
    instrumentations: [getNodeAutoInstrumentations()],
    serviceName,
  });

  sdk.start();
  logger.info({ otel_endpoint: OTEL_ENDPOINT }, "otel initialized");

  process.on("SIGTERM", () => {
    sdk
      .shutdown()
      .then(() => logger.info("otel shutdown complete"))
      .catch((error) => logger.error({ error }, "otel shutdown failed"));
  });

  return { sdk, logger, tracer: trace.getTracer(serviceName) };
}

export { trace } from "@opentelemetry/api";
