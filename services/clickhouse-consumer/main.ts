import { createClient } from "@clickhouse/client";
import pkg from "kafkajs";
const { Kafka } = pkg;
import { initTelemetry } from "@sp-stats/telemetry";

const { logger, tracer } = initTelemetry("clickhouse-consumer");

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:19092").split(
  ",",
);
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "stream-place-events";
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || "clickhouse-consumer";
const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL || "http://localhost:8123";
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || "sp_stats";

interface TapEvent {
  type: "record" | "identity";
  did: string;
  atUri: string;
  timestamp: number;
  createdAt?: string;
  action?: "create" | "update" | "delete";
  isBackfill: boolean;
  collection?: string;
  rkey?: string;
  recordData?: any;
}

class ClickHouseConsumer {
  private kafka: any;
  private consumer: any;
  private clickhouse: ReturnType<typeof createClient>;
  private running = false;

  constructor() {
    this.kafka = new Kafka({
      clientId: "clickhouse-consumer",
      brokers: KAFKA_BROKERS,
    });

    this.consumer = this.kafka.consumer({
      groupId: KAFKA_GROUP_ID,
    });

    this.clickhouse = createClient({
      url: CLICKHOUSE_URL,
      database: CLICKHOUSE_DATABASE,
    });
  }

  async start() {
    return tracer.startActiveSpan("clickhouse-consumer.start", async (span) => {
      try {
        logger.info("connecting to kafka...");
        await this.consumer.connect();
        logger.info("connected to kafka");

        logger.info({ topic: KAFKA_TOPIC }, "subscribing to topic");
        await this.consumer.subscribe({
          topic: KAFKA_TOPIC,
          fromBeginning: true,
        });

        this.running = true;

        logger.info("starting consumer...");
        await this.consumer.run({
          eachMessage: async (payload: any) => {
            await this.handleMessage(payload);
          },
        });

        logger.info("clickhouse-consumer running");
        span.end();
      } catch (error) {
        logger.error({ error }, "failed to start consumer");
        span.recordException(error as Error);
        span.end();
        throw error;
      }
    });
  }

  private async handleMessage({ message, partition, topic }: any) {
    return tracer.startActiveSpan("handle_message", async (span) => {
      try {
        span.setAttribute("kafka.topic", topic);
        span.setAttribute("kafka.partition", partition);
        span.setAttribute("kafka.offset", message.offset);

        const value = message.value?.toString();
        if (!value) {
          logger.warn("received empty message");
          span.end();
          return;
        }

        const event: TapEvent = JSON.parse(value);
        span.setAttribute("event.type", event.type);
        span.setAttribute("event.at_uri", event.atUri);

        await this.insertEvent(event);

        logger.info(
          {
            type: event.type,
            at_uri: event.atUri,
            collection: event.collection,
            is_backfill: event.isBackfill,
          },
          "stored event",
        );

        span.end();
      } catch (error) {
        logger.error({ error }, "failed to process message");
        span.recordException(error as Error);
        span.end();
        // don't throw - let kafka consumer continue
      }
    });
  }

  private async insertEvent(event: TapEvent) {
    return tracer.startActiveSpan("insert_to_clickhouse", async (span) => {
      try {
        span.setAttribute("event.type", event.type);
        span.setAttribute("event.at_uri", event.atUri);

        const row = {
          at_uri: event.atUri,
          ingested_at: event.timestamp,
          created_at: event.createdAt ? new Date(event.createdAt).getTime() : null,
          did: event.did,
          collection: event.collection || "",
          rkey: event.rkey || "",
          event_type: event.type,
          action: event.action || "",
          is_backfill: event.isBackfill ? 1 : 0,
          record_data: event.recordData || {},
        };

        const startTime = performance.now();
        await this.clickhouse.insert({
          table: "stream_place_events",
          values: [row],
          format: "JSONEachRow",
        });
        const duration = performance.now() - startTime;

        span.setAttribute("insert.duration_ms", duration);
        span.end();
      } catch (error) {
        logger.error({ event, error }, "failed to insert to clickhouse");
        span.recordException(error as Error);
        span.end();
        throw error;
      }
    });
  }

  async stop() {
    logger.info("shutting down...");
    this.running = false;
    await this.consumer.disconnect();
    await this.clickhouse.close();
    logger.info("shutdown complete");
  }
}

const consumer = new ClickHouseConsumer();

process.on("SIGINT", async () => {
  await consumer.stop();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await consumer.stop();
  process.exit(0);
});

await consumer.start();
