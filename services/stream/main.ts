import { SimpleIndexer, Tap, TapChannel } from "@atproto/tap";
import { createClient } from "@clickhouse/client";
import { initTelemetry } from "@sp-stats/telemetry";

const { logger, tracer } = initTelemetry("stream-processor");

const TAP_URL = process.env.TAP_URL || "http://localhost:2480";
const TAP_ADMIN_PASSWORD = process.env.TAP_ADMIN_PASSWORD;
const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL || "http://localhost:8123";
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || "sp_stats";
const CONCURRENCY = parseInt(process.env.CONCURRENCY || "5", 10);

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

class StreamProcessor {
  private tap: Tap;
  private indexer: SimpleIndexer;
  private channel: TapChannel | null = null;
  private clickhouse: ReturnType<typeof createClient>;
  private lastEventTime: number = Date.now();
  private healthCheckInterval?: NodeJS.Timeout;
  private isShuttingDown: boolean = false;
  private eventQueue: Array<{ evt: any; resolve: () => void }> = [];
  private activeWorkers: number = 0;

  constructor() {
    this.tap = new Tap(TAP_URL, { adminPassword: TAP_ADMIN_PASSWORD });
    this.indexer = new SimpleIndexer();

    this.clickhouse = createClient({
      url: CLICKHOUSE_URL,
      database: CLICKHOUSE_DATABASE,
    });

    this.setupIndexer();
  }

  async ensureSchema() {
    return tracer.startActiveSpan("ensure_schema", async (span) => {
      try {
        logger.info("ensuring database schema exists");

        await this.clickhouse.exec({
          query: "CREATE DATABASE IF NOT EXISTS sp_stats",
        });

        await this.clickhouse.exec({
          query: `
            CREATE TABLE IF NOT EXISTS stream_place_events (
              at_uri String,
              ingested_at DateTime64(3),
              created_at Nullable(DateTime64(3)),
              did String,
              collection String,
              rkey String,
              event_type String,
              action String,
              is_backfill UInt8,
              record_data JSON
            ) ENGINE = MergeTree()
            ORDER BY (collection, ingested_at, did)
            PARTITION BY toYYYYMM(ingested_at)
          `,
        });

        await this.clickhouse.exec({
          query: `
            CREATE INDEX IF NOT EXISTS idx_did
            ON stream_place_events (did)
            TYPE bloom_filter GRANULARITY 1
          `,
        });

        await this.clickhouse.exec({
          query: `
            CREATE INDEX IF NOT EXISTS idx_at_uri
            ON stream_place_events (at_uri)
            TYPE bloom_filter GRANULARITY 1
          `,
        });

        logger.info("schema ready");
        span.end();
      } catch (error) {
        logger.error({ error }, "failed to ensure schema");
        span.recordException(error as Error);
        span.end();
        throw error;
      }
    });
  }

  async restartChannel() {
    if (this.isShuttingDown) {
      return;
    }

    logger.warn("restarting tap connection...");
    try {
      if (this.channel) {
        logger.info("destroying existing channel");
        // don't wait for destroy - just fire and forget
        this.channel.destroy().catch((err: any) => {
          logger.warn({ error: err }, "error destroying channel (ignoring)");
        });
        this.channel = null;
        logger.info("existing channel destroyed");
      }

      logger.info("creating new tap client");
      this.tap = new Tap(TAP_URL, { adminPassword: TAP_ADMIN_PASSWORD });

      logger.info("creating new channel");
      this.channel = this.tap.channel(this.indexer);

      logger.info("starting channel");
      this.channel.start();

      this.lastEventTime = Date.now();
      logger.info("tap connection restarted successfully");
    } catch (error) {
      logger.error(
        {
          error,
          message: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : undefined,
        },
        "failed to restart channel - retrying in 5s",
      );
      setTimeout(() => this.restartChannel(), 5000);
    }
  }

  private startHealthCheck() {
    this.healthCheckInterval = setInterval(() => {
      const timeSinceLastEvent = Date.now() - this.lastEventTime;
      const maxIdleTime = 600; // 6 seconds

      if (timeSinceLastEvent > maxIdleTime) {
        logger.warn(
          { timeSinceLastEvent },
          "no events received recently - restarting channel",
        );
        this.restartChannel();
      }
    }, 3000); // check every 3 seconds
  }

  private async processEvent(evt: any) {
    return tracer.startActiveSpan("process_record", async (span) => {
      try {
        if (!evt.collection?.startsWith("place.stream.")) {
          span.end();
          return;
        }

        span.setAttribute("event.did", evt.did);
        span.setAttribute("event.collection", evt.collection);
        span.setAttribute("event.action", evt.action);

        const tapEvent: TapEvent = {
          type: "record",
          did: evt.did,
          atUri: `at://${evt.did}/${evt.collection}/${evt.rkey}`,
          timestamp: Date.now(),
          createdAt: (evt.record as any)?.createdAt,
          action: evt.action,
          isBackfill: false,
          collection: evt.collection,
          rkey: evt.rkey,
          recordData: evt.record,
        };

        await this.insertEvent(tapEvent);
        this.lastEventTime = Date.now();
        span.end();
      } catch (error) {
        logger.error({ event: evt, error }, "failed to process record event");
        span.recordException(error as Error);
        span.end();
      }
    });
  }

  private async worker() {
    while (!this.isShuttingDown) {
      if (this.eventQueue.length === 0) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        continue;
      }

      const item = this.eventQueue.shift();
      if (!item) continue;

      this.activeWorkers++;
      await this.processEvent(item.evt);
      this.activeWorkers--;
      item.resolve();
    }
  }

  private setupIndexer() {
    this.indexer.identity(async (evt) => {
      // no-op - we only care about records
    });

    this.indexer.record(async (evt) => {
      return new Promise<void>((resolve) => {
        this.eventQueue.push({ evt, resolve });
      });
    });

    this.indexer.error(async (err) => {
      logger.error({ error: err }, "indexer error - restarting channel");
      await this.restartChannel();
    });
  }

  async start() {
    return tracer.startActiveSpan("stream-processor.start", async (span) => {
      try {
        await this.ensureSchema();

        // start worker pool
        for (let i = 0; i < CONCURRENCY; i++) {
          this.worker();
        }
        logger.info({ workers: CONCURRENCY }, "worker pool started");

        this.channel = this.tap.channel(this.indexer);
        this.channel.start();
        this.lastEventTime = Date.now();
        logger.info("tap channel started");

        this.startHealthCheck();
        logger.info("health check started");

        logger.info("stream processor running");
        span.end();
      } catch (error) {
        logger.error({ error }, "failed to start");
        span.recordException(error as Error);
        span.end();
        throw error;
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
          created_at: event.createdAt
            ? new Date(event.createdAt).getTime()
            : null,
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

        logger.info(
          {
            type: event.type,
            at_uri: event.atUri,
            collection: event.collection,
            is_backfill: event.isBackfill,
            duration_ms: duration.toFixed(0),
          },
          "stored event",
        );

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
    this.isShuttingDown = true;
    logger.info("shutting down...");

    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }

    if (this.channel) {
      await this.channel.destroy();
    }

    await this.clickhouse.close();

    logger.info("shutdown complete");
  }
}

const processor = new StreamProcessor();

process.on("SIGINT", async () => {
  await processor.stop();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await processor.stop();
  process.exit(0);
});

process.on("unhandledRejection", async (error) => {
  logger.error(
    {
      error,
      errorString: String(error),
      errorStack: error instanceof Error ? error.stack : undefined,
      errorType: error?.constructor?.name,
    },
    "unhandled rejection - exiting",
  );
  await processor.stop();
  process.exit(1);
});

process.on("uncaughtException", async (error) => {
  logger.error(
    {
      error,
      message: error?.message,
      stack: error?.stack,
      type: error?.constructor?.name,
    },
    "uncaught exception - exiting",
  );
  await processor.stop();
  process.exit(1);
});

await processor.start().catch(async (error) => {
  logger.error(
    {
      error,
      message: error?.message,
      stack: error?.stack,
      type: error?.constructor?.name,
    },
    "startup failed - exiting",
  );
  await processor.stop();
  process.exit(1);
});
