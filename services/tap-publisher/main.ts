import { SimpleIndexer, Tap } from "@atproto/tap";
import { Kafka } from "kafkajs";
import { initTelemetry } from "@sp-stats/telemetry";

const { logger, tracer } = initTelemetry("tap-publisher");

const TAP_URL = process.env.TAP_URL || "http://localhost:2480";
const TAP_ADMIN_PASSWORD = process.env.TAP_ADMIN_PASSWORD;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:19092").split(
  ",",
);
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "stream-place-events";
const RELAY_URL =
  process.env.RELAY_URL || "https://relay1.us-east.bsky.network";

const STREAM_PLACE_COLLECTIONS = [
  "place.stream.chat.profile",
  "place.stream.chat.message",
  "place.stream.livestream",
];

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

class TapPublisher {
  private tap: Tap;
  private indexer: SimpleIndexer;
  private kafka: Kafka;
  private producer: any;
  private channel: any;

  constructor() {
    this.tap = new Tap(TAP_URL, { adminPassword: TAP_ADMIN_PASSWORD });
    this.indexer = new SimpleIndexer();

    this.kafka = new Kafka({
      clientId: "tap-publisher",
      brokers: KAFKA_BROKERS,
    });

    this.producer = this.kafka.producer();
    this.setupIndexer();
  }

  async restartChannel() {
    logger.warn("restarting tap channel after error...");
    try {
      if (this.channel) {
        await this.channel.destroy().catch(() => {});
      }
      this.channel = this.tap.channel(this.indexer);
      this.channel.start();
      logger.info("tap channel restarted");
    } catch (error) {
      logger.error({ error }, "failed to restart channel");
      // retry after delay
      setTimeout(() => this.restartChannel(), 5000);
    }
  }

  private setupIndexer() {
    // skip identity events - we only care about records
    this.indexer.identity(async (evt) => {
      // no-op
    });

    this.indexer.record(async (evt) => {
      await tracer.startActiveSpan("process_record", async (span) => {
        try {
          // filter for stream.place.* records
          if (!evt.collection?.startsWith("stream.place.")) {
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

          await this.publishEvent(tapEvent);
          span.end();
        } catch (error) {
          logger.error({ event: evt, error }, "failed to process record event");
          span.recordException(error as Error);
          span.end();
          throw error;
        }
      });
    });

    this.indexer.error(async (err) => {
      logger.error({ error: err }, "indexer error");

      // if it's a websocket error, try to restart the channel
      if (err.message?.includes("RSV") || err.message?.includes("WebSocket")) {
        await this.restartChannel();
      }
    });
  }

  async start() {
    return tracer.startActiveSpan("tap-publisher.start", async (span) => {
      try {
        logger.info("connecting to kafka...");
        await this.producer.connect();
        logger.info("connected to kafka");

        logger.info("discovering DIDs from relay...");
        const dids = await this.discoverDids();
        logger.info({ did_count: dids.size }, "discovery complete");
        span.setAttribute("dids.count", dids.size);

        this.channel = this.tap.channel(this.indexer);
        this.channel.start();
        logger.info("tap channel started");

        if (dids.size > 0) {
          logger.info("adding repos to tap...");
          await this.tap.addRepos(Array.from(dids));
          logger.info("repos added to tap");
        } else {
          logger.warn("no DIDs found, nothing to track");
        }

        span.end();
      } catch (error) {
        logger.error({ error }, "failed to start");
        span.recordException(error as Error);
        span.end();
        throw error;
      }
    });
  }

  private async discoverDids(): Promise<Set<string>> {
    return tracer.startActiveSpan("discover_dids", async (span) => {
      const allDids = new Set<string>();

      for (const collection of STREAM_PLACE_COLLECTIONS) {
        logger.info({ collection }, "fetching DIDs");
        try {
          const dids = await this.fetchDidsForCollection(collection);
          dids.forEach((did) => allDids.add(did));
          logger.info({ collection, count: dids.length }, "fetched DIDs");
        } catch (error) {
          logger.error({ collection, error }, "failed to fetch DIDs");
          span.recordException(error as Error);
        }
      }

      span.setAttribute("total_dids", allDids.size);
      span.end();
      return allDids;
    });
  }

  private async fetchDidsForCollection(collection: string): Promise<string[]> {
    return tracer.startActiveSpan("fetch_dids_for_collection", async (span) => {
      span.setAttribute("collection", collection);

      const dids: string[] = [];
      let cursor: string | undefined;
      let pageCount = 0;

      try {
        do {
          const url = new URL(
            `${RELAY_URL}/xrpc/com.atproto.sync.listReposByCollection`,
          );
          url.searchParams.set("collection", collection);
          if (cursor) {
            url.searchParams.set("cursor", cursor);
          }

          const startTime = performance.now();
          const response = await fetch(url.toString());
          const duration = performance.now() - startTime;

          if (!response.ok) {
            const errorText = await response.text();
            logger.error(
              {
                collection,
                status: response.status,
                error: errorText,
              },
              "relay request failed",
            );
            throw new Error(`HTTP ${response.status}: ${errorText}`);
          }

          const data = await response.json();
          pageCount++;

          if (data.repos) {
            dids.push(...data.repos.map((repo: any) => repo.did));
            logger.debug(
              {
                collection,
                page: pageCount,
                count: data.repos.length,
                duration_ms: duration.toFixed(0),
              },
              "fetched page",
            );
          }

          cursor = data.cursor;
        } while (cursor);

        span.setAttribute("pages", pageCount);
        span.setAttribute("dids", dids.length);
        span.end();
        return dids;
      } catch (error) {
        span.recordException(error as Error);
        span.end();
        throw error;
      }
    });
  }

  private async publishEvent(event: TapEvent) {
    return tracer.startActiveSpan("publish_to_kafka", async (span) => {
      try {
        span.setAttribute("event.type", event.type);
        span.setAttribute("event.at_uri", event.atUri);
        span.setAttribute("event.is_backfill", event.isBackfill);

        await this.producer.send({
          topic: KAFKA_TOPIC,
          messages: [
            {
              key: event.did,
              value: JSON.stringify(event),
            },
          ],
        });

        logger.info(
          {
            type: event.type,
            at_uri: event.atUri,
            collection: event.collection,
            action: event.action,
            is_backfill: event.isBackfill,
          },
          "published event",
        );

        span.end();
      } catch (error) {
        logger.error({ event, error }, "failed to publish to kafka");
        span.recordException(error as Error);
        span.end();
        throw error;
      }
    });
  }

  async stop() {
    logger.info("shutting down...");
    if (this.channel) {
      await this.channel.destroy();
    }
    await this.producer.disconnect();
    logger.info("shutdown complete");
  }
}

const publisher = new TapPublisher();

process.on("SIGINT", async () => {
  await publisher.stop();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await publisher.stop();
  process.exit(0);
});

// catch unhandled websocket errors and restart
process.on("unhandledRejection", (error) => {
  logger.error({ error }, "unhandled rejection");
});

await publisher.start();
