import { SimpleIndexer, Tap } from "@atproto/tap";
import { createClient } from "@clickhouse/client";
import { initTelemetry } from "@sp-stats/telemetry";

const { logger, tracer } = initTelemetry("stream-processor");

const TAP_URL = process.env.TAP_URL || "http://localhost:2480";
const TAP_ADMIN_PASSWORD = process.env.TAP_ADMIN_PASSWORD;
const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL || "http://localhost:8123";
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || "sp_stats";
const RELAY_URL =
  process.env.RELAY_URL || "https://relay1.us-east.bsky.network";
const DID_SCRAPE_INTERVAL_MS = parseInt(
  process.env.DID_SCRAPE_INTERVAL_MS || `${20 * 60 * 1000}`,
  10,
);

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

class StreamProcessor {
  private tap: Tap;
  private indexer: SimpleIndexer;
  private channel: any;
  private clickhouse: ReturnType<typeof createClient>;
  private didCache: Set<string> = new Set();
  private scrapeInterval: NodeJS.Timeout | null = null;

  constructor() {
    this.tap = new Tap(TAP_URL, { adminPassword: TAP_ADMIN_PASSWORD });
    this.indexer = new SimpleIndexer();

    this.clickhouse = createClient({
      url: CLICKHOUSE_URL,
      database: CLICKHOUSE_DATABASE,
    });

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
      setTimeout(() => this.restartChannel(), 5000);
    }
  }

  private setupIndexer() {
    this.indexer.identity(async (evt) => {
      // no-op - we only care about records
    });

    this.indexer.record(async (evt) => {
      await tracer.startActiveSpan("process_record", async (span) => {
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

      if (err.message?.includes("RSV") || err.message?.includes("WebSocket")) {
        await this.restartChannel();
      }
    });
  }

  async start() {
    return tracer.startActiveSpan("stream-processor.start", async (span) => {
      try {
        logger.info("discovering DIDs from relay...");
        await this.scrapeDids();
        logger.info(
          { did_count: this.didCache.size },
          "initial scrape complete",
        );
        span.setAttribute("initial_dids.count", this.didCache.size);

        this.channel = this.tap.channel(this.indexer);
        this.channel.start();
        logger.info("tap channel started");

        logger.info(
          { interval_ms: DID_SCRAPE_INTERVAL_MS },
          "starting DID scrape job",
        );
        this.scrapeInterval = setInterval(() => {
          this.scrapeDids().catch((error) => {
            logger.error({ error }, "DID scrape job failed");
          });
        }, DID_SCRAPE_INTERVAL_MS);

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

  private async scrapeDids() {
    return tracer.startActiveSpan("scrape_dids", async (span) => {
      const previousCount = this.didCache.size;

      for (const collection of STREAM_PLACE_COLLECTIONS) {
        logger.info({ collection }, "fetching DIDs");
        try {
          const dids = await this.fetchDidsForCollection(collection);
          const newDids: string[] = [];

          for (const did of dids) {
            if (!this.didCache.has(did)) {
              this.didCache.add(did);
              newDids.push(did);
            }
          }

          logger.info(
            { collection, total: dids.length, new: newDids.length },
            "fetched DIDs",
          );

          if (newDids.length > 0) {
            logger.info({ count: newDids.length }, "adding new repos to tap");
            await this.tap.addRepos(newDids);
          }
        } catch (error) {
          logger.error({ collection, error }, "failed to fetch DIDs");
          span.recordException(error as Error);
        }
      }

      const addedCount = this.didCache.size - previousCount;
      span.setAttribute("total_dids", this.didCache.size);
      span.setAttribute("new_dids", addedCount);
      logger.info(
        {
          previous: previousCount,
          current: this.didCache.size,
          added: addedCount,
        },
        "scrape complete",
      );

      span.end();
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

        logger.debug(
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
    logger.info("shutting down...");

    if (this.scrapeInterval) {
      clearInterval(this.scrapeInterval);
      this.scrapeInterval = null;
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

process.on("unhandledRejection", (error) => {
  logger.error({ error }, "unhandled rejection");
});

await processor.start();
