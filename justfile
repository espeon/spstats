# start all infrastructure
up:
    docker compose up -d

# stop all infrastructure
down:
    docker compose down

# start observability stack (tempo + victoria + grafana)
obs-up:
    docker compose -f docker-compose.observability.yml up -d

# stop observability stack
obs-down:
    docker compose -f docker-compose.observability.yml down

# view observability logs
obs-logs service="":
    #!/usr/bin/env bash
    if [ -z "{{service}}" ]; then
        docker compose -f docker-compose.observability.yml logs -f
    else
        docker compose -f docker-compose.observability.yml logs -f {{service}}
    fi

# view logs
logs service="":
    #!/usr/bin/env bash
    if [ -z "{{service}}" ]; then
        docker compose logs -f
    else
        docker compose logs -f {{service}}
    fi

# start tap-publisher
tap:
    pnpm --filter tap-publisher dev

# start clickhouse-consumer
consumer:
    pnpm --filter clickhouse-consumer dev

# add repos to track (usage: just add-repos did:plc:xyz did:plc:abc)
add-repos +dids:
    curl -X POST http://localhost:2480/repos \
        -u "admin:admin" \
        -H "Content-Type: application/json" \
        -d '{"dids": [{{dids}}]}'

# connect to clickhouse client
ch:
    docker exec -it sp-stats-clickhouse-1 clickhouse-client --database sp_stats

# query recent events
recent limit="10":
    docker exec -it sp-stats-clickhouse-1 clickhouse-client --database sp_stats \
        --query "SELECT * FROM stream_place_events ORDER BY ingested_at DESC LIMIT {{limit}}"

# query events by collection
by-collection:
    docker exec -it sp-stats-clickhouse-1 clickhouse-client --database sp_stats \
        --query "SELECT collection, count() as count FROM stream_place_events GROUP BY collection"

# query events by did
by-did:
    docker exec -it sp-stats-clickhouse-1 clickhouse-client --database sp_stats \
        --query "SELECT did, count() as count FROM stream_place_events GROUP BY did ORDER BY count DESC"

# clean all data
clean:
    docker compose down -v
    docker compose -f docker-compose.observability.yml down -v
    rm -rf clickhouse_data redpanda_data

# reset clickhouse schema
reset-schema:
    docker exec sp-stats-clickhouse-1 clickhouse-client --database=sp_stats --query "DROP TABLE IF EXISTS stream_place_events"


nuke:
    just clean && just up && sleep 5 && just reset-schema

# dev: start infra + both services (requires overmind or similar)
dev:
    @echo "start infrastructure with: just up"
    @echo "then run services with: just tap (in one terminal) and just consumer (in another)"
