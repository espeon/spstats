# stream.place stats

tracks ingestion of `stream.place.*` records from the AT Protocol network.

## quick start

prerequisites:
- node.js 22+
- pnpm 9+
- docker

using just:

```bash
# install dependencies
pnpm install

# start infrastructure
just up

# optional: start observability stack (tempo + victoria + grafana)
just obs-up

# in separate terminals:
just tap
just consumer

# view grafana at http://localhost:3000
```
