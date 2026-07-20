# AI knowledge base (RAG corpus)

Editable docs live at the gem repo root under `ai/`:

- `ai/README.md` — deep architecture / config / atomicity reference (includes §46 Health alerts)
- `ai/FAQ.md` — curated Q&A (includes section **AS. Health alerts** for `/alerts` configuration)

Boot-time pods **do not** parse those markdown files. They load the packaged
artifact `knowledge_chunks.json` (next to this README) into Redis.

## When Redis is updated

| What | When |
|------|------|
| Knowledge chunks | Only if packaged `corpus_version` ≠ Redis `meta.corpus_version` |
| Live config snapshot (`config` + `config:live` chunk) | At most every **24 hours** on pod boot (`CONFIG_REFRESH_SECONDS`): knobs, broker `topic_inventory`, and `routing` (handler manifest + priority YAML) |

Many UI pods call `sync!`; a short NX lock ensures only one writer runs.

Disable: `config.ai_knowledge_enabled = false` (or `KAFKA_BATCH_AI_KNOWLEDGE_ENABLED=false`).

## Rebuild knowledge + chunks (before a gem release)

From the **kafka-batch gem repository root** (not an app that depends on the gem):

1. Edit `ai/README.md` and/or `ai/FAQ.md`.
2. Rebuild the packaged chunks (updates `corpus_version` automatically from content hashes):

```bash
bin/build_ai_chunks
# or: bundle exec rake kafka_batch:build_ai_chunks
```

3. Commit both the markdown and `lib/kafka_batch/ai/knowledge_chunks.json`.
4. Ship a new gem version. On deploy, the first pod that boots with the new
   `corpus_version` rewrites Redis chunks + config; other pods skip.

### Force-push into Redis (ops / staging)

```bash
# Clear meta so the next sync treats corpus as missing, then sync:
FORCE=1 bundle exec rake kafka_batch:sync_ai_knowledge
```

Or from a console after configure:

```ruby
KafkaBatch::Ai::KnowledgeIndex.sync!
```

## Checklist for a knowledge-base-only release

- [ ] Docs updated under `ai/`
- [ ] `bin/build_ai_chunks` run; `knowledge_chunks.json` committed
- [ ] Specs green (`bundle exec rspec spec/ai_knowledge_index_spec.rb`)
- [ ] Gem version bumped and released
- [ ] After deploy, confirm Redis meta `corpus_version` matches the new package
      (`KafkaBatch::Ai::KnowledgeIndex.meta`)

## Chat + OpenRouter settings

| Redis key | Purpose |
|-----------|---------|
| `kafka_batch:ai:settings` | Encrypted OpenRouter API key + model / base_url |
| `kafka_batch:ai:chat:history` | Global shared admin chat (LIST, newest first; trimmed) |

- Configure the key from the dashboard **AI Settings** page (`/ai`).
- History is **one global thread** for all admins/pods (not per-user). Cap with
  `config.ai_chat_history_max_lines` (default **500**).
- Chat flow: retrieve top knowledge chunks → optional LiveData prefetch/tools →
  OpenRouter → append user + assistant to history.
- LiveData (`config.ai_live_data_enabled`, default true): named O(1) **read-only**
  Redis lookups (batch HMGET, ZCARD/HLEN/LLEN/SISMEMBER/GET, …), **prefetched**
  server-side into the prompt. Never KEYS/SCAN, never writes.
  OpenRouter tool-calling is **off by default** (`ai_live_data_model_tools`) —
  many providers return HTTP 400 for tool schemas.

API (CSRF-protected mutations):

- `GET/PUT/DELETE /api/ai/settings`
- `GET/DELETE /api/ai/history`
- `POST /api/ai/chat` `{ "message": "...", "context": { "batch_id": "…" } }`

## Safety

- RAG / settings / history use only `kafka_batch:ai:*` keys.
- Operational reads go **only** through `Ai::LiveData` allowlisted tools (no raw
  Redis from the model). Tools never mutate Redis.
