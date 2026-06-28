# kafka-batch

[![CI](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/y-shashank/kafka-batch/badges/coverage.json)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)

Drop-in replacement for **Sidekiq Pro Batches** using Apache Kafka as the transport layer. Provides the same `on_success` / `on_complete` callback semantics, per-job retry with backoff, and idempotent completion tracking — at a fraction of the cost.

Built on the [Karafka](https://karafka.io) ecosystem: **WaterDrop** for producing, **Karafka consumers** for processing.

---

## Table of Contents

- [How it works](#how-it-works)
- [Installation](#installation)
- [Configuration](#configuration)
  - [MySQL store](#mysql-store)
  - [Redis store](#redis-store)
  - [Karafka routing](#karafka-routing)
- [Completion counting & scalability](#completion-counting--scalability)
- [Defining workers](#defining-workers)
- [Creating batches](#creating-batches)
  - [Standalone jobs (no batch)](#standalone-jobs-no-batch)
  - [Batch.find and Batch.cancel](#batchfind-and-batchcancel)
- [Callbacks](#callbacks)
- [Retry behaviour](#retry-behaviour)
  - [Early batch completion (`complete_after_retries`)](#early-batch-completion-complete_after_retries)
- [Dead Letter Topic](#dead-letter-topic)
- [Multi-tenant fairness (WFQ)](#multi-tenant-fairness-wfq)
- [Web UI](#web-ui)
- [Reconciler](#reconciler)
- [Instrumentation](#instrumentation)
- [Rake tasks](#rake-tasks)
- [Reliability guarantees](#reliability-guarantees)
- [Known limitations](#known-limitations)
- [Migrating from Sidekiq Pro Batches](#migrating-from-sidekiq-pro-batches)
- [Architecture deep-dive](#architecture-deep-dive)
- [Topic reference](#topic-reference)
- [Contributing](#contributing)

---

## How it works

```
┌──────────────────────────────────────────────────────────────────┐
│                        Your application                           │
│                                                                   │
│  Batch.create do |b|                                              │
│    b.push(MyWorker, { id: 1 })  ──┐                              │
│    b.push(MyWorker, { id: 2 })  ──┤──► Kafka: worker topic       │
│    b.push(MyWorker, { id: 3 })  ──┘   (idempotent producer)      │
│                                                                   │
│  BatchRecord written to MySQL/Redis BEFORE first produce          │
└──────────────────────────────────────────────────────────────────┘
                │ (jobs topic)
   ┌────────────▼─────────────┐
   │    Karafka: JobConsumer   │
   │                          │
   │  worker.perform(payload)  │
   │    ├─ success ───────────┼──► kafka_batch.events
   │    │                     │    event carries source coords
   │    │                     │    {src_topic, src_partition,
   │    │                     │     src_offset}; keyed by
   │    │                     │     src_topic/src_partition
   │    └─ failure            │
   │        ├─ retriable ─────┼──► kafka_batch.jobs.retry
   │        └─ exhausted ─────┼──► kafka_batch.dead_letter
   └──────────────────────────┘         +events (failed)
                │ (events topic)
   ┌────────────▼─────────────┐
   │  Karafka: EventConsumer   │
   │                          │
   │  store.record_completion_ │
   │  by_offset(...)           │   dedup: apply only if
   │   monotonic per-partition │   src_offset > stored cursor
   │   cursor  →  O(partitions)│   (absorbs redelivered AND
   │    ├─ running ──► skip   │    re-produced events)
   │    ├─ duplicate ► skip   │
   │    └─ done ──────────────┼──► kafka_batch.callbacks
   └──────────────────────────┘
                │ (callbacks topic)
   ┌────────────▼─────────────┐
   │ Karafka: CallbackConsumer │   at-least-once
   │                          │   (callbacks idempotent)
   │  callback_dispatched? ───┼── yes ─► skip (duplicate)
   │    │ no                   │
   │    ▼                      │
   │  on_success(batch)        │   invoke FIRST,
   │  on_complete(batch)       │   then claim_callback()
   │  claim_callback()  ───────┼── mark dispatched (CAS)
   └──────────────────────────┘
                                  (crash before claim ⇒
                                   re-invoke on redelivery,
                                   never a lost callback)

   ┌──────────────────────────┐
   │  Karafka: RetryConsumer   │  ◄── kafka_batch.jobs.retry
   │                          │
   │  retry_after in future?  │
   │    ├─ yes ──► pause()    │  (Karafka partition pause –
   │    │         then retry   │   zero thread blocking)
   │    └─ no  ──► produce    │
   │              to retry_to  │──► original worker topic
   └──────────────────────────┘
```

> **Multi-tenant fairness mode** (opt-in) inserts a stage before `JobConsumer`: `Batch.push` writes to a per-tenant **ingest** topic, a `Fairness::Dispatcher` fairly forwards onto a throttled **ready** topic, and the `JobConsumer` swarm drains *that*. Everything downstream (events/callbacks/retry/DLT) is identical. See [Multi-tenant fairness](#multi-tenant-fairness-wfq).

---

## Installation

Add to your `Gemfile`:

```ruby
gem "kafka-batch"
```

Run the installer:

```bash
bundle exec rails generate kafka_batch:install
# or with Redis store:
bundle exec rails generate kafka_batch:install --store redis
```

This creates:
- `config/initializers/kafka_batch.rb`
- Database migrations (MySQL store only)

Run migrations if using the MySQL store:

```bash
bundle exec rails db:migrate
```

Create the required Kafka topics (adjust partitions to your throughput):

```bash
kafka-topics.sh --create --topic kafka_batch.jobs       --partitions 6
kafka-topics.sh --create --topic kafka_batch.events     --partitions 3
kafka-topics.sh --create --topic kafka_batch.callbacks  --partitions 1
kafka-topics.sh --create --topic kafka_batch.jobs.retry --partitions 3
kafka-topics.sh --create --topic kafka_batch.dead_letter --partitions 1
```

---

## Configuration

Edit `config/initializers/kafka_batch.rb`:

```ruby
KafkaBatch.configure do |config|
  # ── State store ────────────────────────────────────────────────────
  # Where batch counters / completion cursors / failure log live (Kafka always
  # holds the actual jobs). See "Choosing a store" below.
  #   :mysql  – durable on disk, queryable via SQL, needs migrations
  #   :redis  – in-memory, lowest latency, no migrations, TTL-based retention
  config.store = :mysql

  # ── Kafka brokers ───────────────────────────────────────────────────
  config.brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

  # ── Topic names ─────────────────────────────────────────────────────
  config.jobs_topic        = "kafka_batch.jobs"
  config.events_topic      = "kafka_batch.events"
  config.callbacks_topic   = "kafka_batch.callbacks"
  config.retry_topic       = "kafka_batch.jobs.retry"   # dedicated retry topic
  config.dead_letter_topic = "kafka_batch.dead_letter"
  # Multi-tenant fairness topics (only used when config.fairness_enabled = true):
  config.fairness_ingest_topic = "kafka_batch.ingest"   # per-tenant intake (durable backlog)
  config.fairness_ready_topic  = "kafka_batch.ready"    # throttled execution queue

  # ── Consumer group ──────────────────────────────────────────────────
  config.consumer_group = "kafka-batch"

  # ── Cancellation ────────────────────────────────────────────────────
  # When true, JobConsumer skips jobs whose batch was cancelled. The set of
  # cancelled batch ids is cached per process and refreshed at most once per
  # cancellation_cache_ttl seconds (no per-job store read), so cancellation
  # takes effect within that window.
  config.skip_cancelled_jobs    = true
  config.cancellation_cache_ttl = 120  # seconds

  # ── Retry behaviour (global defaults; override per Worker class) ────
  # Fixed, short retry schedule (Kafka-friendly): 1st retry after
  # retry_first_delay, later retries after retry_delay, with +/- retry_jitter.
  config.max_retries       = 3   # attempts before dead letter
  config.retry_first_delay = 10  # seconds before the 1st retry
  config.retry_delay       = 180 # seconds before each later retry (3 min)
  config.retry_jitter      = 0.1 # +/- 10% randomization
  # After this many retries a still-failing job counts toward on_complete while
  # it keeps retrying in the background up to max_retries (per-Worker override).
  # Default == max_retries default, so default behaviour is unchanged.
  config.complete_after_retries = 3

  # ── Completion-event emission retries (inline; blocks the worker thread) ─
  config.event_emit_retries = 3
  config.event_emit_backoff = 2  # seconds; sleep = attempt × backoff

  # ── Redis (used by the :redis store AND the :redis liveness backend) ─
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.redis_pool_size = 5
  config.batch_ttl       = 7 * 24 * 3600  # seconds until Redis batch keys expire

  # ── Failure-log retention (Redis store only) ────────────────────────
  # Failure records are a dashboard convenience – the real job data is durable
  # in Kafka – so they get a shorter TTL and a per-batch cap to bound RAM.
  config.failures_ttl           = 24 * 3600  # seconds
  config.max_failures_per_batch = 1000        # 0 = unlimited

  # ── Live-activity backend (/live page; independent of config.store) ──
  #   :redis – per-job tracking in Redis (most detail; writes scale with jobs)
  #   :store – consumer heartbeats in config.store (sampled; writes scale with
  #            #consumers — needs the consumer-heartbeats table on :mysql)
  #   :off   – disable the /live page
  config.liveness_backend            = :redis
  config.liveness_ttl                = 30  # seconds a heartbeat/entry is "live"
  config.liveness_heartbeat_interval = 5   # :store throttle: 1 write/consumer/N s
  config.track_running_jobs          = true # gate :redis per-job running-state writes

  # ── Multi-tenant fairness (Kafka-only, opt-in; see the Fairness section) ─
  config.fairness_enabled        = false
  config.fairness_ready_lag_high = 5000 # dispatcher pauses forwarding above this depth
  config.fairness_ready_lag_low  = 1000 # ...resumes below this depth

  # ── Reconciliation ───────────────────────────────────────────────────
  config.reconciliation_interval = 300  # seconds (re-check stuck "running" batches)
  config.reconciler_lock_ttl     = 600  # seconds; distributed-lock TTL for one sweep

  # ── Topic validation at boot ─────────────────────────────────────────
  # When true, Rails boot raises if any required topics are missing in Kafka.
  # Disable in CI / test environments where Kafka is not running.
  config.validate_topics_on_boot = false

  # ── Advanced rdkafka / WaterDrop config overrides ───────────────────
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024"   }
end
```

### Full config reference

Every option on `KafkaBatch.config`:

| Option | Type | Default | Description |
|---|---|---|---|
| `store` | Symbol | `:mysql` | State store for counters/cursors/failures: `:mysql` or `:redis` |
| `brokers` | Array&lt;String&gt; | `["localhost:9092"]` | Kafka bootstrap brokers |
| `consumer_group` | String | `"kafka-batch"` | Base consumer-group name (suffixed `-control` / `-jobs` / `-dispatch`) |
| `logger` | Logger | `Rails.logger` | Logger instance |
| `jobs_topic` | String | `"kafka_batch.jobs"` | Default worker jobs topic (non-fairness) |
| `events_topic` | String | `"kafka_batch.events"` | Completion-event topic |
| `callbacks_topic` | String | `"kafka_batch.callbacks"` | Batch-callback topic |
| `retry_topic` | String | `"kafka_batch.jobs.retry"` | Retry topic |
| `dead_letter_topic` | String | `"kafka_batch.dead_letter"` | Dead-letter topic |
| `max_retries` | Integer | `3` | Retry attempts before dead-letter (per-Worker override) |
| `retry_first_delay` | Integer (s) | `10` | Delay before the 1st retry |
| `retry_delay` | Integer (s) | `180` | Delay before each later retry |
| `retry_jitter` | Float | `0.1` | ± randomization on retry delays |
| `complete_after_retries` | Integer | `3` | Count a still-failing job toward `on_complete` after N retries (keeps retrying in bg; per-Worker override) |
| `event_emit_retries` | Integer | `3` | Inline retries when producing a completion event |
| `event_emit_backoff` | Integer (s) | `2` | Linear backoff for event-emit retries (`attempt × backoff`) |
| `skip_cancelled_jobs` | Boolean | `true` | Skip jobs whose batch was cancelled |
| `cancellation_cache_ttl` | Integer (s) | `120` | Refresh interval for the per-process cancelled-batch cache |
| `redis_url` | String | `"redis://localhost:6379/0"` | Redis URL (used by `:redis` store and `:redis` liveness) |
| `redis_pool_size` | Integer | `5` | Redis connection-pool size |
| `batch_ttl` | Integer (s) | `604800` (7d) | TTL for Redis batch keys |
| `failures_ttl` | Integer (s) | `86400` (1d) | TTL for the Redis failure log |
| `max_failures_per_batch` | Integer | `1000` | Cap on tracked failing jobs per batch (Redis; `0` = unlimited) |
| `liveness_backend` | Symbol | `:redis` | `/live` source: `:redis`, `:store`, or `:off` |
| `liveness_ttl` | Integer (s) | `30` | How long a heartbeat/entry is considered live |
| `liveness_heartbeat_interval` | Integer (s) | `5` | `:store` heartbeat write throttle |
| `track_running_jobs` | Boolean | `true` | Gate per-job running-state writes (`:redis` liveness) |
| `fairness_enabled` | Boolean | `false` | Enable multi-tenant fairness (Kafka-only dispatcher) |
| `fairness_ingest_topic` | String | `"kafka_batch.ingest"` | Per-tenant intake topic (fairness) |
| `fairness_ready_topic` | String | `"kafka_batch.ready"` | Throttled execution topic (fairness) |
| `fairness_ready_lag_high` | Integer | `5000` | Dispatcher pauses forwarding above this ready-topic depth |
| `fairness_ready_lag_low` | Integer | `1000` | Dispatcher resumes forwarding below this depth |
| `fairness_global_concurrency` | Integer | `50` | **Optional `Scheduler` only** — total in-flight slots |
| `fairness_max_inflight_per_tenant` | Integer | `0` | **Optional `Scheduler` only** — per-tenant cap (`0` = none) |
| `fairness_ready_window` | Integer | `500` | **Optional `Scheduler` only** — bounded ready jobs/tenant in Redis |
| `fairness_default_weight` | Float | `1.0` | **Optional `Scheduler` only** — default tenant weight |
| `reconciliation_interval` | Integer (s) | `300` | Age after which a "running" batch is re-checked by the reconciler |
| `reconciler_lock_ttl` | Integer (s) | `600` | Distributed-lock TTL for one reconciler sweep |
| `producer_config` | Hash | `{}` | Raw rdkafka/WaterDrop producer overrides |
| `consumer_config` | Hash | `{}` | Raw rdkafka consumer overrides (merged into every consumer) |
| `validate_topics_on_boot` | Boolean | `false` | Raise at boot if required topics are missing |

**Per-Worker overrides** (on the worker class, not `config`): `kafka_topic` (required), `max_retries`, `complete_after_retries`.

### Choosing a store

KafkaBatch makes **two independent storage choices**. Kafka is always the source of truth for the actual jobs and completion events; these only hold derived/aggregate state.

#### 1. State store — `config.store` (`:mysql` | `:redis`)

Holds batch counters, the per-partition completion cursors (exactly-once dedup), and the failure log. Both options implement the **same guarantees** (exactly-once counting, callbacks, reconciler, open batches) — pick based on operational fit.

| | `:mysql` | `:redis` |
|---|---|---|
| Durability | On disk; survives restarts | In-memory (lost on flush unless Redis persistence is configured) |
| Setup | Run migrations (tables below) | None; keys auto-expire |
| Retention | Manual / `delete_batch` | TTL — `batch_ttl` (batches), `failures_ttl` (failures) |
| Queryable | Yes, via SQL | Key lookups only |
| Hot-batch counter writes | One row lock per batch (kept cheap by per-poll **batched counting**) | Atomic Lua, microsecond-fast — no row contention |
| Best for | Auditability, durability, an existing RDBMS | Lowest latency, no schema, very high single-batch throughput |

#### 2. Live-activity backend — `config.liveness_backend` (`:redis` | `:store` | `:off`)

Powers **only** the `/live` dashboard page, and is **independent** of `config.store` (e.g. you can run `store = :mysql` with `liveness_backend = :redis`).

| | `:redis` | `:store` | `:off` |
|---|---|---|---|
| Source | Per-job keys in Redis (`redis_url`) | Consumer heartbeats in `config.store` | — |
| Detail | Every running job (most detailed) | Sampled "current job" per consumer | none |
| Write volume | Scales with **job throughput** | Scales with **#consumers** (throttled to 1 write / `liveness_heartbeat_interval`) | none |
| Requires | Redis reachable | the heartbeats table (on `:mysql`) | — |
| Resilience | Best-effort behind a circuit breaker | Best-effort; stale rows filtered by `liveness_ttl` | — |

> On `:store`, "running jobs" is **sampled** — very short jobs may not appear between heartbeats, but active consumers always show. Use `:redis` when you want every in-flight job listed.

### MySQL store

Requires these migrations:

| Migration | What it creates |
|---|---|
| `create_kafka_batch_records` | Batch state table with counters and status |
| `add_callback_tracking_to_kafka_batch_records` | `callback_dispatched_at` column for callback dispatch tracking (duplicate suppression + lost-callback reconciliation) |
| `create_kafka_batch_consumer_offsets` | Per-partition monotonic completion cursor (one row per `source_topic, source_partition`) |
| `add_locked_at_to_kafka_batch_records` | `locked_at` column (the batch "sealed" marker that gates completion during block-form population) |
| `add_description_to_kafka_batch_records` | optional `description` column shown in the Web UI |
| `add_callback_dispatched_by_to_kafka_batch_records` | records which consumer pod/process ran the batch's callbacks |
| `create_kafka_batch_failures` | Always-on per-batch failure log (upserted per failing job from the first failed attempt; bounded by failures, not total jobs) |
| `create_kafka_batch_consumer_heartbeats` | Consumer heartbeats for the `:store` live-activity backend (one row per consumer; only needed if `liveness_backend = :store`) |

```bash
bundle exec rails db:migrate
```

### Redis store

No migrations needed. Batch state is stored as a Redis Hash at `kafka_batch:b:{batch_id}` (expires after `config.batch_ttl` seconds, refreshed on every completion event). Per-partition completion cursors live in a single `kafka_batch:offsets` hash — O(num partitions), never growing with job count.

> **Reconciler on Redis:** fully supported — the store maintains `kafka_batch:index:running` and `kafka_batch:index:done` sorted sets automatically, so `stale_batches` / `done_batches_without_callback` work without any app-side bookkeeping.

### Karafka routing

Wire up KafkaBatch routes inside your `karafka.rb`. Call `KafkaBatch.draw_routes(self)` from **inside** `routes.draw`, and make sure your worker classes are **loaded first** (reference them or eager-load) so the registry is populated:

```ruby
class KarafkaApp < Karafka::App
  setup do |config|
    config.kafka = { "bootstrap.servers" => ENV["KAFKA_BROKERS"] }
    config.client_id = "my-app"
    # Recommended: >1 so control-plane messages (events/callbacks) are worked
    # in parallel with jobs and don't queue behind a long-running job.
    config.concurrency = 5
  end

  routes.draw do
    # Your own routes
    topic "my_app.events" do
      consumer MyEventsConsumer
    end

    # Ensure worker classes are registered before drawing routes:
    ProcessOrderWorker
    # KafkaBatch: control-plane group + jobs group
    KafkaBatch.draw_routes(self)
  end
end
```

`draw_routes` registers **two consumer groups**, deliberately isolating the control plane from job execution so progress/callbacks aren't blocked behind long jobs:

| Group | Topic(s) | Consumer(s) |
|---|---|---|
| `<consumer_group>-control` | `events`, `callbacks`, `jobs.retry` | `EventConsumer`, `CallbackConsumer`, `RetryConsumer` |
| `<consumer_group>-jobs` | each worker's `kafka_topic` | `JobConsumer` |

> **Tip:** keep `config.concurrency > 1` (and/or run a separate `karafka server` process for the control group) so the control plane is processed in parallel with jobs. With `concurrency = 1`, a long-running job can delay (not starve) event/callback processing.

---

## Completion counting & scalability

Knowing when a batch is "done" requires idempotent counting over an at-least-once event stream. KafkaBatch does this with an **offset-inbox**: state stays **O(number of worker-topic partitions)**, independent of batch size, so a 10-job batch and a 50-million-job batch cost the same to track.

Each completion event carries the **immutable source coordinates** of its job message — `src_topic`, `src_partition`, `src_offset` — and is keyed by `src_topic/src_partition`. The store keeps a **monotonic per-partition cursor** (one row in `kafka_batch_consumer_offsets`, or one field in the Redis `kafka_batch:offsets` hash) and applies a completion only when `src_offset` exceeds the cursor.

Because the source offset is stable across reprocessing, this deduplicates **both**:

- **redelivered** events (consumer redelivery / rebalance), and
- **re-produced** events (the job message was redelivered and the worker re-ran) — the second copy carries the same source offset and is rejected.

Keying events by source partition also spreads completion processing across the event-topic partitions instead of funnelling a whole batch through one, so completion throughput scales horizontally with partition count.

**Guarantees**

- ✅ Exact completion counting with flat, batch-size-independent state.
- ✅ Horizontally scalable completion processing (per-partition, not per-batch).
- ✅ Relies on the **idempotent producer** (enabled by default) so the worker topic itself can't contain produce-retry duplicates.

**Trade-offs / what is *not* guaranteed**

- ❌ No per-job audit — only aggregate counts (`completed_count` / `failed_count`). Failures are still visible in the dead-letter topic.
- ❌ `perform` still runs **at-least-once** — workers must be idempotent (unchanged).
- ⚠️ Slightly higher per-event latency than a naive counter; on Redis **Cluster** the two-key Lua requires same-slot placement.

> **Why no Kafka transactions?** True exactly-once read-process-write with transactional offset commits is a Karafka **Pro** feature. The offset-inbox reaches the same *counting* guarantee on open-source Karafka by deduping on the job's immutable source offset plus the idempotent producer — no broker transactions required.

---

## Defining workers

Include `KafkaBatch::Worker` and implement `#perform`:

```ruby
class ProcessOrderWorker
  include KafkaBatch::Worker

  kafka_topic            "orders.process" # required – Kafka topic to consume from
  max_retries            5                # optional – overrides config.max_retries
  complete_after_retries 3                # optional – overrides config.complete_after_retries

  # payload is the Hash passed to Batch#push or Batch.enqueue
  def perform(payload)
    order = Order.find(payload["order_id"])
    order.process!
  end
end
```

> **Workers must be idempotent.** If `perform` succeeds but the subsequent event-emission fails, the job message is redelivered and `perform` runs again. Design your workers so running twice produces the same result (upsert, check-before-write, etc.).

---

## Creating batches

```ruby
batch = KafkaBatch::Batch.create(
  on_success:  "BatchSuccessCallback",   # called if ALL jobs succeed
  on_complete: "BatchCompleteCallback",  # called when ALL jobs finish (any status)
  description: "Nightly report rebuild",  # optional human label, shown in the Web UI
  meta: { report_id: 42, user_id: 99 }  # arbitrary data forwarded to callbacks
) do |b|
  Order.find_each do |order|
    b.push(ProcessOrderWorker, { order_id: order.id })
  end
end

puts batch.id  # => "550e8400-e29b-41d4-a716-446655440000"
```

`description:` is an optional free-text label to help you tell batches apart in the dashboard (shown on both the list and detail pages). On the MySQL store it requires the `add_description_to_kafka_batch_records` migration; the Redis store needs nothing.

There is **no lock step**. A batch stays **open** and accepts more jobs — from anywhere, including from jobs that belong to it — until it **completes** (all jobs done → callback fires) or is cancelled. The completion callback fires automatically the moment the batch drains (`completed + failed >= total_jobs`).

The **block form is recommended** for one-shot population: the batch is held open for the duration of the block, so it cannot complete mid-population even if early jobs finish before later ones are pushed. When the block returns it is sealed and finalizes once everything is done.

> **Wrap the payload in `{ }`.** Because `push`/`push_many`/`enqueue` accept a
> `job_id:` keyword, a brace-less hash (e.g. `push(W, order_id: 1)`) is parsed by
> Ruby 3 as keyword arguments and raises `ArgumentError: unknown keyword`. Always
> pass the payload as an explicit Hash: `push(W, { order_id: 1 })`.

An optional explicit `job_id` can be passed for tracing:

```ruby
b.push(ProcessOrderWorker, { order_id: 1 }, job_id: "order-1-#{Time.now.to_i}")
```

### Adding jobs over time

**Add jobs from inside a running job** (the main reason there's no lock) — a worker can fan out into its *own* batch via `batch` (nil for standalone jobs). This is always safe: a running job is itself a pending unit, so the batch can't drain while it runs, and its children are counted before its own completion is recorded.

```ruby
class CrawlPageWorker
  include KafkaBatch::Worker
  kafka_topic "crawl.pages"

  def perform(payload)
    page = fetch(payload["url"])
    page.links.each do |link|
      batch&.push(CrawlPageWorker, { "url" => link })   # add child jobs to the same batch
    end
  end
end
```

**Push many at once** — grows `total_jobs` with a single store write, then produces each job:

```ruby
batch.push_many(ProcessUserWorker, users.map { |u| { "user_id" => u.id } })
# => ["job-uuid-1", "job-uuid-2", ...]
```

**Re-attach from another process** with `Batch.open(id)`:

```ruby
KafkaBatch::Batch.open(batch_id).push(ProcessUserWorker, { "user_id" => 7 })
```

- `Batch.create` **without a block** returns a `Batch` that is sealed immediately, so it completes as soon as it drains. If every pushed job can finish before you push more, prefer the block form — otherwise the callback may fire early and further pushes raise `KafkaBatch::BatchClosedError`.
- `Batch.open(id)` re-attaches to an existing batch so you can `push`/`push_many` from anywhere (raises `BatchNotFoundError` if unknown).
- `total_jobs` updates live as you push (visible in `Batch.find` and the [Web UI](#web-ui)).
- Pushing into a **completed** or **cancelled** batch raises `KafkaBatch::BatchClosedError`.
- If a `push` fails to produce, the job count is rolled back so the total stays accurate.

> The reconciler skips held (block-form, not-yet-sealed) batches, so an in-progress population is never mistaken for a stuck one.

### Standalone jobs (no batch)

```ruby
KafkaBatch::Batch.enqueue(ProcessOrderWorker, { order_id: 99 })
```

The job goes through the same retry / DLT flow but no batch completion tracking occurs.

### Batch.find and Batch.cancel

```ruby
# Look up the current state of a batch
batch = KafkaBatch::Batch.find(batch_id)
# => { id: "uuid", status: "running", completed_count: 42, total_jobs: 100, ... }

# Cancel a batch: remaining jobs are skipped and callbacks never fire
KafkaBatch::Batch.cancel(batch_id)
```

`cancel` sets `status` to `"cancelled"` in the store. With `config.skip_cancelled_jobs = true` (the default), the `JobConsumer` **skips execution** of not-yet-processed jobs in that batch — so cancelling effectively stops the remaining work.

To avoid a store read on every job, each consumer process caches the set of cancelled batch ids and refreshes it at most once per `config.cancellation_cache_ttl` seconds (default 120). Cancellation is therefore **eventually-consistent**: some already-queued jobs may still run until the next refresh — an accepted trade-off for throughput. The `EventConsumer` also treats a cancelled batch as a no-op, so callbacks never fire regardless.

Set `config.skip_cancelled_jobs = false` to disable the cancellation gate entirely (cancel then only suppresses callbacks).

You can also cancel (and delete) batches from the [Web UI](#web-ui).

---

## Callbacks

Callbacks are plain Ruby classes with a method matching the callback type:

```ruby
class BatchSuccessCallback
  # Called only when every job in the batch succeeded (failed_count == 0).
  def on_success(batch)
    AdminMailer.batch_complete(
      id:        batch["batch_id"],
      count:     batch["total_jobs"],
      meta:      batch["meta"]
    ).deliver_later
  end
end

class BatchCompleteCallback
  # Called when all jobs finish regardless of individual failure count.
  def on_complete(batch)
    if batch["failed_count"].positive?
      Sentry.capture_message("Batch #{batch['batch_id']} had failures", extra: batch)
    end
    TempStorage.delete(batch.dig("meta", "temp_dir"))
  end
end
```

The `batch` hash passed to callbacks:

```ruby
{
  "batch_id"        => "uuid",
  "outcome"         => "success",   # "success" | "complete"
  "total_jobs"      => 1000,
  "completed_count" => 998,
  "failed_count"    => 2,
  "on_success"      => "BatchSuccessCallback",
  "on_complete"     => "BatchCompleteCallback",
  "meta"            => { "report_id" => 42 },
  "finished_at"     => "2024-01-15T10:30:00Z",
  "reconciled"      => false        # true if fired by the reconciler
}
```

| Callback | When it fires |
|---|---|
| `on_success` | All jobs succeeded (`failed_count == 0`) |
| `on_complete` | All jobs finished regardless of failures |

**At-least-once guarantee (callbacks must be idempotent):** `CallbackConsumer` invokes the callbacks **first**, then claims dispatch by setting `callback_dispatched_at`. Because callback messages are keyed by `batch_id`, all callbacks for a batch land on a single partition and are processed sequentially, so a duplicate message is cheaply suppressed by the pre-invocation `callback_dispatched?` check. A crash between invocation and the claim results in re-invocation on redelivery — never a silently lost callback. This matches Sidekiq Pro's "callbacks may run more than once" semantics, so **make your callbacks idempotent**.

**Unresolvable class names:** If the callback class doesn't exist (typo, rename after deploy), the message is forwarded to `dead_letter_topic` with `dlt_type: "callback"` instead of being silently dropped.

**Callback exceptions forwarded to DLT:** If a callback class raises `StandardError` at runtime, the error is forwarded to the DLT with `dlt_type: "callback_error"` so it is visible and replayable. Dispatch is still claimed afterwards (the failure is captured in the DLT) — if you need retry semantics on a callback, make the callback class a `KafkaBatch::Worker` itself.

---

## Retry behaviour

When a job raises an exception, `JobConsumer` catches it and takes one of two paths based on the current attempt count:

**Retriable (attempt < max_retries):**
The message is produced to `kafka_batch.jobs.retry` with two extra fields:
- `retry_after` — ISO8601 timestamp of when to re-enqueue (exponential backoff; see below)
- `retry_to` — the original worker topic to re-enqueue to

The `JobConsumer` partition is immediately freed for the next message. No thread blocking occurs.

**RetryConsumer** picks up the message. If `retry_after` is still in the future, it calls Karafka's `pause(offset, ms)` to suspend that partition for up to `MAX_PAUSE_SECONDS` (30s) at a time, then checks again. When the message is due it re-enqueues to `retry_to` and commits.

**Exhausted (attempt == max_retries):**
A `failed` event is emitted to the events topic (so the batch counter is updated) and the message is forwarded to the dead-letter topic.

Backoff is a **fixed, short schedule** (deliberately Kafka-friendly): the **1st** retry after `retry_first_delay` (default **10s**), and **every subsequent** retry after `retry_delay` (default **180s / 3 min**), each with `±retry_jitter` (default 10%) randomization to avoid synchronized retry storms. e.g. `max_retries: 4` ⇒ retries at ~10s, ~3m, ~6m, ~9m.

Short delays keep the `RetryConsumer`'s `pause()` head-of-line wait negligible (≤ `retry_delay`), so no scheduler/re-queue machinery is needed. The **time until the next retry** is recorded on each retrying failure and shown in the dashboard's *Job failures* "Next retry" column (e.g. `in 2m 47s`).

> For long downstream outages this exhausts retries within ~`max_retries × retry_delay`; raise `max_retries` (cheap, since each retry is short) or replay from the DLT.

Override attempts per worker with `max_retries`.

### Early batch completion (`complete_after_retries`)

A persistently-failing job can otherwise hold up its batch's **`on_complete`** for the whole retry budget (`max_retries × retry_delay`), even when every other job is done. To cap that latency, a job counts toward its batch (as *failed*) after **`complete_after_retries`** retries (default **3**) — while it **keeps retrying in the background** up to `max_retries`:

```ruby
class FlakyWorker
  include KafkaBatch::Worker
  kafka_topic "flaky.jobs"
  max_retries            20   # keep trying for a long time
  complete_after_retries 3    # ...but don't make the batch wait past 3 retries
end
```

- The batch's `on_complete` fires once all jobs have either succeeded **or** hit `complete_after_retries`.
- Counting is **exactly once** — a `batch_counted` flag rides the retry message, so the later background retries (success or exhaustion) never double-count.
- **`on_success` is unaffected**: it still fires only when every job genuinely succeeds. A batch with an early-counted job reports outcome `complete` (a job was counted failed), so `on_success` won't fire even if that job later succeeds in the background.
- Default `complete_after_retries` (3) == default `max_retries` (3), so **default behaviour is unchanged** — set `max_retries` higher to benefit.

**Event emission retries:** If `perform` succeeds but the subsequent produce to `kafka_batch.events` fails (transient Kafka issue), the gem retries emission up to `EVENT_EMIT_RETRIES` (3) times with a short backoff. If all retries fail, the offset is left uncommitted so Karafka redelivers the job message and `perform` runs again. This is why workers must be idempotent.

---

## Dead Letter Topic

Jobs that exhaust all retries, and callback classes that cannot be resolved, are forwarded to `kafka_batch.dead_letter`. The payload is the original message augmented with:

```json
{
  "dlt_type":          "job",
  "dlt_source_topic":  "orders.process",
  "dlt_error_class":   "ActiveRecord::RecordNotFound",
  "dlt_error_message": "Couldn't find Order with id=99",
  "dlt_at":            "2024-01-15T10:30:00Z"
}
```

For unresolvable callback classes (`dlt_type: "callback"`) and callback runtime errors (`dlt_type: "callback_error"`):

```json
{
  "dlt_type":            "callback",
  "dlt_callback_class":  "MySuccessCallback",
  "dlt_callback_method": "on_success",
  "dlt_error_class":     "NameError",
  "dlt_error_message":   "uninitialized constant MySuccessCallback",
  "dlt_source_topic":    "kafka_batch.callbacks",
  "dlt_at":              "2024-01-15T10:30:00Z"
}
```

For malformed JSON payloads (events or callbacks topics), the raw payload is forwarded as:

```json
{
  "dlt_type":          "malformed_event",
  "dlt_source_topic":  "kafka_batch.events",
  "dlt_raw_payload":   "...",
  "dlt_error_class":   "ArgumentError",
  "dlt_error_message": "Invalid JSON in event: ...",
  "dlt_at":            "2024-01-15T10:30:00Z"
}
```

Subscribe a consumer in your `karafka.rb` to alert, log, or trigger manual replay:

```ruby
topic KafkaBatch.config.dead_letter_topic do
  consumer DeadLetterConsumer
end
```

---

## Multi-tenant fairness (WFQ)

When many tenants (businesses) push jobs into the same system, a naive Kafka topic processes them roughly FIFO — so one tenant dumping 10M jobs starves everyone behind it. KafkaBatch shares capacity dynamically across tenants using **only Kafka — no Redis required**:

- **1 active tenant → 100%** of capacity; **2 → ~50:50**; **N → ~1/N each** — and it's **work-conserving** (an idle tenant's share is instantly redistributed).
- The durable backlog stays in **Kafka** (the ingest topic), so memory is bounded regardless of backlog size. Nothing is stored in Redis on the fairness path.
- Fairness is **approximate** ("good enough"): it relies on Kafka's balanced per-partition fetch plus a shallow, throttled ready topic.

> **Does fairness need Redis? No.** The default fairness path (ingest → dispatcher → ready → swarm) uses Kafka only. Redis is involved **only** if you opt into the standalone `KafkaBatch::Fairness::Scheduler` (a virtual-time WFQ engine for *strict weighted* shares), which is **not** wired into the default path.

Tag jobs with a tenant:

```ruby
batch = KafkaBatch::Batch.create(on_complete: "Cb", tenant_id: "acme")
batch.push(ProcessUserWorker, { "user_id" => 1 })          # inherits tenant "acme"
batch.push(ProcessUserWorker, { "user_id" => 2 }, tenant_id: "globex")  # override
```

Configure (no Redis needed):

```ruby
config.fairness_enabled        = true
config.fairness_ingest_topic   = "kafka_batch.ingest"  # per-tenant intake (durable backlog)
config.fairness_ready_topic    = "kafka_batch.ready"   # throttled execution queue
config.fairness_ready_lag_high = 5000   # dispatcher pauses forwarding above this depth
config.fairness_ready_lag_low  = 1000   # ...resumes below this depth
```

> The `fairness_global_concurrency`, `fairness_ready_window`, `fairness_max_inflight_per_tenant`, and `fairness_default_weight` settings apply **only** to the optional Redis-backed `Scheduler` — not the default dispatcher.

### How it's wired (reuses normal Kafka consumers, no Redis on the path)

Execution stays on ordinary `JobConsumer`s — fairness is achieved by controlling the *order* jobs reach them, using only Kafka:

```
push → ingest topic (keyed one-tenant-per-partition)
        │
   Fairness::Dispatcher (Karafka consumer): forwards each job ingest → ready,
        │   THROTTLED so the ready topic's un-consumed depth stays between
        │   fairness_ready_lag_low/high (pauses above high, resumes below low)
        ▼
   ready topic ── swarm of normal JobConsumers (full speed) → perform → events
```

Two things make this fair, with no Redis and no extra process:

1. **Kafka's balanced fetch.** A consumer fetches roughly evenly across its assigned partitions, so with ingest keyed one-tenant-per-partition the dispatcher naturally forwards a balanced mix. One active tenant fills the ready topic alone (**100%**); N active split **~1/N**; idle tenants contribute nothing (**work-conserving**).
2. **A shallow ready topic.** The throttle keeps the ready topic's depth bounded, so a newly active tenant only ever waits behind ~the watermark of queued work — not the whole backlog. That's what keeps fairness *dynamic*.

`draw_routes` wires this automatically when `fairness_enabled` (a `…-dispatch` group on the ingest topic + the `…-jobs` group on the ready topic). The durable backlog stays in the **ingest topic (Kafka)**; the ready topic + existing retry/DLT path keep the usual at-least-once guarantees.

Fairness here is **approximate** ("good enough"): granularity is the fetch batch, and it assumes ~even partition assignment per tenant and similar job sizes. For **strict weighted shares**, `KafkaBatch::Fairness::Scheduler` is available as a standalone Redis-backed virtual-time WFQ engine (`enqueue`/`checkout`/`complete`/`set_weight`/`stats`) you can build a custom dispatcher/worker around.

---

## Web UI

A small, dependency-free Rack dashboard (think a tiny "Sidekiq Web") for inspecting batches. It works with either store and renders self-contained HTML/CSS — no asset pipeline or extra gems.

Mount it in your routes:

```ruby
# config/routes.rb
require "kafka_batch/web"

Rails.application.routes.draw do
  mount KafkaBatch::Web => "/kafka_batch"
end
```

> **Mount it behind authentication.** The UI exposes destructive actions (cancel / delete). Wrap the mount in your admin constraint, e.g.:
>
> ```ruby
> authenticate :user, ->(u) { u.admin? } do
>   mount KafkaBatch::Web => "/kafka_batch"
> end
> ```

What it shows:

- **Summary metrics** — total batches and counts by status (running / success / complete / cancelled).
- **Batch list** — newest first, with status badge, total / done / failed / **pending** counts, a progress bar, and status filters. Paginated.
- **Batch detail** — all fields, callbacks, meta, progress, and a **Job failures** list. Failures are recorded on the **first failed attempt** (status `retrying` while retries remain, `failed` once exhausted) so problems surface immediately rather than after hours of retries. Each row shows the worker, attempt #, error class/message, and time. Upserted per job and bounded by the number of failing jobs, so it's cheap even for huge batches.
- **All failures** (`/failures`) — a cross-batch view of every failure in one place (linked from the dashboard), filterable by `retrying` / `failed`, each row linking back to its batch.
- **Live activity** (`/live`) — currently-running jobs (job, batch, worker, which consumer process, topic/partition, start time) and the live consumer processes (host, PID, last-seen), auto-refreshing every 5s. It's **approximate** (very short-lived jobs may not appear between snapshots). Choose a backend with `config.liveness_backend`:
  - **`:redis`** (default) — full per-job tracking in Redis (`config.redis_url`) with a short TTL (`config.liveness_ttl`, default 30s), best-effort behind a circuit breaker so it never slows jobs; crashed entries expire on their own. If Redis isn't reachable, the page says the feature is unavailable. (`config.track_running_jobs = false` disables the per-job writes.)
  - **`:store`** — consumer **heartbeat + sampled current job** in the configured store (e.g. MySQL). Writes scale with the **number of consumers, not job throughput** (throttled to once per `config.liveness_heartbeat_interval`, default 5s), so it's reliable and low-impact — no per-job row churn. Staleness is handled by `last_seen` + a sweep in the reconciler. You see consumer count + what each is working on (sampled), rather than every individual in-flight job. Requires the `create_kafka_batch_consumer_heartbeats` migration on MySQL.
  - **`:off`** — disabled.
- **Actions** —
  - **Cancel** (running batches): sets status to `cancelled`; with `skip_cancelled_jobs` the remaining jobs stop processing (eventually-consistent — within `cancellation_cache_ttl`).
  - **Delete**: removes the batch record (best used for finished batches).

Routes (relative to the mount point):

| Method | Path | Action |
|---|---|---|
| `GET` | `/` | Batch list + metrics (`?status=`, `?page=`) |
| `GET` | `/batches/:id` | Batch detail |
| `POST` | `/batches/:id/cancel` | Cancel batch |
| `POST` | `/batches/:id/delete` | Delete batch |

> **Redis note:** the list is backed by an `kafka_batch:index:all` sorted set. Since Redis batch keys expire after `batch_ttl`, the UI shows batches within that window (expired members are pruned lazily). MySQL-backed batches persist until deleted.

---

## Reconciler

The reconciler detects and recovers two classes of stuck batches:

### 1. Stuck-running batches

**Cause:** `EventConsumer` lag or message loss — all jobs completed but the counter never reached `total_jobs` because event messages were never produced or consumed.

**Detection:** `status = "running"` and `created_at < now - reconciliation_interval`.

**Recovery:** Compares `completed_count + failed_count` against `total_jobs`. If equal, transitions status and re-produces the callback message.

### 2. Lost-callback batches

**Cause:** `EventConsumer` updated the store to `success`/`complete` but crashed before or during the produce to `kafka_batch.callbacks`. The batch is "done" in the store but the callback was never fired.

**Detection:** `status IN (success, complete)` AND `callback_dispatched_at IS NULL` AND `finished_at < now - reconciliation_interval`.

**Recovery:** Re-produces the callback message to `kafka_batch.callbacks`. The `CallbackConsumer`'s `callback_dispatched_at` claim suppresses duplicates in the normal path, so re-producing is safe even if this runs multiple times (callbacks must be idempotent).

> **Redis store:** the running / lost-callback indexes (`kafka_batch:index:running` and `kafka_batch:index:done` sorted sets) are maintained automatically as batches move through their lifecycle, so the reconciler works on the Redis store too — no app-side bookkeeping required.

**Distributed lock:** `Reconciler.run` acquires a store-level distributed lock before sweeping, so running the rake task from multiple servers concurrently is safe — only one process runs the sweep at a time. MySQL uses `GET_LOCK`/`RELEASE_LOCK`; Redis uses `SET NX EX`.

```bash
bundle exec rake kafka_batch:reconcile
```

Schedule with cron or Whenever:

```ruby
# config/schedule.rb
every 5.minutes do
  rake "kafka_batch:reconcile"
end
```

---

## Instrumentation

KafkaBatch emits `ActiveSupport::Notifications` events at key lifecycle points so you can wire in metrics, logging, or alerting without modifying the gem.

| Event | Payload |
|---|---|
| `job.processed.kafka_batch` | `job_id`, `batch_id`, `worker_class`, `duration` |
| `job.retried.kafka_batch` | `job_id`, `batch_id`, `worker_class`, `attempt`, `next_attempt`, `retry_after` |
| `job.failed.kafka_batch` | `job_id`, `batch_id`, `worker_class`, `attempt`, `error_class`, `error_message` |
| `batch.completed.kafka_batch` | `batch_id`, `outcome`, `total_jobs`, `completed_count`, `failed_count` |
| `callback.invoked.kafka_batch` | `batch_id`, `callback_class`, `callback_method` |
| `callback.failed.kafka_batch` | `batch_id`, `callback_class`, `callback_method`, `error_class`, `error_message` |
| `reconciler.ran.kafka_batch` | `stale_count`, `lost_count`, `duration` |

Subscribe in an initializer:

```ruby
# config/initializers/kafka_batch_instrumentation.rb

ActiveSupport::Notifications.subscribe("job.processed.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  StatsD.increment("kafka_batch.job.processed", tags: ["worker:#{event.payload[:worker_class]}"])
  StatsD.timing("kafka_batch.job.duration_ms", event.duration)
end

ActiveSupport::Notifications.subscribe("job.failed.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  Sentry.capture_message("KafkaBatch job exhausted retries", extra: event.payload)
end

ActiveSupport::Notifications.subscribe("batch.completed.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  StatsD.increment("kafka_batch.batch.completed", tags: ["outcome:#{event.payload[:outcome]}"])
end
```

When `ActiveSupport` is not available (non-Rails environments), all instrumentation calls are no-ops — the gem works without it.

---

## Rake tasks

| Task | Description |
|---|---|
| `kafka_batch:reconcile` | Run both reconciler sweeps (stuck-running + lost-callback) |
| `kafka_batch:install_migrations` | Copy all migrations to `db/migrate/` |
| `kafka_batch:workers` | Print all registered workers, topics, and retry config |

```bash
bundle exec rake kafka_batch:workers
#  ProcessOrderWorker   → topic: orders.process   retries: 5  backoff: 10s
#  GenerateReportWorker → topic: reports.generate retries: 3  backoff: 5s
```

---

## Reliability guarantees

| Guarantee | How it's achieved |
|---|---|
| **Batch never prematurely completes** | Store record (with exact `total_jobs`) is written before the first message is produced |
| **Partial produce is cleaned up** | Any `StandardError` in `flush!` calls `delete_batch` to roll back the store record |
| **Job completion is idempotent** | Monotonic per-partition cursor over the job message's source offset deduplicates redelivered and re-produced events |
| **Counter increment is atomic** | MySQL `SELECT FOR UPDATE` + `UPDATE field = field + 1`; Redis Lua script |
| **Redis `create_batch` is race-free** | Lua script uses `HSETNX` as existence sentinel — single atomic operation, no TOCTOU |
| **Callback fires at least once** | Callback is invoked, then `callback_dispatched_at` is set; duplicates are suppressed and crashes lead to safe re-invocation (callbacks must be idempotent) |
| **Lost callbacks are recovered** | Reconciler scans for `status IN (success,complete) AND callback_dispatched_at IS NULL` |
| **Reconciler runs once per cluster** | Distributed lock (MySQL `GET_LOCK`, Redis `SET NX EX`) prevents concurrent reconciler sweeps |
| **Retries don't block partitions** | Failed jobs go to `kafka_batch.jobs.retry`; `RetryConsumer` uses Karafka `pause()` |
| **Event emission failure ≠ job failure** | Separate rescue blocks; emission retried independently before leaving offset uncommitted |
| **Malformed JSON is never silently dropped** | Unparseable messages in all consumers are forwarded to DLT before committing |
| **Cancellation stops remaining jobs** | `JobConsumer` skips jobs of cancelled batches using a per-process cancelled-id cache (eventually-consistent within `cancellation_cache_ttl`) |
| **Callback exceptions are not silently swallowed** | `StandardError` in callbacks → DLT with `dlt_type: "callback_error"` |
| **Unresolvable callbacks are not silently dropped** | Forwarded to `dead_letter_topic` with `dlt_type: "callback"` |
| **DLT publish failure causes redelivery** | If DLT produce fails, offset is left uncommitted so Karafka redelivers the message |
| **Consumer crash after callback but before commit** | `claim_callback` CAS prevents double-invocation on redelivery |
| **Worker resolution is fast** | `WORKER_CACHE` hash caches `const_get` lookups per class name; thread-safe via mutex |
| **Store and worker registry are thread-safe** | Double-checked locking with `Mutex` on both `store` and `workers` singleton accessors |

---

## Known limitations

**Workers must be idempotent.** If event emission fails after a successful `perform`, Karafka redelivers the job and `perform` runs again. Design workers to tolerate duplicate execution (upsert, guard clauses, etc.).

**No per-job audit.** Completion counting is offset-based (aggregate counts only), so the store cannot answer "did job X run / which jobs failed?". Failed jobs are still captured in the dead-letter topic for inspection/replay.

**Redis TTL.** Batch keys expire after `batch_ttl` seconds. The TTL is refreshed on every job completion event, but a batch with no activity for longer than `batch_ttl` will lose its state. Set `batch_ttl` well above your longest expected batch duration.

**Worker class renames after deploy.** In-flight messages carry the original class name. After removing or renaming a worker, the consumer forwards those jobs straight to the DLT (and emits a `failed` event so the batch still completes) rather than blocking the partition. Perform a rolling deploy or drain the topic before removing the class.

**No automatic metrics sink.** Instrumentation events are emitted via `ActiveSupport::Notifications` (see [Instrumentation](#instrumentation)) but nothing is sent to Prometheus/StatsD by default. Subscribe to the events to forward them to your metrics backend.

---

## Migrating from Sidekiq Pro Batches

| Sidekiq Pro | kafka-batch |
|---|---|
| `Sidekiq::Batch.new` | `KafkaBatch::Batch.create` |
| `batch.jobs { MyWorker.perform_async(...) }` | `b.push(MyWorker, ...)` inside block |
| `batch.on(:success, MyCallback)` | `on_success: "MyCallback"` parameter |
| `batch.on(:complete, MyCallback)` | `on_complete: "MyCallback"` parameter |
| Callback `#on_success(status)` | `#on_success(batch_hash)` |
| Callback `#on_complete(status)` | `#on_complete(batch_hash)` |
| `status.bid` | `batch["batch_id"]` |
| `status.total` | `batch["total_jobs"]` |
| `status.failures` | `batch["failed_count"]` |

**What you don't need to change:** Callback class names and method signatures are structurally the same.

**Key difference:** Workers must `include KafkaBatch::Worker`, define a `kafka_topic`, and be **idempotent**. They are consumed by Karafka rather than Sidekiq threads.

---

## Architecture deep-dive

### Why write the batch record before producing?

If the store record were written after producing, a fast consumer could complete all N jobs and find no batch record — resulting in `not_found` and a permanently lost callback. Writing first with the exact count guarantees the store is ready before any completion event can arrive.

### Why roll back on partial produce failure?

If only M of N messages reach Kafka, the store has `total_jobs: N` but only M jobs will ever complete. Without rollback, the batch hangs in "running" indefinitely. With `delete_batch`, the caller receives a `ProducerError` and can retry the entire `Batch.create` call.

### Why `FOR UPDATE` and not `LOCK IN SHARE MODE`?

Share locks (`LOCK IN SHARE MODE`) allow concurrent readers. Two `EventConsumer` threads processing the last two jobs can both enter the transaction simultaneously, both increment, both reload and see `completed >= total`, and both publish the callback — double-firing `on_success`. `FOR UPDATE` (`.lock`) serialises access: the second thread blocks until the first commits, sees the already-finalised status, and returns `:duplicate`.

### Why separate `perform` and event-emission rescue blocks?

With a single `rescue`, a transient Kafka error on `emit_event` looks identical to a job failure and triggers a job retry. The work was already done — retrying the job runs it again (possibly corrupting state) and eventually sends a false "failed" event to the DLT. Separate rescue blocks mean event-emission failures are retried independently, and only a worker-raised exception triggers the job retry path.

### Why a dedicated retry topic instead of `sleep`?

A `sleep` inside `JobConsumer` blocks the entire Kafka partition for the backoff duration. The retry topic approach forwards the message immediately and suspends only the *retry partition* (via Karafka `pause()`) — the job partition is fully unblocked. Because the backoff schedule is short and bounded (`retry_delay`, default 3 min), the retry partition's head-of-line pause stays small.

### Why invoke the callback first, then claim?

Callbacks are **at-least-once**: the `CallbackConsumer` invokes the callback, then sets `callback_dispatched_at`. A crash between the two re-invokes on redelivery (never a lost callback) — matching Sidekiq Pro. Duplicates in the normal path are suppressed by a pre-invocation `callback_dispatched?` check, which is reliable because callback messages are keyed by `batch_id` (one partition → one consumer → sequential). Make callbacks idempotent.

### Message flow (numbered)

```
1.  App             → MySQL/Redis       CREATE batch record (total_jobs = N)
2.  App             → Kafka jobs topic  PRODUCE N job messages (idempotent producer)
3.  JobConsumer     → worker            CALL perform(payload)
4a. (success)       → Kafka events      PRODUCE {batch_id, status: success, src_topic/partition/offset}
4b. (failure)       → Kafka retry       PRODUCE {retry_after, retry_to, attempt+1}
4c. (exhausted)     → Kafka events      PRODUCE {batch_id, status: failed, src_*}
                    → Kafka DLT         PRODUCE original message + error context
5.  RetryConsumer   pauses partition    WAIT until retry_after via Karafka pause()
                    → Kafka jobs topic  PRODUCE message back to original topic
6.  EventConsumer   → MySQL/Redis       ATOMIC offset-cursor dedup + increment + check
7.  EventConsumer   → Kafka callbacks  PRODUCE callback message (if batch done)
8.  CallbackConsumer → callback class   INVOKE on_success / on_complete, then CLAIM
```

**In fairness mode**, step 2 changes to: `Batch.push → Kafka ingest topic` (keyed by `tenant_id`), then `Fairness::Dispatcher → Kafka ready topic` (fairly ordered + throttled), and the `JobConsumer` swarm consumes the **ready** topic. Steps 3–8 are otherwise unchanged.

---

## Topic reference

| Topic (default name) | Produced by | Consumed by | Purpose |
|---|---|---|---|
| `kafka_batch.jobs` (per worker) | `Batch.create` / `Batch.enqueue` | `JobConsumer` | Individual job messages (non-fairness mode) |
| `kafka_batch.jobs.retry` | `JobConsumer` | `RetryConsumer` | Failed jobs awaiting backoff |
| `kafka_batch.events` | `JobConsumer` | `EventConsumer` | Job completion signals |
| `kafka_batch.callbacks` | `EventConsumer` / `Reconciler` | `CallbackConsumer` | Batch-complete triggers |
| `kafka_batch.dead_letter` | `JobConsumer` / `CallbackConsumer` / `RetryConsumer` | Your consumer | Exhausted jobs + unresolvable callbacks |
| `kafka_batch.ingest` *(fairness only)* | `Batch.push` (keyed by `tenant_id`) | `Fairness::Dispatcher` | Per-tenant intake queue (durable backlog) |
| `kafka_batch.ready` *(fairness only)* | `Fairness::Dispatcher` | `JobConsumer` swarm | Fairly-ordered, throttled execution queue |

In **fairness mode** (`config.fairness_enabled = true`), jobs flow `ingest → ready → JobConsumer` instead of straight to the per-worker topic. The `ingest` topic holds the durable backlog (key it one-tenant-per-partition), and the `ready` topic is the shallow, throttled queue the worker swarm drains. See [Multi-tenant fairness](#multi-tenant-fairness-wfq).

---

## Contributing

1. Fork the repo
2. `bundle install`
3. `bundle exec rspec`
4. Submit a PR

Please add tests for new behaviour and keep the store interface (`lib/kafka_batch/stores/base.rb`) in sync if you add a new store backend.

---

## License

MIT
