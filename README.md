# kafka-batch

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
- [Defining workers](#defining-workers)
- [Creating batches](#creating-batches)
  - [Standalone jobs (no batch)](#standalone-jobs-no-batch)
- [Callbacks](#callbacks)
- [Retry behaviour](#retry-behaviour)
- [Dead Letter Topic](#dead-letter-topic)
- [Reconciler](#reconciler)
- [Rake tasks](#rake-tasks)
- [Migrating from Sidekiq Pro Batches](#migrating-from-sidekiq-pro-batches)
- [Architecture deep-dive](#architecture-deep-dive)
- [Topic reference](#topic-reference)
- [Contributing](#contributing)

---

## How it works

```
┌─────────────────────────────────────────────────────────────────┐
│                        Your application                          │
│                                                                  │
│  Batch.create do |b|                                             │
│    b.push(MyWorker, { id: 1 })   ──┐                            │
│    b.push(MyWorker, { id: 2 })   ──┤──► Kafka: jobs topic       │
│    b.push(MyWorker, { id: 3 })   ──┘                            │
│                         ▲                                        │
│  BatchRecord created in │                                        │
│  MySQL / Redis with     │                                        │
│  total_jobs = 3         │                                        │
└─────────────────────────┼────────────────────────────────────────┘
                          │
        ┌─────────────────▼────────────────────┐
        │         Karafka: JobConsumer          │
        │                                       │
        │  worker.perform(payload)              │
        │    ├─ success ──► events topic        │
        │    └─ failure ──► retry / DLT         │
        └─────────────────┬────────────────────┘
                          │
        ┌─────────────────▼────────────────────┐
        │        Karafka: EventConsumer         │
        │                                       │
        │  store.record_job_completion(...)     │
        │    ├─ still running ──► continue      │
        │    └─ all done ──────► callbacks topic│
        └─────────────────┬────────────────────┘
                          │
        ┌─────────────────▼────────────────────┐
        │       Karafka: CallbackConsumer       │
        │                                       │
        │  MySuccessCallback.new.on_success(b)  │
        │  MyCompleteCallback.new.on_complete(b)│
        └──────────────────────────────────────┘
```

**Reliability guarantees:**

- The batch record is written with the exact job count *before* any Kafka messages are produced — a batch can never be marked complete prematurely.
- Job completion is idempotent: duplicate `events` messages (Kafka at-least-once) are detected and silently dropped via a unique constraint (MySQL) or `SADD` (Redis).
- Counter increments are atomic at the database level — `SELECT FOR UPDATE` + `UPDATE field = field + 1` (MySQL) or a single Lua script (Redis) — so only one process ever fires the terminal callback.
- A periodic [reconciler](#reconciler) catches batches where the completion event was lost.

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
- database migrations (MySQL store only)

Run migrations if using the MySQL store:

```bash
bundle exec rails db:migrate
```

---

## Configuration

Edit `config/initializers/kafka_batch.rb`:

```ruby
KafkaBatch.configure do |config|
  # ── Store ────────────────────────────────────────────────────────
  # :mysql  – persistent, survives Redis restarts, queryable via SQL
  # :redis  – lower latency, no schema migration needed
  config.store = :mysql

  # ── Kafka brokers ────────────────────────────────────────────────
  config.brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

  # ── Topic names ──────────────────────────────────────────────────
  config.jobs_topic        = "kafka_batch.jobs"
  config.events_topic      = "kafka_batch.events"
  config.callbacks_topic   = "kafka_batch.callbacks"
  config.dead_letter_topic = "kafka_batch.dead_letter"

  # ── Consumer group ───────────────────────────────────────────────
  config.consumer_group = "kafka-batch"

  # ── Retry behaviour (global defaults; override per Worker class) ─
  config.max_retries   = 3   # max attempts before DLT
  config.retry_backoff = 5   # seconds; sleep = attempt * retry_backoff

  # ── Redis (only when store: :redis) ─────────────────────────────
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.redis_pool_size = 5
  config.batch_ttl       = 7 * 24 * 3600  # seconds; 7 days

  # ── Reconciliation ───────────────────────────────────────────────
  config.reconciliation_interval = 300  # seconds

  # ── Advanced rdkafka/WaterDrop overrides ─────────────────────────
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024"   }
end
```

### MySQL store

Requires two tables added by the bundled migrations:

| Table | Purpose |
|---|---|
| `kafka_batch_records` | One row per batch; holds counters and status |
| `kafka_batch_job_completions` | One row per completed job; provides the dedup unique constraint |

Run migrations:

```bash
bundle exec rails db:migrate
```

### Redis store

No migrations needed. Batch state is stored as a Redis Hash under the key `kafka_batch:b:{batch_id}`. Completed job IDs are tracked in a Set at `kafka_batch:b:{batch_id}:done_jobs`. Both keys expire after `config.batch_ttl` seconds.

> **Note:** Because Redis has no native range-scan on hash fields, the reconciler is a no-op with the Redis store. If you need reconciliation, keep a separate sorted set of batch IDs by `created_at` score and implement a custom sweep.

### Karafka routing

Add KafkaBatch routes inside your `karafka.rb`. Call `KafkaBatch.draw_routes` **after** all your worker classes have been loaded so the worker registry is populated:

```ruby
# karafka.rb
class KarafkaApp < Karafka::App
  setup do |config|
    config.kafka = { "bootstrap.servers" => ENV["KAFKA_BROKERS"] }
    config.client_id = "my-app"
    # ...
  end

  routes.draw do
    # Your own routes first
    topic "my_app.events" do
      consumer MyEventsConsumer
    end

    # KafkaBatch internal routes (events, callbacks) +
    # one route per registered KafkaBatch::Worker
    KafkaBatch.draw_routes(self)
  end
end
```

`draw_routes` registers three consumer groups:

| Group | Topic | Consumer |
|---|---|---|
| `kafka-batch-jobs` | each worker's `kafka_topic` | `JobConsumer` |
| `kafka-batch-events` | `kafka_batch.events` | `EventConsumer` |
| `kafka-batch-callbacks` | `kafka_batch.callbacks` | `CallbackConsumer` |

---

## Defining workers

Include `KafkaBatch::Worker` and implement `#perform`:

```ruby
class ProcessOrderWorker
  include KafkaBatch::Worker

  kafka_topic  "orders.process"   # required – Kafka topic to consume from
  max_retries  5                  # optional – overrides config.max_retries
  retry_backoff 10                # optional – overrides config.retry_backoff (seconds)

  # payload is the Hash you passed to Batch#push
  def perform(payload)
    order = Order.find(payload["order_id"])
    order.process!
    NotificationService.send_receipt(order)
  end
end
```

```ruby
class GenerateReportWorker
  include KafkaBatch::Worker

  kafka_topic "reports.generate"
  # max_retries and retry_backoff fall back to global config values

  def perform(payload)
    Report.generate(
      user_id:    payload["user_id"],
      date_range: payload["date_range"]
    )
  end
end
```

Workers are auto-registered when the class is loaded. Make sure your worker files are required/autoloaded before `KafkaBatch.draw_routes` is called (Rails autoloading handles this automatically).

---

## Creating batches

```ruby
batch_id = KafkaBatch::Batch.create(
  on_success:  "BatchSuccessCallback",   # called if ALL jobs succeed
  on_complete: "BatchCompleteCallback",  # called when ALL jobs finish (any status)
  meta: { report_id: 42, user_id: 99 }  # arbitrary data passed to callbacks
) do |b|
  Order.find_each do |order|
    b.push(ProcessOrderWorker, order_id: order.id)
  end
end

puts batch_id  # => "550e8400-e29b-41d4-a716-446655440000"
```

You can `push` as many jobs as needed inside the block. All jobs are buffered until the block returns; the batch record is written with the exact count before the first Kafka message is produced.

An optional explicit `job_id` can be supplied for tracing:

```ruby
b.push(ProcessOrderWorker, { order_id: 1 }, job_id: "order-1-process")
```

### Standalone jobs (no batch)

Enqueue a single job without batch context:

```ruby
KafkaBatch::Batch.enqueue(ProcessOrderWorker, order_id: 99)
```

The job goes through the same `JobConsumer` retry / DLT flow, but no batch completion tracking occurs.

---

## Callbacks

Callbacks are plain Ruby classes. Define the method matching the callback type:

```ruby
class BatchSuccessCallback
  # Called only when every job in the batch succeeded.
  # batch is a Hash with all batch fields.
  def on_success(batch)
    puts "Batch #{batch['batch_id']} finished perfectly!"
    puts "Processed #{batch['completed_count']} orders"

    AdminMailer.batch_complete(batch).deliver_later
  end
end

class BatchCompleteCallback
  # Called when all jobs are done regardless of individual failures.
  def on_complete(batch)
    if batch["failed_count"].positive?
      Sentry.capture_message(
        "Batch #{batch['batch_id']} had #{batch['failed_count']} failures",
        extra: batch
      )
    end

    # Always clean up
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
  "completed_count" => 1000,
  "failed_count"    => 0,
  "on_success"      => "BatchSuccessCallback",
  "on_complete"     => "BatchCompleteCallback",
  "meta"            => { "report_id" => 42 },
  "finished_at"     => "2024-01-15T10:30:00Z"
}
```

| Callback | When it fires |
|---|---|
| `on_success` | All jobs succeeded (`failed_count == 0`) |
| `on_complete` | All jobs finished, regardless of failures |

Both callbacks can be set on the same batch. If `on_complete` is set, it always fires. `on_success` only fires on a clean run.

---

## Retry behaviour

When a job raises an exception, the `JobConsumer` catches it and:

1. If `attempt < max_retries`: sleeps `attempt * retry_backoff` seconds, then re-enqueues the message to the same Kafka topic with `attempt + 1`. The message is committed so the consumer moves on immediately after re-enqueueing.
2. If `attempt >= max_retries`: emits a `failed` event to the events topic (which decrements the batch "OK" counter and increments "failed") and publishes the original message to the [Dead Letter Topic](#dead-letter-topic).

Backoff is linear: attempt 1 → 5s, attempt 2 → 10s, attempt 3 → 15s (with `retry_backoff: 5`).

Override per worker:

```ruby
class CriticalWorker
  include KafkaBatch::Worker
  kafka_topic   "critical.jobs"
  max_retries   10
  retry_backoff 30   # 30s, 60s, 90s, ...
end
```

---

## Dead Letter Topic

Jobs that exhaust all retries are forwarded to `kafka_batch.dead_letter` (configurable via `config.dead_letter_topic`). The DLT payload is the original job message augmented with:

```json
{
  "dlt_source_topic":  "orders.process",
  "dlt_error_class":   "ActiveRecord::RecordNotFound",
  "dlt_error_message": "Couldn't find Order with id=99",
  "dlt_at":            "2024-01-15T10:30:00Z"
}
```

Subscribe a separate consumer to `kafka_batch.dead_letter` in your `karafka.rb` to alert, log, or requeue manually:

```ruby
topic KafkaBatch.config.dead_letter_topic do
  consumer DeadLetterConsumer
end
```

---

## Reconciler

The reconciler detects batches stuck in `running` state (e.g. because a completion event was produced but the broker was unavailable before it was consumed). It re-checks the counters and re-fires the callback if the batch is actually complete.

Run it periodically:

```bash
bundle exec rake kafka_batch:reconcile
```

Schedule with cron, Whenever, or Karafka's scheduled messages:

```ruby
# config/schedule.rb (Whenever)
every 5.minutes do
  rake "kafka_batch:reconcile"
end
```

Or inside a Karafka scheduled consumer if you prefer to keep everything in Kafka.

---

## Rake tasks

| Task | Description |
|---|---|
| `kafka_batch:reconcile` | Run the stuck-batch reconciler |
| `kafka_batch:install_migrations` | Copy migrations to `db/migrate/` |
| `kafka_batch:workers` | Print all registered workers and their topics |

```bash
bundle exec rake kafka_batch:workers
#  ProcessOrderWorker   → topic: orders.process   retries: 5
#  GenerateReportWorker → topic: reports.generate retries: 3
```

---

## Migrating from Sidekiq Pro Batches

| Sidekiq Pro | kafka-batch |
|---|---|
| `Sidekiq::Batch.new` | `KafkaBatch::Batch.create` |
| `batch.jobs { MyWorker.perform_async(...) }` | `b.push(MyWorker, ...)` inside block |
| `batch.on(:success, MyCallback, ...)` | `on_success: "MyCallback"` parameter |
| `batch.on(:complete, MyCallback, ...)` | `on_complete: "MyCallback"` parameter |
| Callback class `#on_success(status)` | `#on_success(batch_hash)` |
| Callback class `#on_complete(status)` | `#on_complete(batch_hash)` |
| `status.bid` | `batch["batch_id"]` |
| `status.total` | `batch["total_jobs"]` |
| `status.failures` | `batch["failed_count"]` |

**What you don't need to change:**

- Callback class names and method names are the same (`on_success`, `on_complete`)
- Retry semantics are equivalent
- Batch metadata (`meta:` replaces Sidekiq batch description)

**Key difference:** Workers must `include KafkaBatch::Worker` and define a `kafka_topic`. They are consumed by Karafka rather than Sidekiq threads.

---

## Architecture deep-dive

### Why write the batch record before producing?

If we produced N messages and then wrote the batch record with `total_jobs: N`, a fast consumer could complete all N jobs and try to check completion before the record even exists — resulting in `not_found` and a lost callback. By writing the record first with the exact count, we guarantee the store is ready before any job can complete.

### Why use `SELECT FOR UPDATE` / Lua for the counter?

Without a lock, two EventConsumer threads could both read `completed = 99, total = 100`, both increment locally to 100, both conclude "we're done", and both publish the callback — firing it twice. The database-level lock ensures only one thread performs the final increment and observes the terminal state.

### Why is dedup separate from the counter?

The dedup table (MySQL) / Set (Redis) handles Kafka's at-least-once delivery. Without it, a re-delivered event message would increment the counter a second time and potentially push it past `total_jobs`, making the batch appear done when jobs are still running.

### Message flow (numbered)

```
1.  App            →  MySQL/Redis     CREATE batch record (total_jobs = N)
2.  App            →  Kafka jobs      PRODUCE N job messages
3.  JobConsumer    →  worker          CALL perform(payload)
4.  JobConsumer    →  Kafka events    PRODUCE {batch_id, job_id, status}
5.  EventConsumer  →  MySQL/Redis     ATOMIC increment + check
6.  EventConsumer  →  Kafka callbacks PRODUCE callback message (if done)
7.  CallbackConsumer → callback class CALL on_success / on_complete
```

### Consumer group topology

```
kafka-batch-jobs      ← all worker topics multiplexed into one group
kafka-batch-events    ← single topic, single group
kafka-batch-callbacks ← single topic, single group
```

---

## Topic reference

| Topic (default name) | Produced by | Consumed by | Purpose |
|---|---|---|---|
| `kafka_batch.jobs` | `Batch.create` / `Batch.enqueue` | `JobConsumer` | Individual job messages |
| `kafka_batch.events` | `JobConsumer` | `EventConsumer` | Job completion signals |
| `kafka_batch.callbacks` | `EventConsumer` | `CallbackConsumer` | Batch-complete triggers |
| `kafka_batch.dead_letter` | `JobConsumer` | Your consumer | Exhausted-retry jobs |

Worker topics are defined per-worker via `kafka_topic` and are also consumed by `JobConsumer`.

---

## Contributing

1. Fork the repo
2. `bundle install`
3. `bundle exec rspec`
4. Submit a PR

Please add tests for any new behaviour and keep the store interface (see `lib/kafka_batch/stores/base.rb`) in sync if you add a new store backend.

---

## License

MIT
