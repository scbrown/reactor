# Reactor

Real-time event processing for [Dolt](https://github.com/dolthub/dolt) databases.

Reactor watches Dolt databases via MySQL binlog streaming and dispatches
configurable reactions when data changes. Think "GitHub Actions, but for your
Dolt database" — when a row is inserted, updated, or deleted, reactor
classifies the change and routes it to configured destinations.

## Why?

Dolt is a version-controlled SQL database. Every transaction produces a
diffable commit. But there's no built-in way to **react** to changes in
real-time. Your options today:

| Approach | Latency | Complexity |
|----------|---------|------------|
| Poll `dolt_diff()` tables | Seconds to minutes | Low but wasteful |
| Debezium + Kafka | Sub-second | Very high (ZooKeeper, Kafka, Kafka Connect) |
| **Reactor** | Sub-second | **Low** (single Python process) |

Reactor uses [`python-mysql-replication`](https://github.com/julien-duponchelle/python-mysql-replication)
to consume Dolt's MySQL binlog stream directly. No message broker, no
infrastructure overhead — just a Python process that reads the binlog and
dispatches reactions.

## How It Works

```
┌──────────────────────────────────────────┐
│  Your Application (writes to Dolt)       │
└──────────────────┬───────────────────────┘
                   │ SQL writes
                   ▼
┌──────────────────────────────────────────┐
│  Dolt SQL Server (binlog enabled)        │
│  - Every transaction → binlog event      │
│  - Row-level change capture              │
└──────────────────┬───────────────────────┘
                   │ binlog stream
                   ▼
┌──────────────────────────────────────────┐
│  Reactor                                 │
│  ┌────────────┐  ┌───────────────────┐   │
│  │ Binlog     │→ │ Event Classifier  │   │
│  │ Consumer   │  │ (row changes →    │   │
│  │            │  │  semantic events)  │   │
│  └────────────┘  └────────┬──────────┘   │
│                           │              │
│  ┌────────────────────────▼──────────┐   │
│  │ Reaction Dispatcher               │   │
│  │ Config-driven rules:              │   │
│  │ • Webhooks (HTTP POST)            │   │
│  │ • Write to another Dolt table     │   │
│  │ • Shell commands                  │   │
│  │ • Custom Python handlers          │   │
│  └───────────────────────────────────┘   │
│                                          │
│  ┌───────────────────────────────────┐   │
│  │ State Tracker                     │   │
│  │ • GTID position (resume on crash) │   │
│  │ • Durable timers (deadlines)      │   │
│  │ • Deduplication                   │   │
│  └───────────────────────────────────┘   │
└──────────────────────────────────────────┘
         │              │
         ▼              ▼
┌──────────────┐ ┌──────────────┐
│ Webhook      │ │ Dolt table   │
│ endpoints    │ │ (event log)  │
└──────────────┘ └──────────────┘
```

## Features

- **Real-time**: Sub-second event delivery via MySQL binlog streaming
- **Lightweight**: Single Python process, no Kafka/ZooKeeper/Debezium
- **Config-driven**: Reaction rules in TOML — no code changes to add new reactions
- **Durable**: GTID position tracking survives restarts; picks up where it left off
- **Timers**: Schedule delayed reactions (e.g., "if no update in 24h, fire alert")
- **Event log**: Optionally write classified events back to Dolt for audit/query
- **Multi-database**: Watch multiple Dolt databases from a single reactor instance

## Dolt Configuration

Enable binlog on your Dolt SQL server:

```sql
SET @@PERSIST.log_bin = 1;
SET @@PERSIST.gtid_mode = ON;
SET @@PERSIST.enforce_gtid_consistency = ON;
SET @@PERSIST.binlog_row_metadata = FULL;
```

Restart `dolt sql-server` after setting these.

## Reaction Rules

Rules are defined in TOML:

```toml
[reactor]
dolt_host = "localhost"
dolt_port = 3306
dolt_user = "root"
server_id = 100  # unique binlog consumer ID

# Event classification: map table changes to semantic events
[[classify]]
database = "my_app"
table = "issues"
insert = "issue.created"
update = "issue.updated"
delete = "issue.deleted"

# Reaction rules
[[reactions]]
event = "issue.created"
condition = "priority <= 1"  # optional filter on row data
actions = [
    { type = "webhook", url = "https://hooks.example.com/urgent", method = "POST" },
    { type = "dolt-insert", database = "my_app", table = "event_log" },
]

[[reactions]]
event = "issue.updated"
condition = "old.status != new.status"
actions = [
    { type = "webhook", url = "https://hooks.example.com/status-change" },
]

[[reactions]]
event = "issue.created"
condition = "type == 'decision'"
timer = { delay = "24h", event = "decision.expired" }
actions = [
    { type = "webhook", url = "https://hooks.example.com/decision-needed" },
]
```

## Event Log

Reactor can write classified events to a Dolt table for downstream consumers:

```sql
CREATE TABLE reactor_events (
    id VARCHAR(26) PRIMARY KEY,       -- ULID (time-sortable)
    timestamp DATETIME(3) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    source_db VARCHAR(50),
    source_table VARCHAR(50),
    subject_id VARCHAR(50),
    actor VARCHAR(100),
    summary TEXT,
    payload JSON,
    INDEX idx_timestamp (timestamp),
    INDEX idx_event_type (event_type)
);
```

This gives you a queryable, version-controlled event stream — with full
`dolt_diff()` and `dolt_history()` support on the events themselves.

## Durable Timers

Reactor supports time-delayed reactions. When an event fires, you can schedule
a follow-up reaction that triggers after a delay — useful for deadlines,
escalation chains, and staleness detection.

Timer state is persisted to Dolt, so timers survive process restarts.

```sql
CREATE TABLE reactor_timers (
    id VARCHAR(26) PRIMARY KEY,
    subject_id VARCHAR(50) NOT NULL,
    timer_type VARCHAR(50) NOT NULL,
    fire_at DATETIME NOT NULL,
    reaction JSON NOT NULL,
    created_at DATETIME NOT NULL,
    INDEX idx_fire_at (fire_at)
);
```

## Use Cases

- **Issue tracker notifications**: New issue → Slack/Discord/email webhook
- **Decision queues**: Decision created → notify stakeholders, auto-expire after deadline
- **Audit trail**: Every data change → append to event log table
- **Staleness detection**: Record claimed → if not resolved in 24h → escalate
- **Alert routing**: Critical record created → page on-call
- **Dashboard updates**: Change stream → SSE/WebSocket → live dashboard
- **Cross-database sync**: Change in DB A → write to DB B

## Status

**Design phase.** See [docs/design.md](docs/design.md) for the full
architecture and implementation plan.

## Requirements

- Dolt SQL server with binlog enabled
- Python 3.10+
- `pymysqlreplication` library

## References

- [Using python-mysql-replication with Dolt](https://www.dolthub.com/blog/2024-08-08-python-mysql-replication-works-with-dolt/)
- [Dolt Change Data Capture](https://www.dolthub.com/blog/2023-03-01-change-data-capture/)
- [Debezium with Dolt](https://www.dolthub.com/blog/2024-07-19-debezium-works-with-dolt/)
- [Dolt Binlog Replication](https://docs.dolthub.com/guides/binlog-replication)

## License

MIT
