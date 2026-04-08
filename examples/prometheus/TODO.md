# Prometheus Observability — Missing Pieces

## Per-type counters on operations

**Problem:** `kharon_tickets_total{operation=...}` is global — can't answer "which ticket type is failing?" or "which type retries the most?"

**What we need:** `kharon_tickets_total{operation="failed", type="send-email"}` — the `type` label on acked/failed/retried/processed counters.

**How:** `processTicket` knows `t.Type` and sets up the context. Store type in context, extract it in `Ack`/`Fail`/`Retry`/etc., call `stats.IncTyped(metric, ticketType)`. Public API stays unchanged. Fallback label `type="external"` for calls outside processTicket (e.g. HTTP handlers).

**Dashboard impact:** enables per-type failure ratio, per-type retry ratio, per-type throughput breakdown.

## Queue depth gauge

**Problem:** we see add rate vs process rate, but don't know the absolute backlog size. A steady gap of +1/s for an hour means 3600 pending tickets, but you can't see that from rates alone.

**What we need:** `kharon_queue_depth{tube="default"}` — count of pending tickets.

**How:** either periodic `SELECT count(*) FROM tickets WHERE status='pending'` on a timer (expensive), or maintain an in-memory gauge: +1 on Put, -1 on Ack/Done/Delete/Cancel/Fail, +1 on Retry. The in-memory gauge drifts on restart but is cheap.

**Dashboard impact:** absolute backlog panel, alert on queue depth exceeding threshold.

## Per-type counters on the dashboard

Once per-type counters land, update the dashboard:
- "Failure Ratio by Type" — `rate(failed{type=X}) / rate(processed{type=X})`
- "Retry Ratio by Type" — same pattern
- "Throughput by Type" — stacked area of `rate(processed{type=X})`
- Grafana variable `$type` to filter all panels

## Alerting rules

Once the metrics are richer, add `examples/prometheus/alerts.yml`:
- `KharonHighFailureRate` — failure ratio > 10% for 5m
- `KharonRetryStorm` — retry ratio > 50% for 5m
- `KharonSlowProcessing` — p99 > SLO threshold for 5m
- `KharonBacklogGrowing` — add rate > process rate sustained for 10m
- `KharonQueueDepthHigh` — pending count > N (needs queue depth gauge)
