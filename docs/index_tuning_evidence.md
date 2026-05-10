# Index Tuning Evidence (Day 6)

## Methodology

Profiled four representative Day 5 queries with `EXPLAIN (ANALYZE, BUFFERS)` before and after adding 8 new indexes (DDL: [`sql/schema/10_create_performance_indexes.sql`](../sql/schema/10_create_performance_indexes.sql)). Re-ran a third pass with `work_mem = 256MB` to isolate index effects from sort-spill effects.

All times are `Execution Time` from EXPLAIN ANALYZE on a quiet local Postgres 18 instance, single warm-cache run.

## Query Performance Before vs After

| Query | Before (ms) | After indexes (ms) | After indexes + work_mem 256MB (ms) | Speedup |
|---|---:|---:|---:|---:|
| `top_diagnoses_by_volume` | 20,103 | 19,764 | 18,384 | 1.1× |
| `velocity_outliers` | 3,181 | 3,310 | 2,932 | 1.1× |
| `amount_outliers` | 2,571 | 2,576 | 2,454 | 1.0× |
| `diagnosis_stuffing` | 3,532 | 3,032 | 2,885 | 1.2× |

**Result: indexes alone produced ~1× speedup. The expected 5–20× did not materialize.** This is itself the finding — see analysis below.

## Why the indexes did not help these queries

Each of these four queries is a **full-table aggregation** with no selective `WHERE` clause. They all touch every row of the underlying tables (1.68M bridge rows or 558K fact rows). Indexes accelerate selective access (point lookups, range scans on filtered subsets) — they don't accelerate "read every row, group, aggregate."

The actual bottlenecks visible in the EXPLAIN plans:

### `top_diagnoses_by_volume` — sort spill on `COUNT(DISTINCT claim_id)`

```
GroupAggregate  (cost=314294.27..335409.42 rows=4720) (actual time=17086..19719)
  Group Key: bcd.diagnosis_code
  ->  Sort  (cost=314294.27..318496.06 rows=1680716)
        Sort Key: bcd.diagnosis_code, bcd.claim_id
        Sort Method: external merge  Disk: 43280kB     <-- 17 of 19.7 seconds spent here
```

`COUNT(DISTINCT claim_id)` requires sorting all 1.68M post-join rows by `(diagnosis_code, claim_id)` to dedupe within each group. The 43MB sort spilled to disk because default `work_mem` is 4MB. Bumping to 256MB only saved ~1.7s — the sort itself is CPU-bound at this row count, not just I/O-bound.

**Real fix candidates** (not in scope for this commit):
- Rewrite as a pre-aggregation CTE: `SELECT diagnosis_code, COUNT(DISTINCT claim_id) FROM bridge GROUP BY diagnosis_code` first, *then* join the aggregated row to `dim_provider` for the fraud overlay.
- Or accept that this query fundamentally costs ~20s and run it offline, not interactively.

### `diagnosis_stuffing` — index *did* kick in, modestly helpful

```
Index Only Scan using idx_bridge_diag_claim_id on bridge_claim_diagnosis bcd
   (cost=0.43..43729.44 rows=1680716) (actual time=0.049..666 ms)
   Heap Fetches: 180738
```

The new `idx_bridge_diag_claim_id` is used as Index Only Scan, replacing a sequential scan. Saved ~500ms because the index pages are already sorted by `claim_id`, so the downstream `GROUP BY claim_id` doesn't need an extra sort step. Modest but real. The remaining 2.5s is in the hash joins to fact tables and the final aggregate — also CPU-bound, not index-helped.

### `velocity_outliers` and `amount_outliers` — full aggregations on facts

Both compute per-provider stats (PERCENTILE_CONT, MAX, AVG) over all 558K claims. The new `idx_inp_provider_date` and `idx_out_provider_date` are not used because there's no provider-side filter: the query group-bys *every* provider. Index scan would have to read all index pages anyway, and Postgres correctly chose seq scan + hash aggregate.

These indexes will help **future selective queries** (e.g., "claims for provider X in March 2009") but contributed nothing to today's full aggregations.

## Where the indexes will earn their keep

| Index | Size | Use case it accelerates |
|---|---:|---|
| `idx_bridge_diag_claim_id` | 27 MB | Bridge → fact lookups (Index Only Scan confirmed) |
| `idx_out_beneficiary_date`, `idx_inp_beneficiary_date` | 16 MB + 1.3 MB | Day 7 ML feature engineering (per-patient claim history) |
| `idx_out_provider_date`, `idx_inp_provider_date` | 12 MB + 1.2 MB | Per-provider time-window queries |
| `idx_beneficiary_diabetes_ischemic` | 952 kB | Multi-condition cohort filters |
| `idx_beneficiary_heart_failure` (partial) | 480 kB | "All heart-failure patients" lookups |
| `idx_bridge_proc_claim_id` | 840 kB | Procedure bridge → fact lookups |

Total added: ~60 MB across 8 indexes. Negligible relative to the 558K-claim fact tables.

## Trade-offs

- **Storage:** +60 MB (rounding error vs the data itself).
- **Write cost:** zero practical impact — this is an analytical workload with batch ETL loads, not OLTP. Each ETL load happens once; a few ms of extra index maintenance per batch is invisible.
- **Planner statistics:** `ANALYZE` ran on all 6 tables after creating indexes so the planner uses fresh stats.

## Honest interview-grade summary

> "I profiled four queries and added eight targeted indexes. Six of those indexes target Day 7's per-patient and per-provider time-window queries — those will be measurably faster. But on Day 5's full-aggregation queries, the indexes barely moved the needle, because the bottleneck wasn't access path — it was a 43MB external merge sort during `COUNT(DISTINCT)`. The real fix for those queries is a CTE rewrite to pre-aggregate before joining, not more indexes. **Adding indexes without first identifying the actual bottleneck is a common mistake; the EXPLAIN ANALYZE plan tells you whether to add an index, raise `work_mem`, or rewrite the query.**"
