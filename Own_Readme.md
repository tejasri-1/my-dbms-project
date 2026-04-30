For own "Initial Test" we implemented a simple logger that prints a message to the console every time a 
Sequential Scan occurs with an equality predicate

#include "utils/elog.h" // For logging

static TupleTableSlot *
SeqNext(SeqScanState *node)
{
    // Add this simple check for your initial test
    if (node->ss.ps.ps_ProjInfo == NULL) {
        // This is a simplified way to "detect" a scan start
        // In a real implementation, you'd check node->ss.ps.plan->qual
        elog(LOG, "Automatic Indexer: Sequential Scan detected on relation OID %u", 
             node->ss.ss_currentRelation->rd_id);
    }
    
    // ... existing code ...
}


-> configure the directory 
./configure --enable-cassert --enable-debug CFLAGS="-O0 -g"
-> Build and install
make -j 4             # Compiles the source (using 4 cores)
sudo make install     # Installs the binaries (usually to /usr/local/pgsql)

-> Initializing and starting the server

# Create a data directory
mkdir ~/my_test_db
/usr/local/pgsql/bin/initdb -D ~/my_test_db

# Start the server
/usr/local/pgsql/bin/postgres -D ~/my_test_db

if any postgres conflict with other 
sudo systemctl stop postgresql

# Step 1: Open a new terminal and connect

(Keep the server terminal running)

/usr/local/pgsql/bin/psql -d postgres

CREATE TABLE online_retail (
    invoiceno TEXT,
    stockcode TEXT,
    description TEXT,
    quantity INT,
    invoicedate TIMESTAMP,
    unitprice NUMERIC,
    customerid INT,
    country TEXT
);

SET max_parallel_workers_per_gather = 0;
SELECT * FROM online_retail WHERE CustomerID = 14911;

\copy online_retail FROM '/tmp/Online_Retail.csv' WITH (FORMAT csv, HEADER true);

# Optional: test a possible index with HypoPG

CREATE EXTENSION IF NOT EXISTS hypopg;

-- Give the planner table statistics after loading the CSV.
ANALYZE online_retail;

-- Current plan without any real index.
EXPLAIN SELECT * FROM online_retail WHERE customerid = 14911;

-- Create a hypothetical index. This does not build a real index on disk.
SELECT * FROM hypopg_create_index(
    'CREATE INDEX ON online_retail (customerid)'
);

-- Check whether PostgreSQL would use that index.
EXPLAIN SELECT * FROM online_retail WHERE customerid = 14911;

-- Remove all hypothetical indexes in this session.
SELECT hypopg_reset();

# Exhaustive HypoPG index advisor for all column combinations

Run this after creating `online_retail`, loading the CSV, and installing HypoPG
in the database.

python3 scripts/hypopg_index_advisor.py \
    --psql /usr/local/pgsql/bin/psql \
    --dbname postgres \
    --table public.online_retail \
    --queries queries.sql \
    --log hypopg_advisor.log \
    --jsonl hypopg_advisor_candidates.jsonl

This will:

1. run `CREATE EXTENSION IF NOT EXISTS hypopg`
2. run `ANALYZE online_retail`
3. log the baseline no-index cost for the workload
4. generate all non-empty btree index column combinations
5. create one hypothetical index at a time
6. run `EXPLAIN (FORMAT JSON)` for every query in `queries.sql`
7. log every candidate index and its total workload cost
8. choose the minimum-cost option, including the no-index baseline
9. create the real index only if an index beats the no-index baseline
10. run `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` for the queries and log timing

# In a NEW terminal (client):
/usr/local/pgsql/bin/psql -d postgres -f /tmp/queries.sql

# at the start of the session, for phase 1 

# Stage 5: internal live workload capture

Stage 5 removes the need to manually insert every query into
`public.auto_index_advisor_workload`. The extension now uses a planner hook to
capture live `SELECT` statements that mention the configured target table.

Install the rebuilt extension and restart PostgreSQL 14:

```bash
sudo make -C contrib/auto_index_advisor USE_PGXS=1 PG_CONFIG=/usr/bin/pg_config install
sudo pg_ctlcluster 14 main restart
```

Enable live capture and test-cleanup behavior:

```sql
ALTER SYSTEM SET auto_index_advisor.enabled = 'off';
ALTER SYSTEM SET auto_index_advisor.capture_workload = 'on';
ALTER SYSTEM SET auto_index_advisor.drop_indexes_before_run = 'on';
ALTER SYSTEM SET auto_index_advisor.drop_indexes_after_apply = 'on';
ALTER SYSTEM SET auto_index_advisor.auto_create = 'off';
ALTER SYSTEM SET auto_index_advisor.max_indexes_per_run = '1';
SELECT pg_reload_conf();
```

For this controlled test, `enabled = off` pauses the background worker while the
planner hook still captures the workload. `auto_index_advisor_run_test_cycle()`
uses a forced manual run, so it can still apply and clean up recommendations.

Run a clean test cycle:

```sql
SELECT auto_index_advisor_drop_indexes();
TRUNCATE public.auto_index_advisor_workload;
TRUNCATE public.auto_index_advisor_log;
```

Now execute the workload normally. These queries are captured automatically:

```bash
psql -d postgres -f queries.sql > /tmp/auto_index_advisor_queries.out
```

Verify Stage 5 captured the workload:

```sql
SELECT count(*) AS captured_queries
FROM public.auto_index_advisor_workload
WHERE enabled;

SELECT log_id, logged_at, event, message
FROM public.auto_index_advisor_log
ORDER BY log_id;
```

Run the advisor after all workload queries have finished:

```sql
SELECT auto_index_advisor_run_test_cycle();
SELECT auto_index_advisor_apply_recommendations();
SELECT auto_index_advisor_drop_indexes();
```

`auto_index_advisor_run_test_cycle()` refreshes/costs recommendations. The apply
and cleanup commands are intentionally separate SQL statements so PostgreSQL
executes Stage 4 create and cleanup in separate transactions.

Verify recommendations and cleanup:

```sql
SELECT column_name, improves_expected, create_status, created_index_name
FROM public.auto_index_advisor_recommendations
ORDER BY expected_with_index NULLS LAST;

SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'online_retail'
  AND indexname LIKE 'auto_advisor_%_idx';
```

Expected cleanup result: the final `pg_indexes` query should return zero rows
when `auto_index_advisor.drop_indexes_after_apply = on`.





































































