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

# Stage 5: online advisor path

Stage 5 uses the planner hook as the main online path. When query N reaches
planning, the advisor reads `public.auto_index_column_stats` from queries
1..N-1, chooses candidate columns, compares without-index cost against the best
with-index cost, creates or reuses the winning index before `standard_planner()`,
then PostgreSQL plans and executes query N. Only after query N finishes are its
column counters flushed into `public.auto_index_column_stats`.

`SELECT`, `UPDATE`, and `DELETE` can trigger the online advisor decision.
`INSERT` only updates stats after execution.

The actual GUC name is `auto_index_advisor.enabled`.

## Clean online workflow

These commands assume:

- PostgreSQL is installed under `/usr/local/pgsql`
- the test data directory is `$HOME/my_test_db`
- the server should run on port `5544`
- the workload file is `queries.sql`
- the target table is `public.online_retail`

Build and install the changed PostgreSQL code and extension:

```bash
cd ~/Desktop/postgres
export PATH=/usr/local/pgsql/bin:$PATH
export PGDATA=$HOME/my_test_db

make -j"$(nproc)"
sudo make install

make -C contrib/auto_index_advisor
sudo make -C contrib/auto_index_advisor install
```

Make sure the advisor is preloaded, then restart the test server:

```bash
grep -q "shared_preload_libraries.*auto_index_advisor" "$PGDATA/postgresql.conf" || \
  echo "shared_preload_libraries = 'auto_index_advisor'" >> "$PGDATA/postgresql.conf"

pg_ctl -D "$PGDATA" stop -m fast || true
pg_ctl -D "$PGDATA" -l "$PGDATA/server.log" -o "-p 5544" start
```

Create or upgrade the extensions:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
CREATE EXTENSION IF NOT EXISTS hypopg;
CREATE EXTENSION IF NOT EXISTS auto_index_advisor;
ALTER EXTENSION auto_index_advisor UPDATE;
SQL
```

Keep the online advisor on and keep the batch apply path off:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
ALTER SYSTEM SET auto_index_advisor.enabled = 'on';
ALTER SYSTEM SET auto_index_advisor.capture_workload = 'on';
ALTER SYSTEM SET auto_index_advisor.enable_hypopg_costing = 'on';
ALTER SYSTEM SET auto_index_advisor.auto_create = 'off';
ALTER SYSTEM SET auto_index_advisor.drop_indexes_before_run = 'off';
ALTER SYSTEM SET auto_index_advisor.drop_indexes_after_apply = 'off';
ALTER SYSTEM SET auto_index_advisor.target_table = 'public.online_retail';
ALTER SYSTEM SET auto_index_advisor.query_decision_log_file_path = '/tmp/auto_index_advisor_query_decisions.log';
SELECT pg_reload_conf();
SQL
```

If the table is not loaded yet, load it once:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
CREATE TABLE IF NOT EXISTS public.online_retail (
    invoiceno text,
    stockcode text,
    description text,
    quantity int,
    invoicedate timestamp,
    unitprice numeric,
    customerid int,
    country text
);

SET datestyle = 'ISO, DMY';
\copy public.online_retail FROM './Online_Retail.csv' WITH (FORMAT csv, HEADER true)
ANALYZE public.online_retail;
SQL
```

Before each fresh workload run, clear old advisor-created indexes, stats, and
batch tables:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
DO $$
DECLARE
    r record;
BEGIN
    IF to_regclass('public.online_retail') IS NULL THEN
        RAISE NOTICE 'target table public.online_retail does not exist; skipping advisor index cleanup';
        RETURN;
    END IF;

    FOR r IN
        SELECT format('%I.%I', ns.nspname, i.relname) AS index_name
        FROM pg_index x
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_namespace ns ON ns.oid = i.relnamespace
        WHERE t.oid = 'public.online_retail'::regclass
          AND i.relname LIKE 'auto_advisor_%_idx'
    LOOP
        EXECUTE 'DROP INDEX IF EXISTS ' || r.index_name;
        RAISE NOTICE 'dropped advisor index %', r.index_name;
    END LOOP;
END $$;

DO $$
DECLARE
    tbl text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'public.auto_index_column_stats',
        'public.auto_index_advisor_workload',
        'public.auto_index_advisor_log',
        'public.auto_index_advisor_recommendations'
    ]
    LOOP
        IF to_regclass(tbl) IS NOT NULL THEN
            EXECUTE format('TRUNCATE TABLE %s', tbl);
            RAISE NOTICE 'truncated %', tbl;
        ELSE
            RAISE NOTICE 'skipped %, table does not exist', tbl;
        END IF;
    END LOOP;
END $$;
SQL
```

Clear the per-query log file after cleanup, so cleanup queries do not appear in
the workload log:

```bash
rm -f /tmp/auto_index_advisor_query_decisions.log
```

Refresh table statistics before running the workload:

```bash
psql -h localhost -p 5544 -d postgres -c "ANALYZE public.online_retail;" \
  > /tmp/auto_index_analyze.out 2>&1
head -n 5 /tmp/auto_index_analyze.out
```

Run the workload. The online advisor runs before each eligible query is planned:

```bash
psql -h localhost -p 5544 -d postgres -f queries.sql \
  > /tmp/auto_index_queries.out 2>&1
head -n 5 /tmp/auto_index_queries.out
```

The full command output is saved in:

```text
/tmp/auto_index_analyze.out
/tmp/auto_index_queries.out
```

Read the per-query decisions:

```bash
cat /tmp/auto_index_advisor_query_decisions.log
```

Check the final stats and advisor-created indexes:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
SELECT table_schema, table_name, column_name,
       access_count, update_count, insert_count, delete_count, total_count
FROM public.auto_index_column_stats
ORDER BY table_schema, table_name, column_name;

SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'online_retail'
  AND indexname LIKE 'auto_advisor_%_idx'
ORDER BY indexname;
SQL
```

Optional server log view:

```bash
tail -n 100 "$PGDATA/server.log"
```




---final code:
Run these from `~/Desktop/postgres` in order.

```bash
cd ~/Desktop/postgres
export PATH=/usr/local/pgsql/bin:$PATH
export PGDATA=$HOME/my_test_db
```

```bash
make -j"$(nproc)"
sudo make install
make -C contrib/auto_index_advisor
sudo make -C contrib/auto_index_advisor install
```

```bash
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  initdb -D "$PGDATA"
fi
```

```bash
grep -q "shared_preload_libraries.*auto_index_advisor" "$PGDATA/postgresql.conf" || \
  echo "shared_preload_libraries = 'auto_index_advisor'" >> "$PGDATA/postgresql.conf"

pg_ctl -D "$PGDATA" stop -m fast || true
pg_ctl -D "$PGDATA" -l "$PGDATA/server.log" -o "-p 5544" start
```

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
CREATE EXTENSION IF NOT EXISTS hypopg;
CREATE EXTENSION IF NOT EXISTS auto_index_advisor;
ALTER EXTENSION auto_index_advisor UPDATE;

ALTER SYSTEM SET auto_index_advisor.enabled = 'on';
ALTER SYSTEM SET auto_index_advisor.capture_workload = 'on';
ALTER SYSTEM SET auto_index_advisor.enable_hypopg_costing = 'on';
ALTER SYSTEM SET auto_index_advisor.auto_create = 'off';
ALTER SYSTEM SET auto_index_advisor.drop_indexes_before_run = 'off';
ALTER SYSTEM SET auto_index_advisor.drop_indexes_after_apply = 'off';
ALTER SYSTEM SET auto_index_advisor.target_table = 'public.online_retail';
ALTER SYSTEM SET auto_index_advisor.query_decision_log_file_path = '/tmp/auto_index_advisor_query_decisions.log';
SELECT pg_reload_conf();
SQL
```

Only if you want to reload the CSV fresh:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
DROP TABLE IF EXISTS public.online_retail;

CREATE TABLE public.online_retail (
    invoiceno text,
    stockcode text,
    description text,
    quantity int,
    invoicedate timestamp,
    unitprice numeric,
    customerid int,
    country text
);

SET datestyle = 'ISO, DMY';
\copy public.online_retail FROM './Online_Retail.csv' WITH (FORMAT csv, HEADER true)

ANALYZE public.online_retail;
SQL
```

Cleanup before running workload:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT format('%I.%I', ns.nspname, i.relname) AS index_name
        FROM pg_index x
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_namespace ns ON ns.oid = i.relnamespace
        WHERE t.oid = 'public.online_retail'::regclass
          AND i.relname LIKE 'auto_advisor_%_idx'
    LOOP
        EXECUTE 'DROP INDEX IF EXISTS ' || r.index_name;
        RAISE NOTICE 'dropped advisor index %', r.index_name;
    END LOOP;
END $$;

DO $$
DECLARE
    tbl text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'public.auto_index_column_stats',
        'public.auto_index_advisor_workload',
        'public.auto_index_advisor_log',
        'public.auto_index_advisor_recommendations'
    ]
    LOOP
        IF to_regclass(tbl) IS NOT NULL THEN
            EXECUTE format('TRUNCATE TABLE %s', tbl);
            RAISE NOTICE 'truncated %', tbl;
        END IF;
    END LOOP;
END $$;
SQL
```

```bash
rm -f /tmp/auto_index_advisor_query_decisions.log
```

Run analyze and workload, saving full output but showing only first 5 lines:

```bash
psql -h localhost -p 5544 -d postgres -c "ANALYZE public.online_retail;" \
  > /tmp/auto_index_analyze.out 2>&1
head -n 5 /tmp/auto_index_analyze.out
```

```bash
psql -h localhost -p 5544 -d postgres -f queries2.sql \
  > /tmp/auto_index_queries.out 2>&1
head -n 5 /tmp/auto_index_queries.out
```

View advisor decision log:

```bash
cat /tmp/auto_index_advisor_query_decisions.log
```

Check final stats and indexes:

```bash
psql -h localhost -p 5544 -d postgres <<'SQL'
SELECT table_schema, table_name, column_name,
       access_count, update_count, insert_count, delete_count, total_count
FROM public.auto_index_column_stats
ORDER BY table_schema, table_name, column_name;

SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'online_retail'
  AND indexname LIKE 'auto_advisor_%_idx'
ORDER BY indexname;
SQL
```


Because we registered the write summary using on_proc_exit() inside the executor hook, any PostgreSQL process that runs executor work and then exits can append a summary. That includes short-lived worker processes, especially parallel query workers or background-worker style processes. 





























































