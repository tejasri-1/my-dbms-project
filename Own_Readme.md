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









































































