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

\copy online_retail FROM '/tmp/Online_Retail.csv' WITH (FORMAT csv, HEADER true);

# In a NEW terminal (client):
/usr/local/pgsql/bin/psql -d postgres -f /tmp/queries.sql

