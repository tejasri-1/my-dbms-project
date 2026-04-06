#include "postgres.h"
#include "utils/hsearch.h"
#include "utils/elog.h"
#include "catalog/pg_class.h"
#include "utils/lsyscache.h"

/* ---------- DATA STRUCTURES ---------- */

typedef struct
{
    Oid relid;
    int attno;
} AutoIndexKey;

typedef struct
{
    AutoIndexKey key;
    int count;
} AutoIndexEntry;

static HTAB *AutoIndexHash = NULL;

/* ---------- INIT ---------- */

void AutoIndex_Init(void)
{
    HASHCTL ctl;

    if (AutoIndexHash != NULL)
        return;

    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(AutoIndexKey);
    ctl.entrysize = sizeof(AutoIndexEntry);

    AutoIndexHash = hash_create("Auto Index Stats",
                                128,
                                &ctl,
                                HASH_ELEM | HASH_BLOBS);
}

/* ---------- UPDATE ---------- */

void AutoIndex_Update(Oid relid, int attno)
{
    bool found;
    AutoIndexKey key;
    AutoIndexEntry *entry;

    if (AutoIndexHash == NULL)
        AutoIndex_Init();

    key.relid = relid;
    key.attno = attno;

    entry = (AutoIndexEntry *)
        hash_search(AutoIndexHash, &key, HASH_ENTER, &found);

    if (!found)
        entry->count = 0;

    entry->count++;
}

/* ---------- PRINT ---------- */

void AutoIndex_Print(void)
{
    HASH_SEQ_STATUS status;
    AutoIndexEntry *entry;

    if (AutoIndexHash == NULL)
        return;

    elog(LOG, "====== AUTO INDEX STATS ======");

    hash_seq_init(&status, AutoIndexHash);

    while ((entry = (AutoIndexEntry *) hash_seq_search(&status)) != NULL)
    {
        char *relname = get_rel_name(entry->key.relid);
        char *colname = get_attname(entry->key.relid, entry->key.attno, false);

        elog(LOG, "Table: %s | Column: %s | Count: %d",
             relname ? relname : "unknown",
             colname ? colname : "unknown",
             entry->count);
    }
}