#include "postgres.h"

#include <math.h>

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "utils/auto_indexer.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* ---------- DATA STRUCTURES ---------- */

#define AUTO_INDEX_MIN_BENEFIT 1.0
#define AUTO_INDEX_MAINTENANCE_FRACTION 0.20

typedef struct
{
    Oid relid;
    int attno;
} AutoIndexKey;

typedef struct
{
    AutoIndexKey key;
    uint64 count;
    Cost total_benefit;
    Cost last_benefit;
    Cost estimated_create_cost;
    Cost estimated_maintenance_cost;
    Cost last_real_cost;
    Cost last_hypothetical_cost;
    bool recommended;
} AutoIndexEntry;

static HTAB *AutoIndexHash = NULL;

static bool AutoIndex_ExtractEqualityAttno(RestrictInfo *rinfo, Index relid,
                                           AttrNumber *attno,
                                           Selectivity *selectivity);
static Cost AutoIndex_EstimateHypotheticalScanCost(RelOptInfo *rel,
                                                   Selectivity selectivity);
static Cost AutoIndex_EstimateCreateCost(RelOptInfo *rel);
static Cost AutoIndex_CheapestRealPathCost(RelOptInfo *rel);
static const char *AutoIndex_BuildCreateIndexCommand(Oid relid, AttrNumber attno);

/* ---------- INIT ---------- */

void AutoIndex_Init(void)
{
    HASHCTL ctl;
    MemoryContext oldcontext;

    if (AutoIndexHash != NULL)
        return;

    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(AutoIndexKey);
    ctl.entrysize = sizeof(AutoIndexEntry);

    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    AutoIndexHash = hash_create("Auto Index Stats",
                                128,
                                &ctl,
                                HASH_ELEM | HASH_BLOBS);

    MemoryContextSwitchTo(oldcontext);
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

/* ---------- PLANNER COSTING ---------- */

void
AutoIndex_ConsiderRel(PlannerInfo *root, RelOptInfo *rel)
{
    ListCell   *lc;
    Cost        real_cost;

    if (rel == NULL || rel->rtekind != RTE_RELATION || rel->baserestrictinfo == NIL)
        return;

    /*
     * This prototype handles base-table scans.  The planner may also plan
     * DML, but index maintenance makes that accounting noisier, so start
     * with read queries.
     */
    if (root->parse == NULL || root->parse->commandType != CMD_SELECT)
        return;

    real_cost = AutoIndex_CheapestRealPathCost(rel);
    if (real_cost <= 0)
        return;

    foreach(lc, rel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        AttrNumber   attno;
        Selectivity  selectivity;
        Cost         hypothetical_cost;
        Cost         benefit;
        Cost         create_cost;
        Cost         maintenance_cost;
        AutoIndexKey key;
        AutoIndexEntry *entry;
        bool         found;

        if (!IsA(rinfo, RestrictInfo))
            continue;

        if (!AutoIndex_ExtractEqualityAttno(rinfo, rel->relid, &attno,
                                            &selectivity))
            continue;

        hypothetical_cost = AutoIndex_EstimateHypotheticalScanCost(rel,
                                                                   selectivity);
        benefit = real_cost - hypothetical_cost;
        if (benefit < AUTO_INDEX_MIN_BENEFIT)
            continue;

        create_cost = AutoIndex_EstimateCreateCost(rel);
        maintenance_cost = create_cost * AUTO_INDEX_MAINTENANCE_FRACTION;

        if (AutoIndexHash == NULL)
            AutoIndex_Init();

        key.relid = root->simple_rte_array[rel->relid]->relid;
        key.attno = attno;

        entry = (AutoIndexEntry *)
            hash_search(AutoIndexHash, &key, HASH_ENTER, &found);

        if (!found)
        {
            entry->count = 0;
            entry->total_benefit = 0;
            entry->recommended = false;
        }

        entry->count++;
        entry->total_benefit += benefit;
        entry->last_benefit = benefit;
        entry->estimated_create_cost = create_cost;
        entry->estimated_maintenance_cost = maintenance_cost;
        entry->last_real_cost = real_cost;
        entry->last_hypothetical_cost = hypothetical_cost;

        elog(LOG,
             "[AUTO-INDEXER] candidate rel=%u attno=%d count=%llu benefit=%.2f total_benefit=%.2f create_cost=%.2f maintenance_cost=%.2f",
             key.relid,
             attno,
             (unsigned long long) entry->count,
             benefit,
             entry->total_benefit,
             entry->estimated_create_cost,
             entry->estimated_maintenance_cost);

        if (!entry->recommended &&
            entry->total_benefit >= entry->estimated_create_cost +
                                    entry->estimated_maintenance_cost)
        {
            entry->recommended = true;
            elog(LOG,
                 "[AUTO-INDEXER] threshold reached; enqueue/create asynchronously: %s",
                 AutoIndex_BuildCreateIndexCommand(key.relid, attno));
        }
    }
}

static bool
AutoIndex_ExtractEqualityAttno(RestrictInfo *rinfo, Index relid,
                               AttrNumber *attno, Selectivity *selectivity)
{
    OpExpr     *op;
    Node       *left;
    Node       *right;
    Var        *var = NULL;
    char       *opname;

    if (!IsA(rinfo->clause, OpExpr))
        return false;

    op = (OpExpr *) rinfo->clause;
    if (list_length(op->args) != 2)
        return false;

    opname = get_opname(op->opno);
    if (opname == NULL || strcmp(opname, "=") != 0)
        return false;

    left = linitial(op->args);
    right = lsecond(op->args);

    if (IsA(left, Var))
        var = (Var *) left;
    else if (IsA(right, Var))
        var = (Var *) right;

    if (var == NULL || var->varno != relid || var->varattno <= 0)
        return false;

    *attno = var->varattno;

    if (rinfo->norm_selec >= 0)
        *selectivity = rinfo->norm_selec;
    else
        *selectivity = 0.10;

    if (*selectivity <= 0)
        *selectivity = 0.0001;
    if (*selectivity > 1)
        *selectivity = 1;

    return true;
}

static Cost
AutoIndex_EstimateHypotheticalScanCost(RelOptInfo *rel, Selectivity selectivity)
{
    double tuples_fetched = clamp_row_est(rel->tuples * selectivity);
    double heap_pages_fetched;
    double estimated_index_pages;

    estimated_index_pages = Max(1.0, ceil((double) rel->pages * 0.25));
    heap_pages_fetched = Min((double) rel->pages,
                             ceil((double) rel->pages * selectivity));

    return (estimated_index_pages * 0.05 * random_page_cost) +
           (heap_pages_fetched * random_page_cost) +
           (tuples_fetched * (cpu_index_tuple_cost + cpu_tuple_cost));
}

static Cost
AutoIndex_EstimateCreateCost(RelOptInfo *rel)
{
    double tuples = Max(rel->tuples, 2.0);

    return ((double) rel->pages * seq_page_cost) +
           (tuples * cpu_tuple_cost) +
           (tuples * log(tuples) / log(2.0) * cpu_operator_cost);
}

static Cost
AutoIndex_CheapestRealPathCost(RelOptInfo *rel)
{
    ListCell   *lc;
    Cost        best = -1;

    foreach(lc, rel->pathlist)
    {
        Path *path = (Path *) lfirst(lc);

        if (path->disabled_nodes > 0)
            continue;

        if (best < 0 || path->total_cost < best)
            best = path->total_cost;
    }

    return best;
}

static const char *
AutoIndex_BuildCreateIndexCommand(Oid relid, AttrNumber attno)
{
    char       *relname = get_rel_name(relid);
    char       *nspname = get_namespace_name(get_rel_namespace(relid));
    char       *attname = get_attname(relid, attno, false);

    if (relname == NULL || nspname == NULL || attname == NULL)
        return "CREATE INDEX CONCURRENTLY <unknown>";

    return psprintf("CREATE INDEX CONCURRENTLY ON %s.%s (%s)",
                    quote_identifier(nspname),
                    quote_identifier(relname),
                    quote_identifier(attname));
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

        elog(LOG,
             "Table: %s | Column: %s | Count: %llu | Total benefit: %.2f | Create+maintenance cost: %.2f | Recommended: %s",
             relname ? relname : "unknown",
             colname ? colname : "unknown",
             (unsigned long long) entry->count,
             entry->total_benefit,
             entry->estimated_create_cost + entry->estimated_maintenance_cost,
             entry->recommended ? "yes" : "no");
    }
}
