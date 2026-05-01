#include "postgres.h"

#include <math.h>

#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/spi.h"
#include "nodes/parsenodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "lib/stringinfo.h"
#include "tcop/tcopprot.h"
#include "utils/auto_indexer.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

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
    uint64 access_count;
    uint64 insert_count;
    uint64 update_count;
    uint64 delete_count;
    Cost total_benefit;
    Cost last_benefit;
    Cost estimated_create_cost;
    Cost estimated_maintenance_cost;
    Cost last_real_cost;
    Cost last_hypothetical_cost;
    bool recommended;
} AutoIndexEntry;

typedef struct
{
    AutoIndexKey key;
    uint64 access_count;
    uint64 insert_count;
    uint64 update_count;
    uint64 delete_count;
} AutoIndexTouchEntry;

typedef struct
{
    Index relid;
    Var *var;
} AutoIndexPredicateVarContext;

static HTAB *AutoIndexHash = NULL;
static HTAB *AutoIndexQueryTouches = NULL;
static bool AutoIndexQueryActive = false;
static bool AutoIndexQueryHeaderPrinted = false;
static uint64 AutoIndexQueryCounter = 0;
static char *AutoIndexCurrentQueryString = NULL;
static int AutoIndexCurrentCommandType = CMD_UNKNOWN;
static int AutoIndexInternalQueryDepth = 0;
static bool AutoIndexStatsTableEnsured = false;

static bool AutoIndex_ExtractPredicateAttno(RestrictInfo *rinfo, Index relid,
                                            AttrNumber *attno,
                                            Selectivity *selectivity);
static bool AutoIndex_FindPredicateVar(Node *node, void *context);
static Cost AutoIndex_EstimateHypotheticalScanCost(RelOptInfo *rel,
                                                   Selectivity selectivity);
static Cost AutoIndex_EstimateCreateCost(RelOptInfo *rel);
static Cost AutoIndex_CheapestRealPathCost(RelOptInfo *rel);
static const char *AutoIndex_BuildCreateIndexCommand(Oid relid, AttrNumber attno);
static const char *AutoIndex_CommandTypeName(int command_type);
static bool AutoIndex_HasSingleColumnIndex(RelOptInfo *rel, AttrNumber attno);
static bool AutoIndex_IsInternalQuery(void);
static bool AutoIndex_ShouldIgnoreRelid(Oid relid);
static uint64 AutoIndex_TotalTouchCount(void);
static void AutoIndex_InitEntry(AutoIndexEntry *entry);
static void AutoIndex_ResetQueryTouches(void);
static void AutoIndex_EnsureQueryTouches(void);
static void AutoIndex_EnsureStatsTable(void);
static double AutoIndex_GetDistinctCount(Oid relid, const char *attname);
static void AutoIndex_FlushQueryTouches(void);
static void AutoIndex_LogFormula(void);
static void AutoIndex_LogQueryHeader(RelOptInfo *rel);
static void AutoIndex_ResetQueryState(void);

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

static void
AutoIndex_EnsureQueryTouches(void)
{
    HASHCTL ctl;
    MemoryContext oldcontext;

    if (AutoIndexQueryTouches != NULL)
        return;

    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(AutoIndexKey);
    ctl.entrysize = sizeof(AutoIndexTouchEntry);

    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    AutoIndexQueryTouches = hash_create("Auto Index Query Touches",
                                        128,
                                        &ctl,
                                        HASH_ELEM | HASH_BLOBS);

    MemoryContextSwitchTo(oldcontext);
}

//Forget the column touches from the previous/current query and start fresh.
static void
AutoIndex_ResetQueryTouches(void)
{
    if (AutoIndexQueryTouches != NULL)
    {
        hash_destroy(AutoIndexQueryTouches);
        AutoIndexQueryTouches = NULL;
    }
}

//Starts per-query tracking. It stores the SQL text, command type, increments a query counter, resets per-query touches, and logs the formula.

void
AutoIndex_BeginQuery(const char *query_string, int command_type)
{
    if (AutoIndex_IsInternalQuery())
        return;

    if (AutoIndexCurrentQueryString != NULL)
    {
        pfree(AutoIndexCurrentQueryString);
        AutoIndexCurrentQueryString = NULL;
    }

    AutoIndexCurrentQueryString = (query_string != NULL && query_string[0] != '\0') ?
        MemoryContextStrdup(TopMemoryContext, query_string) : NULL;
    AutoIndexCurrentCommandType = command_type;
    AutoIndexQueryActive = true;
    AutoIndexQueryHeaderPrinted = false;
    AutoIndexQueryCounter++;
    AutoIndex_ResetQueryTouches();

    elog(LOG, "Online HypoPG advisor run: %s",
         timestamptz_to_str(GetCurrentTimestamp()));
    elog(LOG, "QUERY_BEGIN number=%llu sql=%s",
         (unsigned long long) AutoIndexQueryCounter,
         AutoIndexCurrentQueryString ? AutoIndexCurrentQueryString : "");
    elog(LOG, "QUERY_KIND kind=%s",
         AutoIndex_CommandTypeName(AutoIndexCurrentCommandType));
    AutoIndex_LogFormula();
}

/* ---------- UPDATE ---------- */

void AutoIndex_Update(Oid relid, int attno)
{
    AutoIndex_RecordTouch(relid, attno, AUTO_INDEX_TOUCH_ACCESS);
}

void
AutoIndex_RecordTouch(Oid relid, int attno, AutoIndexTouchKind kind)
{
    bool found;
    AutoIndexKey key;
    AutoIndexEntry *entry;
    bool touch_found;
    AutoIndexTouchEntry *touch_entry;
    const char *kind_name;
    bool duplicate_query_touch = false;

    if (AutoIndex_IsInternalQuery() ||
        AutoIndex_ShouldIgnoreRelid(relid) ||
        attno <= 0)
        return;

    if (AutoIndexHash == NULL)
        AutoIndex_Init();

    AutoIndex_EnsureQueryTouches();

    key.relid = relid;
    key.attno = attno;

    entry = (AutoIndexEntry *)
        hash_search(AutoIndexHash, &key, HASH_ENTER, &found);

    if (!found)
        AutoIndex_InitEntry(entry);

    touch_entry = (AutoIndexTouchEntry *)
        hash_search(AutoIndexQueryTouches, &key, HASH_ENTER, &touch_found);
    if (!touch_found)
    {
        touch_entry->access_count = 0;
        touch_entry->insert_count = 0;
        touch_entry->update_count = 0;
        touch_entry->delete_count = 0;
    }

    kind_name = "access";
    switch (kind)
    {
        case AUTO_INDEX_TOUCH_INSERT:
            duplicate_query_touch = touch_entry->insert_count > 0;
            kind_name = "insert";
            break;
        case AUTO_INDEX_TOUCH_UPDATE:
            duplicate_query_touch = touch_entry->update_count > 0;
            kind_name = "update";
            break;
        case AUTO_INDEX_TOUCH_DELETE:
            duplicate_query_touch = touch_entry->delete_count > 0;
            kind_name = "delete";
            break;
        case AUTO_INDEX_TOUCH_ACCESS:
        default:
            duplicate_query_touch = touch_entry->access_count > 0;
            kind_name = "access";
            break;
    }

    if (duplicate_query_touch)
    {
        elog(LOG,
             "STATS_TOUCH_SKIPPED query_number=%llu relid=%u attno=%d kind=%s reason=already_touched_in_query",
             (unsigned long long) AutoIndexQueryCounter,
             relid,
             attno,
             kind_name);
        return;
    }

    switch (kind)
    {
        case AUTO_INDEX_TOUCH_INSERT:
            touch_entry->insert_count++;
            kind_name = "insert";
            break;
        case AUTO_INDEX_TOUCH_UPDATE:
            touch_entry->update_count++;
            kind_name = "update";
            break;
        case AUTO_INDEX_TOUCH_DELETE:
            touch_entry->delete_count++;
            kind_name = "delete";
            break;
        case AUTO_INDEX_TOUCH_ACCESS:
        default:
            touch_entry->access_count++;
            kind_name = "access";
            break;
    }

    elog(LOG,
         "STATS_TOUCH_RECORDED query_number=%llu relid=%u attno=%d kind=%s pending_until=query_end",
         (unsigned long long) AutoIndexQueryCounter,
         relid,
         attno,
         kind_name);
}

void
AutoIndex_ObserveSeqScan(Oid relid)
{
    char *relname;

    if (AutoIndex_IsInternalQuery() || AutoIndex_ShouldIgnoreRelid(relid))
        return;

    relname = get_rel_name(relid);
    elog(LOG, "SEQSCAN_OBSERVATION relid=%u relname=%s",
         relid,
         relname ? relname : "unknown");
}

/* ---------- PLANNER COSTING ---------- */

void
AutoIndex_ConsiderRel(PlannerInfo *root, RelOptInfo *rel)
{
    ListCell   *lc;
    Cost        real_cost;
    RangeTblEntry *rte;
    char       *relname;
    char       *nspname;
    StringInfoData column_names;
    int         total_columns;
    bool        saw_candidate = false;

    if (AutoIndex_IsInternalQuery() ||
        rel == NULL ||
        rel->rtekind != RTE_RELATION ||
        rel->baserestrictinfo == NIL)
        return;

    /*
     * This prototype handles base-table scans.  We also account for UPDATE
     * and DELETE predicates here because they use the same scan machinery.
     */
    if (root->parse == NULL ||
        (root->parse->commandType != CMD_SELECT &&
         root->parse->commandType != CMD_UPDATE &&
         root->parse->commandType != CMD_DELETE))
        return;

    rte = root->simple_rte_array[rel->relid];
    if (rte == NULL || AutoIndex_ShouldIgnoreRelid(rte->relid))
        return;

    relname = get_rel_name(rte != NULL ? rte->relid : InvalidOid);
    nspname = get_namespace_name(get_rel_namespace(rte != NULL ? rte->relid : InvalidOid));
    total_columns = (rte != NULL && rte->eref != NULL) ? list_length(rte->eref->colnames) : 0;

    if (AutoIndexQueryActive && !AutoIndexQueryHeaderPrinted)
    {
        AutoIndex_LogQueryHeader(rel);
        initStringInfo(&column_names);
        foreach(lc, rel->baserestrictinfo)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
            AttrNumber   attno;
            Selectivity  selectivity;
            char        *attname;

            if (!IsA(rinfo, RestrictInfo))
                continue;
            if (!AutoIndex_ExtractPredicateAttno(rinfo, rel->relid, &attno,
                                                 &selectivity))
                continue;
            attname = get_attname(rte != NULL ? rte->relid : InvalidOid,
                                  attno, false);
            if (attname != NULL)
                appendStringInfoString(&column_names, column_names.len > 0 ? "," : "");
            if (attname != NULL)
                appendStringInfoString(&column_names, attname);
        }
        elog(LOG,
             "table=%s.%s rows=%.0f columns=[%s]",
             nspname ? nspname : "unknown",
             relname ? relname : "unknown",
             (double) rel->tuples,
             column_names.data ? column_names.data : "");
        elog(LOG,
             "catalog_stats relpages=%.0f reltuples=%.0f",
             (double) rel->pages,
             (double) rel->tuples);
        elog(LOG,
             "planner_cost_settings={'cpu_operator_cost': %.4f, 'cpu_tuple_cost': %.4f, 'random_page_cost': %.4f, 'seq_page_cost': %.4f}",
             cpu_operator_cost,
             cpu_tuple_cost,
             random_page_cost,
             seq_page_cost);
        elog(LOG,
             "thresholds min_access_fraction_floor=%.2f dynamic_access_fraction=%.4f min_distinct_ratio=%.2f",
             0.05,
             Max(0.05, total_columns > 0 ? (2.0 / (double) total_columns) : 0.05),
             0.0);
        AutoIndexQueryHeaderPrinted = true;
    }

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
        Cost         expected_without_index;
        Cost         expected_with_index;
        bool         with_index_wins;
        uint64       total_touches;
        AutoIndexKey key;
        AutoIndexEntry *entry;
        bool         found;
        bool         has_real_index;
        const char *attname;

        if (!IsA(rinfo, RestrictInfo))
            continue;

        if (!AutoIndex_ExtractPredicateAttno(rinfo, rel->relid, &attno,
                                             &selectivity))
            continue;
        AutoIndex_RecordTouch(rte != NULL ? rte->relid : InvalidOid,
                              attno,
                              root->parse->commandType == CMD_UPDATE ?
                              AUTO_INDEX_TOUCH_UPDATE :
                              root->parse->commandType == CMD_DELETE ?
                              AUTO_INDEX_TOUCH_DELETE :
                              AUTO_INDEX_TOUCH_ACCESS);

        hypothetical_cost = AutoIndex_EstimateHypotheticalScanCost(rel,
                                                                   selectivity);
        benefit = real_cost - hypothetical_cost;
        create_cost = AutoIndex_EstimateCreateCost(rel);
        maintenance_cost = create_cost * AUTO_INDEX_MAINTENANCE_FRACTION;
        attname = get_attname(rte != NULL ? rte->relid : InvalidOid, attno, false);
        has_real_index = AutoIndex_HasSingleColumnIndex(rel, attno);
        saw_candidate = true;

        if (AutoIndexHash == NULL)
            AutoIndex_Init();

        key.relid = root->simple_rte_array[rel->relid]->relid;
        key.attno = attno;

        entry = (AutoIndexEntry *)
            hash_search(AutoIndexHash, &key, HASH_ENTER, &found);

        if (!found)
            AutoIndex_InitEntry(entry);

        entry->total_benefit += benefit;
        entry->last_benefit = benefit;
        entry->estimated_create_cost = create_cost;
        entry->estimated_maintenance_cost = maintenance_cost;
        entry->last_real_cost = real_cost;
        entry->last_hypothetical_cost = hypothetical_cost;
        total_touches = AutoIndex_TotalTouchCount();
        expected_without_index = real_cost * Max((double) total_touches, 1.0);
        expected_with_index = create_cost +
            hypothetical_cost * Max((double) entry->count, 1.0) +
            maintenance_cost;
        with_index_wins = expected_with_index < expected_without_index;

        {
            double access_fraction = total_touches > 0 ?
                (double) entry->access_count / (double) total_touches : 0.0;
            double insert_fraction = total_touches > 0 ?
                (double) entry->insert_count / (double) total_touches : 0.0;
            double update_fraction = total_touches > 0 ?
                (double) entry->update_count / (double) total_touches : 0.0;
            double delete_fraction = total_touches > 0 ?
                (double) entry->delete_count / (double) total_touches : 0.0;

            elog(LOG,
                 "COLUMN_STATS column=%s access_count=%llu total_count=%llu insert_count=%llu update_count=%llu delete_count=%llu access_fraction=%.4f insert_fraction=%.4f update_fraction=%.4f delete_fraction=%.4f distinct_ratio=unknown",
                 attname ? attname : "unknown",
                 (unsigned long long) entry->access_count,
                 (unsigned long long) total_touches,
                 (unsigned long long) entry->insert_count,
                 (unsigned long long) entry->update_count,
                 (unsigned long long) entry->delete_count,
                 access_fraction,
                 insert_fraction,
                 update_fraction,
                 delete_fraction);
        }
        elog(LOG,
             "BASELINE_SCAN node=SeqScan explain_cost=%.2f formula_cost=%.2f rows=%.0f plan=Seq Scan",
             real_cost,
             real_cost,
             rel->tuples);
        elog(LOG,
             "CANDIDATE_SELECTION source=planner_restrictinfo query_reference_required=false access_threshold=%.4f",
             AUTO_INDEX_MIN_BENEFIT);
        if (benefit < AUTO_INDEX_MIN_BENEFIT)
        {
            double all_fraction = total_touches > 0 ?
                (double) entry->count / (double) total_touches : 0.0;

            elog(LOG,
                 "CANDIDATE_SKIP column=%s reason=access_fraction all_count=%llu all_fraction=%.4f threshold=%.4f distinct_ratio=%.4f cost_status=costed workload_queries=%llu baseline_scan_cost=%.2f hypo_scan_cost=%.2f create_cost=%.2f write_cost=%.2f expected_without_index=%.2f expected_with_index=%.2f improves_expected=false recommended_index=no_index",
                 attname ? attname : "unknown",
                 (unsigned long long) entry->count,
                 all_fraction,
                 AUTO_INDEX_MIN_BENEFIT,
                 selectivity,
                 (unsigned long long) entry->count,
                 real_cost,
                 hypothetical_cost,
                 create_cost,
                 maintenance_cost,
                 expected_without_index,
                 expected_with_index);
            elog(LOG,
                 "QUERY_COST_SUMMARY query_number=%llu column=%s read_cost_without_index=%.2f estimated_without_index_cost=not_applicable_no_candidate create_index_cost=not_applicable_no_candidate read_cost_with_index=not_applicable_no_candidate write_cost=not_applicable_no_candidate estimated_with_index_cost=not_applicable_no_candidate",
                 (unsigned long long) AutoIndexQueryCounter,
                 attname ? attname : "unknown",
                 real_cost);
            elog(LOG,
                 "QUERY_INDEX_DECISION query_number=%llu decision=use_no_index curr_index_pointer=no_index reason=no_candidate without_index_cost=not_applicable_no_candidate best_with_index_cost=not_applicable_no_candidate best_create_cost=not_applicable_no_candidate best_read_cost=not_applicable_no_candidate best_write_cost=not_applicable_no_candidate",
                 (unsigned long long) AutoIndexQueryCounter);
            elog(LOG,
                 "CREATE_INDEX_DECISION decision=do_not_create reason=no_candidate");
            continue;
        }

        {
            double all_fraction = total_touches > 0 ?
                (double) entry->count / (double) total_touches : 0.0;

            elog(LOG,
                 "CANDIDATE_KEEP column=%s reason=candidate all_count=%llu all_fraction=%.4f threshold=%.4f distinct_ratio=%.4f cost_status=costed workload_queries=%llu baseline_scan_cost=%.2f hypo_scan_cost=%.2f create_cost=%.2f write_cost=%.2f expected_without_index=%.2f expected_with_index=%.2f improves_expected=%s recommended_index=%s",
                 attname ? attname : "unknown",
                 (unsigned long long) entry->count,
                 all_fraction,
                 AUTO_INDEX_MIN_BENEFIT,
                 selectivity,
                 (unsigned long long) entry->count,
                 real_cost,
                 hypothetical_cost,
                 create_cost,
                 maintenance_cost,
                 expected_without_index,
                 expected_with_index,
                 with_index_wins ? "true" : "false",
                 has_real_index ? "use_existing" : "auto_advisor_candidate_idx");
        }

        elog(LOG,
             "QUERY_COST_SUMMARY query_number=%llu column=%s read_cost_without_index=%.2f estimated_without_index_cost=%.2f create_index_cost=%.2f read_cost_with_index=%.2f write_cost=%.2f estimated_with_index_cost=%.2f",
             (unsigned long long) AutoIndexQueryCounter,
             attname ? attname : "unknown",
             real_cost,
             expected_without_index,
             create_cost,
             hypothetical_cost,
             maintenance_cost,
             expected_with_index);
        elog(LOG,
             "QUERY_INDEX_DECISION query_number=%llu decision=%s curr_index_pointer=%s reason=%s without_index_cost=%.2f best_with_index_cost=%.2f best_create_cost=%.2f best_read_cost=%.2f best_write_cost=%.2f",
             (unsigned long long) AutoIndexQueryCounter,
             !with_index_wins ? "use_no_index" :
             has_real_index ? "use_existing_index" : "create_index",
             with_index_wins && attname != NULL ? attname : "no_index",
             !with_index_wins ? "without_index_cost_is_lower" :
             has_real_index ? "existing_index_available" : "cost_wins",
             expected_without_index,
             expected_with_index,
             create_cost,
             hypothetical_cost,
             maintenance_cost);
        elog(LOG,
             "CREATE_INDEX_DECISION decision=%s%s%s reason=%s",
             !with_index_wins ? "do_not_create" :
             has_real_index ? "use_existing" : "create",
             with_index_wins && attname != NULL ? " column=" : "",
             with_index_wins && attname != NULL ? attname : "",
             !with_index_wins ? "without_index_cost_is_lower" :
             has_real_index ? "already_exists" : "cost_wins");

        if (with_index_wins &&
            !entry->recommended &&
            entry->total_benefit >= entry->estimated_create_cost +
                                    entry->estimated_maintenance_cost)
        {
            entry->recommended = true;
            elog(LOG,
                 "[AUTO-INDEXER] threshold reached; enqueue/create asynchronously: %s",
                 AutoIndex_BuildCreateIndexCommand(key.relid, attno));
        }
    }

    if (!saw_candidate)
        elog(LOG, "NO_CANDIDATES query_number=%llu",
             (unsigned long long) AutoIndexQueryCounter);
}

static uint64
AutoIndex_TotalTouchCount(void)
{
    HASH_SEQ_STATUS seq;
    AutoIndexEntry *entry;
    uint64 total = 0;

    if (AutoIndexHash == NULL)
        return 0;

    hash_seq_init(&seq, AutoIndexHash);
    while ((entry = (AutoIndexEntry *) hash_seq_search(&seq)) != NULL)
        total += entry->count;

    return total;
}

static void
AutoIndex_InitEntry(AutoIndexEntry *entry)
{
    entry->count = 0;
    entry->access_count = 0;
    entry->insert_count = 0;
    entry->update_count = 0;
    entry->delete_count = 0;
    entry->total_benefit = 0;
    entry->last_benefit = 0;
    entry->estimated_create_cost = 0;
    entry->estimated_maintenance_cost = 0;
    entry->last_real_cost = 0;
    entry->last_hypothetical_cost = 0;
    entry->recommended = false;
}

static void
AutoIndex_EnsureStatsTable(void)
{
    int ret;
    const char *sql =
        "CREATE TABLE IF NOT EXISTS public.auto_index_column_stats ("
        "table_schema text NOT NULL, "
        "table_name text NOT NULL, "
        "column_name text NOT NULL, "
        "access_count float8 NOT NULL DEFAULT 0, "
        "total_count float8 NOT NULL DEFAULT 0, "
        "insert_count float8 NOT NULL DEFAULT 0, "
        "update_count float8 NOT NULL DEFAULT 0, "
        "delete_count float8 NOT NULL DEFAULT 0, "
        "n_distinct float8 NOT NULL DEFAULT 0, "
        "PRIMARY KEY (table_schema, table_name, column_name))";

    if (AutoIndexStatsTableEnsured)
        return;

    ret = SPI_execute(sql, false, 0);
    if (ret != SPI_OK_UTILITY)
        elog(ERROR, "auto_indexer: could not ensure stats table");

    AutoIndexStatsTableEnsured = true;
}

static double
AutoIndex_GetDistinctCount(Oid relid, const char *attname)
{
    int ret;
    StringInfoData sql;
    double n_distinct = 0.0;
    char *schema_name;
    char *table_name;

    schema_name = get_namespace_name(get_rel_namespace(relid));
    table_name = get_rel_name(relid);
    if (schema_name == NULL || table_name == NULL || attname == NULL)
        return 0.0;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT COALESCE(n_distinct, 0)::float8 "
                     "FROM pg_stats "
                     "WHERE schemaname = %s "
                     "  AND tablename = %s "
                     "  AND attname = %s",
                     quote_literal_cstr(schema_name),
                     quote_literal_cstr(table_name),
                     quote_literal_cstr(attname));

    ret = SPI_execute(sql.data, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed == 1)
        n_distinct = DatumGetFloat8(SPI_getbinval(SPI_tuptable->vals[0],
                                                  SPI_tuptable->tupdesc,
                                                  1,
                                                  &(bool){false}));

    pfree(sql.data);
    return n_distinct;
}

static void
AutoIndex_FlushQueryTouches(void)
{
    HASH_SEQ_STATUS seq;
    AutoIndexTouchEntry *touch_entry;
    uint64 total_touches;
    bool connected = false;
    bool snapshot_pushed = false;
    int ret;

    if (AutoIndexQueryTouches == NULL || hash_get_num_entries(AutoIndexQueryTouches) == 0)
        return;

    AutoIndexInternalQueryDepth++;
    PG_TRY();
    {
        if (SPI_connect() != SPI_OK_CONNECT)
            elog(ERROR, "auto_indexer: could not connect SPI");
        connected = true;
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_pushed = true;

        AutoIndex_EnsureStatsTable();

        hash_seq_init(&seq, AutoIndexQueryTouches);
        while ((touch_entry = (AutoIndexTouchEntry *) hash_seq_search(&seq)) != NULL)
        {
            AutoIndexEntry *global_entry;
            AutoIndexKey key = touch_entry->key;
            bool found;

            global_entry = (AutoIndexEntry *)
                hash_search(AutoIndexHash, &key, HASH_ENTER, &found);
            if (!found)
                AutoIndex_InitEntry(global_entry);

            if (touch_entry->access_count > 0)
            {
                global_entry->access_count++;
                global_entry->count++;
                elog(LOG,
                     "STATS_UPDATE query_number=%llu relid=%u attno=%d kind=access count=%llu",
                     (unsigned long long) AutoIndexQueryCounter,
                     key.relid,
                     key.attno,
                     (unsigned long long) global_entry->count);
            }

            if (touch_entry->insert_count > 0)
            {
                global_entry->insert_count++;
                global_entry->count++;
                elog(LOG,
                     "STATS_UPDATE query_number=%llu relid=%u attno=%d kind=insert count=%llu",
                     (unsigned long long) AutoIndexQueryCounter,
                     key.relid,
                     key.attno,
                     (unsigned long long) global_entry->count);
            }

            if (touch_entry->update_count > 0)
            {
                global_entry->update_count++;
                global_entry->count++;
                elog(LOG,
                     "STATS_UPDATE query_number=%llu relid=%u attno=%d kind=update count=%llu",
                     (unsigned long long) AutoIndexQueryCounter,
                     key.relid,
                     key.attno,
                     (unsigned long long) global_entry->count);
            }

            if (touch_entry->delete_count > 0)
            {
                global_entry->delete_count++;
                global_entry->count++;
                elog(LOG,
                     "STATS_UPDATE query_number=%llu relid=%u attno=%d kind=delete count=%llu",
                     (unsigned long long) AutoIndexQueryCounter,
                     key.relid,
                     key.attno,
                     (unsigned long long) global_entry->count);
            }
        }

        total_touches = AutoIndex_TotalTouchCount();

        hash_seq_init(&seq, AutoIndexQueryTouches);
        while ((touch_entry = (AutoIndexTouchEntry *) hash_seq_search(&seq)) != NULL)
        {
            AutoIndexEntry *global_entry;
            AutoIndexKey key = touch_entry->key;
            bool found;
            char *schema_name;
            char *table_name;
            char *column_name;
            double n_distinct;
            StringInfoData sql;

            global_entry = (AutoIndexEntry *)
                hash_search(AutoIndexHash, &key, HASH_FIND, &found);
            if (!found || global_entry == NULL)
                continue;

            schema_name = get_namespace_name(get_rel_namespace(key.relid));
            table_name = get_rel_name(key.relid);
            column_name = get_attname(key.relid, key.attno, false);
            if (schema_name == NULL || table_name == NULL || column_name == NULL)
                continue;

            n_distinct = AutoIndex_GetDistinctCount(key.relid, column_name);

            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "INSERT INTO public.auto_index_column_stats "
                             "(table_schema, table_name, column_name, "
                             " access_count, total_count, insert_count, update_count, delete_count, n_distinct) "
                             "VALUES (%s, %s, %s, %.17g, %.17g, %.17g, %.17g, %.17g, %.17g) "
                             "ON CONFLICT (table_schema, table_name, column_name) DO UPDATE SET "
                             "access_count = EXCLUDED.access_count, "
                             "total_count = EXCLUDED.total_count, "
                             "insert_count = EXCLUDED.insert_count, "
                             "update_count = EXCLUDED.update_count, "
                             "delete_count = EXCLUDED.delete_count, "
                             "n_distinct = EXCLUDED.n_distinct",
                             quote_literal_cstr(schema_name),
                             quote_literal_cstr(table_name),
                             quote_literal_cstr(column_name),
                             (double) global_entry->access_count,
                             (double) total_touches,
                             (double) global_entry->insert_count,
                             (double) global_entry->update_count,
                             (double) global_entry->delete_count,
                             n_distinct);

            ret = SPI_execute(sql.data, false, 0);
            if (ret < 0)
                elog(ERROR, "auto_indexer: could not upsert stats row");

            pfree(sql.data);
        }

        if (connected)
            SPI_finish();
        if (snapshot_pushed)
            PopActiveSnapshot();
    }
    PG_CATCH();
    {
        if (connected)
            SPI_finish();
        if (snapshot_pushed)
            PopActiveSnapshot();
        AutoIndexInternalQueryDepth--;
        PG_RE_THROW();
    }
    PG_END_TRY();

    AutoIndexInternalQueryDepth--;
}

static bool
AutoIndex_ExtractPredicateAttno(RestrictInfo *rinfo, Index relid,
                                AttrNumber *attno, Selectivity *selectivity)
{
    AutoIndexPredicateVarContext ctx;

    if (rinfo == NULL || rinfo->clause == NULL)
        return false;

    MemSet(&ctx, 0, sizeof(ctx));
    ctx.relid = relid;
    (void) AutoIndex_FindPredicateVar((Node *) rinfo->clause, &ctx);

    if (ctx.var == NULL)
        return false;

    *attno = ctx.var->varattno;

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

static bool
AutoIndex_FindPredicateVar(Node *node, void *context)
{
    AutoIndexPredicateVarContext *ctx = (AutoIndexPredicateVarContext *) context;

    if (node == NULL)
        return false;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;

        if (var->varlevelsup == 0 &&
            var->varno == ctx->relid &&
            var->varattno > 0)
        {
            ctx->var = var;
            return true;
        }

        return false;
    }

    return expression_tree_walker(node, AutoIndex_FindPredicateVar, context);
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

static const char *
AutoIndex_CommandTypeName(int command_type)
{
    switch (command_type)
    {
        case CMD_SELECT:
            return "SELECT";
        case CMD_INSERT:
            return "INSERT";
        case CMD_UPDATE:
            return "UPDATE";
        case CMD_DELETE:
            return "DELETE";
        default:
            return "OTHER";
    }
}

static bool
AutoIndex_HasSingleColumnIndex(RelOptInfo *rel, AttrNumber attno)
{
    ListCell   *lc;

    foreach(lc, rel->indexlist)
    {
        IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

        if (index->nkeycolumns == 1 && index->indexkeys[0] == attno)
            return true;
    }

    return false;
}

static bool
AutoIndex_IsInternalQuery(void)
{
    return AutoIndexInternalQueryDepth > 0;
}

void
AutoIndex_EnterInternalQuery(void)
{
    AutoIndexInternalQueryDepth++;
}

void
AutoIndex_LeaveInternalQuery(void)
{
    if (AutoIndexInternalQueryDepth > 0)
        AutoIndexInternalQueryDepth--;
}

static bool
AutoIndex_ShouldIgnoreRelid(Oid relid)
{
    Oid namespace_id;

    if (!OidIsValid(relid))
        return true;

    namespace_id = get_rel_namespace(relid);
    if (!OidIsValid(namespace_id))
        return true;

    return IsCatalogNamespace(namespace_id) || IsToastNamespace(namespace_id);
}

static void
AutoIndex_LogFormula(void)
{
    elog(LOG, "FORMULA");
    elog(LOG, "  after each executed query, update planner/executor stats for the target relation");
    elog(LOG, "  total_count means total column-touch events processed for this table so far");
    elog(LOG, "  per query: each referenced column increments one matching counter by 1 after execution");
    elog(LOG, "  duplicate references to the same column and same command kind in one query are counted once");
    elog(LOG, "  total_count increases by the number of applied per-column counter increments");
    elog(LOG, "  eligible_column = access_count+update_count+insert_count+delete_count / total_count > max(0.05, 2/total_columns) AND distinct_count / row_count > 0.00");
    elog(LOG, "  candidate selection does not require the column to appear in the current query");
    elog(LOG, "  candidate selection includes columns that already have real indexes");
    elog(LOG, "  explain_baseline_scan_cost = EXPLAIN cost with no hypothetical index, logged for comparison");
    elog(LOG, "  hypo_scan_cost = EXPLAIN cost after hypopg_reset() + hypopg_create_index(candidate), logged for comparison");
    elog(LOG, "  baseline_cost = relpages*seq_page_cost + rows*cpu_tuple_cost");
    elog(LOG, "  create_cost = relpages*seq_page_cost + rows*cpu_tuple_cost + rows*index_columns*cpu_operator_cost + rows*log2(rows)*cpu_operator_cost");
    elog(LOG, "  expected_without_index = baseline_cost * total_count");
    elog(LOG, "  write_index_maint_cost = insert_count*C_insert + update_count*C_update + delete_count*C_delete");
    elog(LOG, "  C_insert = log2(rows)*cpu_operator_cost + cpu_tuple_cost");
    elog(LOG, "  C_update = 2*C_insert, C_delete = C_insert");
    elog(LOG, "  stats_maint_cost = (insert_count + update_count + delete_count) * cpu_operator_cost");
    elog(LOG, "  write_maint_cost = write_index_maint_cost + stats_maint_cost");
    elog(LOG, "  expected_with_index = create_cost + access_count * hypo_scan_cost + write_maint_cost");
    elog(LOG, "  SELECT increments access_count by 1 for each referenced column after the query runs");
    elog(LOG, "  INSERT increments insert_count by 1 for each inserted column after the query runs");
    elog(LOG, "  UPDATE increments update_count by 1 for each referenced/modified column after the query runs");
    elog(LOG, "  DELETE increments delete_count by 1 for each referenced column after the query runs");
    elog(LOG, "  ANALYZE runs every 1 write query/queries");
    elog(LOG, "  choose least expected_with_index among candidates, then compare with expected_without_index");
    elog(LOG, "  if best with-index cost wins: use existing index if present, otherwise create it");
    elog(LOG, "  if without-index cost wins: curr_index_pointer=no_index");
}

static void
AutoIndex_LogQueryHeader(RelOptInfo *rel)
{
    uint64 total_touches;

    (void) rel;
    total_touches = AutoIndex_TotalTouchCount();

    elog(LOG,
         "STATS_TABLE loaded table=public.auto_index_column_stats touch_events=%llu",
         (unsigned long long) total_touches);
    elog(LOG,
         "TOTAL_COLUMN_ACCESSES total_count=%llu",
         (unsigned long long) total_touches);
}

static void
AutoIndex_ResetQueryState(void)
{
    AutoIndexQueryActive = false;
    AutoIndexQueryHeaderPrinted = false;
    AutoIndexCurrentCommandType = CMD_UNKNOWN;
    AutoIndex_ResetQueryTouches();

    if (AutoIndexCurrentQueryString != NULL)
    {
        pfree(AutoIndexCurrentQueryString);
        AutoIndexCurrentQueryString = NULL;
    }
}

/* ---------- PRINT ---------- */

void AutoIndex_Print(void)
{
    HASH_SEQ_STATUS status;
    AutoIndexEntry *entry;

    if (AutoIndex_IsInternalQuery() || AutoIndexHash == NULL)
        return;

    AutoIndex_FlushQueryTouches();

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

    if (AutoIndexQueryActive)
    {
        elog(LOG, "QUERY_EXECUTED number=%llu rows_returned=unknown",
             (unsigned long long) AutoIndexQueryCounter);
        elog(LOG, "QUERY_END number=%llu",
             (unsigned long long) AutoIndexQueryCounter);
    }

    AutoIndex_ResetQueryState();
}
