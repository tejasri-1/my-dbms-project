/* -------------------------------------------------------------------------
 *
 * auto_index_advisor.c
 *		Stage-1/2 internal background worker for the HypoPG index-advisor flow.
 *
 * This worker reads the same stats-table shape used by
 * scripts/hypopg_index_advisor.py, computes the same access/write fractions,
 * applies the same candidate threshold, emits PostgreSQL server logs,
 * persists Stage-2 recommendations, performs Stage-3 HypoPG costing for SQL
 * stored in the advisor workload table, and can optionally apply Stage-4 real
 * index creation for winning recommendations, and captures live workload
 * statements through the planner hook for Stage 5.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <ctype.h>
#include <math.h>

#include "access/xact.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/auto_indexer.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(auto_index_advisor_check);
PG_FUNCTION_INFO_V1(auto_index_advisor_refresh_recommendations);
PG_FUNCTION_INFO_V1(auto_index_advisor_refresh_costing);
PG_FUNCTION_INFO_V1(auto_index_advisor_apply_recommendations);
PG_FUNCTION_INFO_V1(auto_index_advisor_drop_indexes);
PG_FUNCTION_INFO_V1(auto_index_advisor_run_test_cycle);
PG_FUNCTION_INFO_V1(auto_index_advisor_run_full_cycle);

void _PG_init(void);
PGDLLEXPORT void auto_index_advisor_main(Datum main_arg);
PGDLLEXPORT void auto_index_advisor_create_index_worker(Datum main_arg);

static bool auto_index_advisor_enabled = true;
static int	auto_index_advisor_naptime = 10;
static char *auto_index_advisor_database = NULL;
static char *auto_index_advisor_role = NULL;
static char *auto_index_advisor_target_table = NULL;
static char *auto_index_advisor_stats_table = NULL;
static char *auto_index_advisor_recommendation_table = NULL;
static char *auto_index_advisor_workload_table = NULL;
static char *auto_index_advisor_log_table = NULL;
static char *auto_index_advisor_log_file_path = NULL;
static char *auto_index_advisor_query_decision_log_file_path = NULL;
static FILE *auto_index_advisor_log_file = NULL;
static char *auto_index_advisor_log_file_resolved_path = NULL;
static bool auto_index_advisor_query_decision_log_notice_emitted = false;
static bool auto_index_advisor_enable_hypopg_costing = true;
static bool auto_index_advisor_auto_create = false;
static bool auto_index_advisor_drop_indexes_before_run = true;
static bool auto_index_advisor_drop_indexes_after_apply = false;
static bool auto_index_advisor_capture_workload = true;
static int	auto_index_advisor_max_indexes_per_run = 1;
static double auto_index_advisor_min_access_fraction = 0.05;
static double auto_index_advisor_min_distinct_ratio = 0.0;

static uint32 auto_index_advisor_wait_event_main = 0;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static bool auto_index_advisor_in_internal_query = false;
static uint64 auto_index_advisor_preplan_query_counter = 0;
static StringInfo advisor_pending_query_columns = NULL;
static uint64 advisor_pending_query_columns_no = 0;
static bool advisor_workflow_timing_started = false;
static bool advisor_workflow_exit_registered = false;
static TimestampTz advisor_workflow_start_time = 0;
static TimestampTz advisor_workflow_end_time = 0;
static int64 advisor_total_query_execution_time_us = 0;
static uint64 advisor_executed_query_count = 0;
static uint64 advisor_logged_query_count = 0;

static void advisor_logf(const char *event, const char *fmt,...)
	pg_attribute_printf(2, 3);
static void advisor_clear_log_table(void);
static void advisor_begin_log_file(void);
static char *advisor_compact_log_message(const char *message);
static char *advisor_export_log_file(void);
static char *advisor_run_full_cycle_internal(char **log_path_out);
static char *advisor_run_once(bool emit_log, bool force);
static char *advisor_apply_recommendations(bool emit_log, bool force);
static char *advisor_drop_existing_indexes(bool emit_log, int *dropped_out);
static char *advisor_drop_created_indexes(bool emit_log, int *dropped_out);
static bool advisor_preplan_current_query(Query *parse, const char *query_string);
static bool advisor_is_online_query_command(CmdType command_type);
static bool advisor_create_index_for_rel_att(Oid relid, AttrNumber attno);
static void advisor_query_decision_log(uint64 query_no,
									   const char *query_string,
									   const char *column_name,
									   bool has_cost_without_index,
									   bool has_costs,
									   double cost_without_index,
									   double cost_with_index,
									   double cost_of_creation,
									   double cost_read,
									   double cost_maintenance_write,
									   const char *final_decision,
									   const char *reason,
									   const char *index_name);
static void advisor_flush_pending_query_decision(void);
static void advisor_log_created_index(const char *index_name,
									  const char *sql);
static void advisor_record_query_execution(TimestampTz start_time,
										   TimestampTz end_time);
static void advisor_workflow_summary_on_exit(int code, Datum arg);
static const char *advisor_final_decision_name(const char *final_decision);
static void advisor_reset_pending_query_columns(void);
static const char *advisor_query_decision_log_path(void);
static void advisor_notice_query_decision_log_path(const char *path);
static bool advisor_enter_internal_query(void);
static void advisor_leave_internal_query(bool old_internal_query);
static void auto_index_advisor_ExecutorRun(QueryDesc *queryDesc,
										   ScanDirection direction,
										   uint64 count);
static void advisor_query_column_log(uint64 query_no,
									 const char *column_name,
									 double access_count,
									 double update_count,
									 double insert_count,
									 double delete_count,
									 bool is_candidate,
									 double activity_count,
									 double total_count,
									 double activity_fraction,
									 double threshold,
									 double cost_without_index,
									 bool has_costs,
									 double cost_with_index,
									 double cost_of_creation,
									 double cost_read,
									 double cost_maintenance_write,
									 const char *reason,
									 const char *index_name);

#define AUTO_INDEX_ADVISOR_LOCK_KEY1 917355
#define AUTO_INDEX_ADVISOR_LOCK_KEY2 18426041

static uint32
advisor_wait_event_main(void)
{
#if PG_VERSION_NUM >= 170000
	if (auto_index_advisor_wait_event_main == 0)
		auto_index_advisor_wait_event_main =
			WaitEventExtensionNew("AutoIndexAdvisorMain");
#else
	auto_index_advisor_wait_event_main = PG_WAIT_EXTENSION;
#endif
	return auto_index_advisor_wait_event_main;
}

static bool
advisor_enter_internal_query(void)
{
	bool		old_internal_query = auto_index_advisor_in_internal_query;

	auto_index_advisor_in_internal_query = true;
	AutoIndex_EnterInternalQuery();
	return old_internal_query;
}

static void
advisor_leave_internal_query(bool old_internal_query)
{
	AutoIndex_LeaveInternalQuery();
	auto_index_advisor_in_internal_query = old_internal_query;
}

static bool
advisor_is_online_query_command(CmdType command_type)
{
	return command_type == CMD_SELECT ||
		command_type == CMD_UPDATE ||
		command_type == CMD_DELETE;
}

static void
advisor_take_xact_lock(void)
{
	int			ret;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT pg_advisory_xact_lock(%d, %d)",
					 AUTO_INDEX_ADVISOR_LOCK_KEY1,
					 AUTO_INDEX_ADVISOR_LOCK_KEY2);
	ret = SPI_execute(sql.data, false, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "auto_index_advisor: could not acquire advisory lock");
	pfree(sql.data);
}

typedef struct AdvisorColumnStat
{
	char	   *column_name;
	AttrNumber	attno;
	double		access_count;
	double		total_count;
	double		insert_count;
	double		update_count;
	double		delete_count;
	double		n_distinct;
	double		distinct_ratio;
	double		access_fraction;
	double		insert_fraction;
	double		update_fraction;
	double		delete_fraction;
} AdvisorColumnStat;

typedef struct AdvisorCostSettings
{
	double		seq_page_cost;
	double		cpu_tuple_cost;
	double		cpu_operator_cost;
} AdvisorCostSettings;

typedef struct AdvisorCostResult
{
	int			query_count;
	double		baseline_scan_cost;
	double		hypo_scan_cost;
	double		create_index_cost;
	double		write_maint_cost;
	double		expected_without_index;
	double		expected_with_index;
	bool		improves_expected;
	const char *status;
} AdvisorCostResult;

typedef struct AdvisorApplyCandidate
{
	char	   *schema_name;
	char	   *table_name;
	char	   *column_name;
	char	   *index_name;
} AdvisorApplyCandidate;

typedef struct AdvisorDynamicIndexJob
{
	Oid			dboid;
	Oid			roleid;
	Oid			relid;
	int32		attno;
} AdvisorDynamicIndexJob;

typedef struct AdvisorPendingQueryDecision
{
	bool		pending;
	uint64		query_no;
	char	   *query_string;
	char	   *column_name;
	bool		has_cost_without_index;
	bool		has_costs;
	double		cost_without_index;
	double		cost_with_index;
	double		cost_of_creation;
	double		cost_read;
	double		cost_maintenance_write;
	char	   *final_decision;
	char	   *reason;
	char	   *index_name;
} AdvisorPendingQueryDecision;

static AdvisorPendingQueryDecision advisor_pending_query_decision = {0};

static void
append_qualified_identifier(StringInfo buf, const char *name)
{
	char	   *raw = pstrdup(name);
	char	   *dot = strchr(raw, '.');

	if (dot == NULL)
	{
		appendStringInfoString(buf, quote_identifier(raw));
		pfree(raw);
		return;
	}

	*dot = '\0';
	appendStringInfo(buf, "%s.%s",
					 quote_identifier(raw),
					 quote_identifier(dot + 1));
	pfree(raw);
}

static double
spi_get_float8(HeapTuple tuple, TupleDesc tupdesc, int attnum)
{
	bool		isnull;
	Datum		value = SPI_getbinval(tuple, tupdesc, attnum, &isnull);

	return isnull ? 0.0 : DatumGetFloat8(value);
}

static char *
spi_get_cstring(HeapTuple tuple, TupleDesc tupdesc, int attnum)
{
	bool		isnull;
	Datum		value = SPI_getbinval(tuple, tupdesc, attnum, &isnull);

	return isnull ? pstrdup("") : TextDatumGetCString(value);
}

static double
advisor_distinct_count(double n_distinct, double row_count)
{
	if (n_distinct < 0.0)
		return fabs(n_distinct) * row_count;
	return n_distinct;
}

static double
advisor_parse_total_cost(const char *json_text)
{
	char	   *key;
	char	   *colon;
	char	   *endptr;
	double		value;

	key = strstr(json_text, "\"Total Cost\"");
	if (key == NULL)
		return -1.0;

	colon = strchr(key, ':');
	if (colon == NULL)
		return -1.0;

	value = strtod(colon + 1, &endptr);
	if (endptr == colon + 1)
		return -1.0;

	return value;
}

static bool
advisor_relation_exists(const char *relname)
{
	int			ret;
	bool		exists;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT to_regclass(%s) IS NOT NULL",
					 quote_literal_cstr(relname));

	ret = SPI_execute(sql.data, false, 1);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "auto_index_advisor: relation existence check failed");

	exists = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										1,
										&(bool){false}));
	pfree(sql.data);
	return exists;
}

static void
advisor_ensure_log_table(void)
{
	int			ret;
	StringInfoData sql;

	if (auto_index_advisor_log_table == NULL ||
		auto_index_advisor_log_table[0] == '\0')
		return;

	if (advisor_relation_exists(auto_index_advisor_log_table))
		return;

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE TABLE ");
	append_qualified_identifier(&sql, auto_index_advisor_log_table);
	appendStringInfo(&sql,
					 " ("
					 "log_id bigserial PRIMARY KEY, "
					 "logged_at timestamptz NOT NULL DEFAULT now(), "
					 "pid integer NOT NULL DEFAULT pg_backend_pid(), "
					 "event text NOT NULL, "
					 "message text NOT NULL"
					 ")");

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "auto_index_advisor: could not create log table");

	pfree(sql.data);
}

static void
advisor_log_line(const char *event, const char *message)
{
	int			ret;
	StringInfoData sql;

	if (auto_index_advisor_log_table == NULL ||
		auto_index_advisor_log_table[0] == '\0')
		return;

	advisor_ensure_log_table();

	initStringInfo(&sql);
	appendStringInfo(&sql, "INSERT INTO ");
	append_qualified_identifier(&sql, auto_index_advisor_log_table);
	appendStringInfo(&sql,
					 " (event, message) VALUES (%s, %s)",
					 quote_literal_cstr(event),
					 quote_literal_cstr(message));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "auto_index_advisor: could not write advisor log");

	if (auto_index_advisor_log_file != NULL)
	{
		char	   *compact_message;

		compact_message = advisor_compact_log_message(message);
		fprintf(auto_index_advisor_log_file,
				"%s pid=%d event=%s message=%s\n",
				timestamptz_to_str(GetCurrentTimestamp()),
				MyProcPid,
				event,
				compact_message);
		fflush(auto_index_advisor_log_file);
		pfree(compact_message);
	}

	pfree(sql.data);
}

static void
advisor_logf(const char *event, const char *fmt,...)
{
	va_list		args;
	StringInfoData message;

	initStringInfo(&message);
	va_start(args, fmt);
	appendStringInfoVA(&message, fmt, args);
	va_end(args);

	advisor_log_line(event, message.data);
	pfree(message.data);
}

static const char *
advisor_query_decision_log_path(void)
{
	return (auto_index_advisor_query_decision_log_file_path != NULL &&
			auto_index_advisor_query_decision_log_file_path[0] != '\0') ?
		auto_index_advisor_query_decision_log_file_path :
		"/tmp/auto_index_advisor_query_decisions.log";
}

static void
advisor_notice_query_decision_log_path(const char *path)
{
	ereport(LOG,
			(errmsg("auto index advisor query log: %s", path)));

	if (!auto_index_advisor_query_decision_log_notice_emitted)
	{
		ereport(NOTICE,
				(errmsg("auto index advisor query log: %s", path)));
		auto_index_advisor_query_decision_log_notice_emitted = true;
	}
}

static const char *
advisor_final_decision_name(const char *final_decision)
{
	if (final_decision == NULL)
		return "no_index";
	if (strcmp(final_decision, "created_index") == 0)
		return "create_new_index";
	if (strcmp(final_decision, "used_existing_index") == 0)
		return "use_existing_index";
	return "no_index";
}

static void
advisor_reset_pending_query_columns(void)
{
	if (advisor_pending_query_columns != NULL)
	{
		if (advisor_pending_query_columns->data != NULL)
			pfree(advisor_pending_query_columns->data);
		pfree(advisor_pending_query_columns);
		advisor_pending_query_columns = NULL;
	}
	advisor_pending_query_columns_no = 0;
}

static void
advisor_query_decision_log(uint64 query_no,
						   const char *query_string,
						   const char *column_name,
						   bool has_cost_without_index,
						   bool has_costs,
						   double cost_without_index,
						   double cost_with_index,
						   double cost_of_creation,
						   double cost_read,
						   double cost_maintenance_write,
						   const char *final_decision,
						   const char *reason,
						   const char *index_name)
{
	MemoryContext oldcontext;

	if (advisor_pending_query_decision.pending)
		advisor_flush_pending_query_decision();

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	advisor_pending_query_decision.pending = true;
	advisor_pending_query_decision.query_no = query_no;
	advisor_pending_query_decision.query_string =
		query_string != NULL ? pstrdup(query_string) : NULL;
	advisor_pending_query_decision.column_name =
		column_name != NULL ? pstrdup(column_name) : NULL;
	advisor_pending_query_decision.has_cost_without_index = has_cost_without_index;
	advisor_pending_query_decision.has_costs = has_costs;
	advisor_pending_query_decision.cost_without_index = cost_without_index;
	advisor_pending_query_decision.cost_with_index = cost_with_index;
	advisor_pending_query_decision.cost_of_creation = cost_of_creation;
	advisor_pending_query_decision.cost_read = cost_read;
	advisor_pending_query_decision.cost_maintenance_write = cost_maintenance_write;
	advisor_pending_query_decision.final_decision =
		final_decision != NULL ? pstrdup(final_decision) : NULL;
	advisor_pending_query_decision.reason =
		reason != NULL ? pstrdup(reason) : NULL;
	advisor_pending_query_decision.index_name =
		index_name != NULL ? pstrdup(index_name) : NULL;
	MemoryContextSwitchTo(oldcontext);
}

static void
advisor_flush_pending_query_decision(void)
{
	FILE	   *file;
	const char *path;
	char	   *compact_query;
	const char *decision_made;
	const char *index_used;
	const char *index_column;
	bool		skip_log_entry;
	uint64		log_query_no;
	AdvisorPendingQueryDecision *entry = &advisor_pending_query_decision;

	if (!entry->pending)
		return;

	skip_log_entry = entry->reason != NULL &&
		strcmp(entry->reason, "query_does_not_reference_target") == 0;
	if (skip_log_entry)
	{
		if (entry->query_string != NULL)
			pfree(entry->query_string);
		if (entry->column_name != NULL)
			pfree(entry->column_name);
		if (entry->final_decision != NULL)
			pfree(entry->final_decision);
		if (entry->reason != NULL)
			pfree(entry->reason);
		if (entry->index_name != NULL)
			pfree(entry->index_name);
		advisor_reset_pending_query_columns();
		memset(entry, 0, sizeof(*entry));
		return;
	}

	path = advisor_query_decision_log_path();

	file = AllocateFile(path, "a");
	if (file == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open auto index advisor query decision log \"%s\": %m",
						path)));
		if (entry->query_string != NULL)
			pfree(entry->query_string);
		if (entry->column_name != NULL)
			pfree(entry->column_name);
		if (entry->final_decision != NULL)
			pfree(entry->final_decision);
		if (entry->reason != NULL)
			pfree(entry->reason);
		if (entry->index_name != NULL)
			pfree(entry->index_name);
		advisor_reset_pending_query_columns();
		memset(entry, 0, sizeof(*entry));
		return;
	}

	compact_query = advisor_compact_log_message(entry->query_string);
	decision_made = advisor_final_decision_name(entry->final_decision);
	index_used = (strcmp(decision_made, "no_index") != 0 &&
				  entry->index_name != NULL) ?
		entry->index_name : "no_index_used";
	index_column = (strcmp(decision_made, "no_index") != 0 &&
					entry->column_name != NULL) ?
		entry->column_name : "none";
	log_query_no = ++advisor_logged_query_count;

	fprintf(file,
			"============================================================\n"
			"query %llu:\n"
			"query_text=%s\n"
			"decision_made=%s\n"
			"index_used=%s\n"
			"index_column=%s\n",
			(unsigned long long) log_query_no,
			compact_query != NULL ? compact_query : "",
			decision_made,
			index_used,
			index_column);

	if (entry->has_cost_without_index)
		fprintf(file, "cost_without_index=%.2f\n", entry->cost_without_index);
	else
		fprintf(file, "cost_without_index=not_applicable\n");

	if (entry->has_costs)
		fprintf(file,
				"least_cost_with_index=%.2f\n"
				"cost_of_creation=%.2f\n"
				"read_cost=%.2f\n"
				"write_maintenance_cost=%.2f\n",
				entry->cost_with_index,
				entry->cost_of_creation,
				entry->cost_read,
				entry->cost_maintenance_write);
	else
		fprintf(file,
				"least_cost_with_index=not_applicable\n"
				"cost_of_creation=not_applicable\n"
				"read_cost=not_applicable\n"
				"write_maintenance_cost=not_applicable\n");

	fprintf(file,
			"reason=%s\n"
			"columns:\n",
			entry->reason != NULL ? entry->reason : "");
	if (advisor_pending_query_columns != NULL &&
		advisor_pending_query_columns_no == entry->query_no &&
		advisor_pending_query_columns->len > 0)
		fprintf(file, "%s", advisor_pending_query_columns->data);
	else
		fprintf(file, "  none\n");
	fprintf(file, "============================================================\n\n");

	if (FreeFile(file))
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close auto index advisor query decision log \"%s\": %m",
						path)));
	advisor_notice_query_decision_log_path(path);
	pfree(compact_query);

	if (entry->query_string != NULL)
		pfree(entry->query_string);
	if (entry->column_name != NULL)
		pfree(entry->column_name);
	if (entry->final_decision != NULL)
		pfree(entry->final_decision);
	if (entry->reason != NULL)
		pfree(entry->reason);
	if (entry->index_name != NULL)
		pfree(entry->index_name);
	advisor_reset_pending_query_columns();
	memset(entry, 0, sizeof(*entry));
}

static void
advisor_log_created_index(const char *index_name, const char *sql)
{
	FILE	   *file;
	const char *path;
	char	   *compact_sql;

	path = advisor_query_decision_log_path();
	file = AllocateFile(path, "a");
	if (file == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open auto index advisor query decision log \"%s\": %m",
						path)));
		return;
	}

	compact_sql = advisor_compact_log_message(sql);
	fprintf(file,
			"created_index=%s\n"
			"status=created_by_background_worker\n"
			"sql=%s\n\n",
			index_name != NULL ? index_name : "",
			compact_sql != NULL ? compact_sql : "");

	if (FreeFile(file))
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close auto index advisor query decision log \"%s\": %m",
						path)));
	advisor_notice_query_decision_log_path(path);
	pfree(compact_sql);
}

static void
advisor_record_query_execution(TimestampTz start_time, TimestampTz end_time)
{
	if (!advisor_workflow_timing_started)
	{
		advisor_workflow_timing_started = true;
		advisor_workflow_start_time = start_time;
	}

	advisor_workflow_end_time = end_time;
	if (end_time >= start_time)
		advisor_total_query_execution_time_us += (end_time - start_time);
	advisor_executed_query_count++;
}

static void
advisor_workflow_summary_on_exit(int code, Datum arg)
{
	FILE	   *file;
	const char *path;
	char	   *start_time;
	char	   *end_time;
	double		workflow_elapsed_ms = 0.0;
	double		total_query_execution_ms;

	(void) code;
	(void) arg;

	if (!advisor_workflow_timing_started)
		return;

	advisor_flush_pending_query_decision();

	path = advisor_query_decision_log_path();
	file = AllocateFile(path, "a");
	if (file == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open auto index advisor query decision log \"%s\": %m",
						path)));
		return;
	}

	start_time = pstrdup(timestamptz_to_str(advisor_workflow_start_time));
	end_time = pstrdup(timestamptz_to_str(advisor_workflow_end_time));
	if (advisor_workflow_end_time >= advisor_workflow_start_time)
		workflow_elapsed_ms =
			((double) (advisor_workflow_end_time -
					   advisor_workflow_start_time)) / 1000.0;
	total_query_execution_ms =
		((double) advisor_total_query_execution_time_us) / 1000.0;

	fprintf(file,
			"============================================================\n"
			"workflow_summary:\n"
			"workflow_start_time=%s\n"
			"workflow_end_time=%s\n"
			"workflow_elapsed_ms=%.3f\n"
			"total_query_execution_time_ms=%.3f\n"
			"executed_query_count=%llu\n"
			"============================================================\n\n",
			start_time,
			end_time,
			workflow_elapsed_ms,
			total_query_execution_ms,
			(unsigned long long) advisor_logged_query_count);

	if (FreeFile(file))
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close auto index advisor query decision log \"%s\": %m",
						path)));

	pfree(start_time);
	pfree(end_time);
}

static void
advisor_query_column_log(uint64 query_no,
						 const char *column_name,
						 double access_count,
						 double update_count,
						 double insert_count,
						 double delete_count,
						 bool is_candidate,
						 double activity_count,
						 double total_count,
						 double activity_fraction,
						 double threshold,
						 double cost_without_index,
						 bool has_costs,
						 double cost_with_index,
						 double cost_of_creation,
						 double cost_read,
						 double cost_maintenance_write,
						 const char *reason,
						 const char *index_name)
{
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	if (advisor_pending_query_columns != NULL &&
		advisor_pending_query_columns_no != query_no)
		advisor_reset_pending_query_columns();

	if (advisor_pending_query_columns == NULL)
	{
		advisor_pending_query_columns = palloc0(sizeof(StringInfoData));
		initStringInfo(advisor_pending_query_columns);
		advisor_pending_query_columns_no = query_no;
	}

	appendStringInfo(advisor_pending_query_columns,
					 "  column=%s access_count=%.0f update_count=%.0f "
					 "insert_count=%.0f delete_count=%.0f candidate=%s "
					 "activity_count=%.0f total_count=%.0f "
					 "activity_fraction=%.6f threshold=%.6f reason=%s "
					 "index=%s ",
					 column_name != NULL ? column_name : "none",
					 access_count,
					 update_count,
					 insert_count,
					 delete_count,
					 is_candidate ? "true" : "false",
					 activity_count,
					 total_count,
					 activity_fraction,
					 threshold,
					 reason != NULL ? reason : "",
					 index_name != NULL ? index_name : "none");
	if (has_costs)
		appendStringInfo(advisor_pending_query_columns,
						 "with_index_cost=%.2f cost_of_creation=%.2f "
						 "read_cost=%.2f write_maintenance_cost=%.2f\n",
						 cost_with_index,
						 cost_of_creation,
						 cost_read,
						 cost_maintenance_write);
	else
		appendStringInfoString(advisor_pending_query_columns,
							   "with_index_cost=not_applicable "
							   "cost_of_creation=not_applicable "
							   "read_cost=not_applicable "
							   "write_maintenance_cost=not_applicable\n");

	MemoryContextSwitchTo(oldcontext);
}

static char *
advisor_compact_log_message(const char *message)
{
	StringInfoData compact;
	const char *ptr;

	initStringInfo(&compact);
	for (ptr = message; ptr != NULL && *ptr != '\0'; ptr++)
	{
		if (*ptr == '\n' || *ptr == '\r' || *ptr == '\t')
			appendStringInfoChar(&compact, ' ');
		else
			appendStringInfoChar(&compact, *ptr);
	}

	return compact.data;
}

static void
advisor_begin_log_file(void)
{
	char	   *path;

	if (auto_index_advisor_log_file != NULL)
		return;

	path = (auto_index_advisor_log_file_path != NULL &&
			auto_index_advisor_log_file_path[0] != '\0') ?
		auto_index_advisor_log_file_path :
		"/tmp/auto_index_advisor.log";

	auto_index_advisor_log_file = AllocateFile(path, "w");
	if (auto_index_advisor_log_file == NULL)
	{
		char	   *fallback_path;

		fallback_path = psprintf("/tmp/auto_index_advisor_%d_%ld.log",
								 MyProcPid,
								 (long) GetCurrentTimestamp());
		auto_index_advisor_log_file = AllocateFile(fallback_path, "w");
		if (auto_index_advisor_log_file == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open log export file \"%s\": %m",
							fallback_path)));

		if (auto_index_advisor_log_file_resolved_path != NULL)
			pfree(auto_index_advisor_log_file_resolved_path);
		auto_index_advisor_log_file_resolved_path = fallback_path;
	}
	else
	{
		if (auto_index_advisor_log_file_resolved_path != NULL)
			pfree(auto_index_advisor_log_file_resolved_path);
		auto_index_advisor_log_file_resolved_path = pstrdup(path);
	}

	fprintf(auto_index_advisor_log_file, "Auto Index Advisor internal log export\n");
	fprintf(auto_index_advisor_log_file, "exported_at=%s\n",
			timestamptz_to_str(GetCurrentTimestamp()));
	fprintf(auto_index_advisor_log_file, "source_table=%s\n",
			auto_index_advisor_log_table ? auto_index_advisor_log_table : "");
	fflush(auto_index_advisor_log_file);
}

static void
advisor_clear_log_table(void)
{
	int			ret;
	StringInfoData sql;

	if (auto_index_advisor_log_table == NULL ||
		auto_index_advisor_log_table[0] == '\0')
		return;

	advisor_ensure_log_table();

	initStringInfo(&sql);
	appendStringInfo(&sql, "TRUNCATE TABLE ");
	append_qualified_identifier(&sql, auto_index_advisor_log_table);
	appendStringInfoString(&sql, " RESTART IDENTITY");

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "auto_index_advisor: could not clear log table");

	pfree(sql.data);
}

static char *
advisor_export_log_file(void)
{
	if (auto_index_advisor_log_file != NULL)
	{
		if (FreeFile(auto_index_advisor_log_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close log export file \"%s\": %m",
							auto_index_advisor_log_file_resolved_path)));
		auto_index_advisor_log_file = NULL;
	}

	if (auto_index_advisor_log_file_resolved_path == NULL)
		advisor_begin_log_file();

	return pstrdup(auto_index_advisor_log_file_resolved_path);
}

static char *
advisor_run_full_cycle_internal(char **log_path_out)
{
	char	   *run_result;
	char	   *apply_result;
	char	   *drop_result;
	char	   *log_path;
	StringInfoData result;

	advisor_begin_log_file();
	advisor_clear_log_table();
	run_result = advisor_run_once(true, true);
	if (run_result == NULL || strncmp(run_result, "OK:", 3) != 0)
	{
		char	   *path;

		path = advisor_export_log_file();
		if (log_path_out != NULL)
			*log_path_out = pstrdup(path);
		initStringInfo(&result);
		appendStringInfo(&result,
						 "FAIL: full_cycle run=[%s] log_path=%s",
						 run_result != NULL ? run_result : "unknown",
						 path);
		return result.data;
	}
	apply_result = advisor_apply_recommendations(true, true);
	drop_result = pstrdup("SKIPPED: advisor-created indexes are retained");
	log_path = advisor_export_log_file();
	if (log_path_out != NULL)
		*log_path_out = pstrdup(log_path);

	initStringInfo(&result);
	appendStringInfo(&result,
					 "OK: full_cycle run=[%s] apply=[%s] drop=[%s] log_path=%s",
					 run_result,
					 apply_result,
					 drop_result,
					 log_path);
	return result.data;
}

static char *
advisor_lower_cstr(const char *value)
{
	char	   *lower;

	if (value == NULL)
		return pstrdup("");

	lower = pstrdup(value);
	for (int i = 0; lower[i] != '\0'; i++)
		lower[i] = (char) tolower((unsigned char) lower[i]);

	return lower;
}

static const char *
advisor_unqualified_name(const char *qualified_name)
{
	const char *dot;

	if (qualified_name == NULL)
		return "";

	dot = strrchr(qualified_name, '.');
	return dot == NULL ? qualified_name : dot + 1;
}

static bool
advisor_query_mentions_target(const char *query_string)
{
	char	   *query_lower;
	char	   *target_lower;
	char	   *table_lower;
	bool		matches;

	if (query_string == NULL || query_string[0] == '\0' ||
		auto_index_advisor_target_table == NULL ||
		auto_index_advisor_target_table[0] == '\0')
		return false;

	query_lower = advisor_lower_cstr(query_string);
	target_lower = advisor_lower_cstr(auto_index_advisor_target_table);
	table_lower = advisor_lower_cstr(advisor_unqualified_name(auto_index_advisor_target_table));

	matches = strstr(query_lower, target_lower) != NULL ||
		strstr(query_lower, table_lower) != NULL;

	pfree(query_lower);
	pfree(target_lower);
	pfree(table_lower);
	return matches;
}

static Oid
advisor_lookup_target_relid(void)
{
	int			ret;
	bool		isnull;
	Datum		value;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT to_regclass(%s)::oid",
					 quote_literal_cstr(auto_index_advisor_target_table));

	ret = SPI_execute(sql.data, true, 1);
	pfree(sql.data);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
		return InvalidOid;

	value = SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc,
						  1,
						  &isnull);
	return isnull ? InvalidOid : DatumGetObjectId(value);
}

static bool
advisor_query_references_target(Query *parse, Oid target_relid)
{
	ListCell   *lc;

	if (parse == NULL || !OidIsValid(target_relid))
		return false;

	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION && rte->relid == target_relid)
			return true;
	}

	return false;
}

static bool
advisor_column_exists(const char *relname, const char *column_name)
{
	int			ret;
	bool		exists;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT EXISTS ("
					 "SELECT 1 FROM pg_attribute "
					 "WHERE attrelid = to_regclass(%s) "
					 "  AND attname = %s "
					 "  AND attnum > 0 "
					 "  AND NOT attisdropped)",
					 quote_literal_cstr(relname),
					 quote_literal_cstr(column_name));

	ret = SPI_execute(sql.data, true, 1);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "auto_index_advisor: column existence check failed");

	exists = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										1,
										&(bool){false}));
	pfree(sql.data);
	return exists;
}

static void
advisor_add_column_if_missing(const char *relname,
							  const char *column_name,
							  const char *definition)
{
	int			ret;
	StringInfoData sql;

	if (advisor_column_exists(relname, column_name))
		return;

	initStringInfo(&sql);
	appendStringInfo(&sql, "ALTER TABLE ");
	append_qualified_identifier(&sql, relname);
	appendStringInfo(&sql, " ADD COLUMN IF NOT EXISTS %s %s",
					 quote_identifier(column_name),
					 definition);

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "auto_index_advisor: could not add recommendation column");

	pfree(sql.data);
}

static char *
advisor_index_name(const char *table_name, const char *column_name)
{
	char	   *raw;
	char	   *clean;
	int			out = 0;

	raw = psprintf("auto_advisor_%s_%s_idx", table_name, column_name);
	clean = palloc0(64);

	for (int i = 0; raw[i] != '\0' && out < 63; i++)
	{
		unsigned char ch = (unsigned char) raw[i];

		if ((ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '_')
			clean[out++] = ch;
		else if (ch >= 'A' && ch <= 'Z')
			clean[out++] = ch - 'A' + 'a';
		else
			clean[out++] = '_';
	}

	clean[out] = '\0';
	return clean;
}

static char *
advisor_create_index_sql(const char *schema_name, const char *table_name,
						 const char *column_name, const char *index_name)
{
	return psprintf("CREATE INDEX %s ON %s.%s (%s)",
					quote_identifier(index_name),
					quote_identifier(schema_name),
					quote_identifier(table_name),
					quote_identifier(column_name));
}

static bool
advisor_find_single_column_index(Oid relid, AttrNumber attno, char **index_name_out)
{
	int			ret;
	bool		found = false;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT i.relname::text "
					 "FROM pg_index x "
					 "JOIN pg_class i ON i.oid = x.indexrelid "
					 "WHERE x.indrelid = %u "
					 "  AND x.indisvalid "
					 "  AND x.indisready "
					 "  AND x.indnkeyatts = 1 "
					 "  AND x.indkey[0] = %d "
					 "ORDER BY i.relname "
					 "LIMIT 1",
					 relid,
					 attno);

	ret = SPI_execute(sql.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		found = true;
		if (index_name_out != NULL)
			*index_name_out = spi_get_cstring(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc,
											  1);
	}

	pfree(sql.data);
	return found;
}

static AdvisorCostSettings
advisor_get_cost_settings(void)
{
	int			ret;
	AdvisorCostSettings settings = {1.0, 0.01, 0.0025};

	ret = SPI_execute(
		"SELECT current_setting('seq_page_cost')::float8, "
		"       current_setting('cpu_tuple_cost')::float8, "
		"       current_setting('cpu_operator_cost')::float8",
		true,
		1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		settings.seq_page_cost =
			spi_get_float8(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		settings.cpu_tuple_cost =
			spi_get_float8(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
		settings.cpu_operator_cost =
			spi_get_float8(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	}

	return settings;
}

static double
advisor_estimate_seq_scan_cost(double row_count, double relpages,
							   AdvisorCostSettings settings)
{
	double		rows = Max(row_count, 1.0);
	double		pages = Max(relpages, 1.0);

	return pages * settings.seq_page_cost +
		rows * settings.cpu_tuple_cost;
}

static double
advisor_estimate_btree_create_cost(double row_count, double relpages,
								   AdvisorCostSettings settings)
{
	double		rows = Max(row_count, 1.0);
	double		pages = Max(relpages, 1.0);
	double		table_scan_cost;
	double		key_cpu_cost;
	double		sort_build_cost;

	table_scan_cost = pages * settings.seq_page_cost +
		rows * settings.cpu_tuple_cost;
	key_cpu_cost = rows * settings.cpu_operator_cost;
	sort_build_cost = rows * log(Max(rows, 2.0)) / log(2.0) *
		settings.cpu_operator_cost;

	return table_scan_cost + key_cpu_cost + sort_build_cost;
}

static double
advisor_estimate_write_maint_cost(AdvisorColumnStat *stat,
								  double row_count,
								  AdvisorCostSettings settings)
{
	double		rows = Max(row_count, 1.0);
	double		c_insert;
	double		c_update;

	c_insert = log(Max(rows, 2.0)) / log(2.0) * settings.cpu_operator_cost +
		settings.cpu_tuple_cost;
	c_update = 2.0 * c_insert;

	return stat->insert_count * c_insert +
		stat->update_count * c_update +
		stat->delete_count * c_insert +
		(stat->insert_count + stat->update_count + stat->delete_count) *
		settings.cpu_operator_cost;
}

static bool
advisor_hypopg_available(void)
{
	int			ret;

	ret = SPI_execute(
		"SELECT 1 FROM pg_available_extensions WHERE name = 'hypopg'",
		true,
		1);

	return ret == SPI_OK_SELECT && SPI_processed == 1;
}

static bool
advisor_ensure_hypopg(void)
{
	int			ret;

	if (!advisor_hypopg_available())
		return false;

	ret = SPI_execute(
		"SELECT 1 FROM pg_extension WHERE extname = 'hypopg'",
		true,
		1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
		return true;

	ret = SPI_execute("CREATE EXTENSION hypopg", false, 0);
	return ret == SPI_OK_UTILITY;
}

static void
advisor_ensure_workload_table(void)
{
	int			ret;
	StringInfoData sql;

	if (auto_index_advisor_workload_table == NULL ||
		auto_index_advisor_workload_table[0] == '\0')
		elog(ERROR, "auto_index_advisor.workload_table is empty");

	if (advisor_relation_exists(auto_index_advisor_workload_table))
		return;

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE TABLE ");
	append_qualified_identifier(&sql, auto_index_advisor_workload_table);
	appendStringInfo(&sql,
					 " ("
					 "query_id bigserial PRIMARY KEY, "
					 "query_text text NOT NULL, "
					 "enabled bool NOT NULL DEFAULT true, "
					 "created_at timestamptz NOT NULL DEFAULT now()"
					 ")");

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "auto_index_advisor: could not create workload table");

	pfree(sql.data);
}

static int
advisor_workload_query_count(void)
{
	int			ret;
	StringInfoData sql;

	advisor_ensure_workload_table();

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*)::int FROM ");
	append_qualified_identifier(&sql, auto_index_advisor_workload_table);
	appendStringInfo(&sql, " WHERE enabled");

	ret = SPI_execute(sql.data, true, 1);
	pfree(sql.data);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
		return 0;

	return DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1,
									   &(bool){false}));
}

static void
advisor_capture_workload_query(Query *parse, const char *query_string)
{
	int			ret;
	uint64		inserted = 0;
	Oid			target_relid;
	StringInfoData sql;
	bool		old_internal_query;

	if (auto_index_advisor_in_internal_query ||
		!auto_index_advisor_capture_workload ||
		query_string == NULL ||
		query_string[0] == '\0')
		return;

	if (!advisor_query_mentions_target(query_string))
		return;

	old_internal_query = advisor_enter_internal_query();
	if (SPI_connect() != SPI_OK_CONNECT)
	{
		advisor_leave_internal_query(old_internal_query);
		return;
	}

	PG_TRY();
	{
		target_relid = advisor_lookup_target_relid();
		if (advisor_query_references_target(parse, target_relid))
		{
			advisor_ensure_workload_table();

			initStringInfo(&sql);
			appendStringInfo(&sql, "INSERT INTO ");
			append_qualified_identifier(&sql, auto_index_advisor_workload_table);
			appendStringInfo(&sql,
							 " (query_text, enabled) "
							 "SELECT %s, true "
							 "WHERE NOT EXISTS (SELECT 1 FROM ",
							 quote_literal_cstr(query_string));
			append_qualified_identifier(&sql, auto_index_advisor_workload_table);
			appendStringInfo(&sql,
							 " WHERE query_text = %s)",
							 quote_literal_cstr(query_string));

			ret = SPI_execute(sql.data, false, 0);
			if (ret == SPI_OK_INSERT)
				inserted = SPI_processed;

			pfree(sql.data);

			if (inserted > 0)
				advisor_logf("WORKLOAD_CAPTURE",
							 "WORKLOAD_CAPTURE target=%s query=%s",
							 auto_index_advisor_target_table,
							 query_string);
		}
	}
	PG_FINALLY();
	{
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
	}
	PG_END_TRY();
}

static PlannedStmt *
auto_index_advisor_planner(Query *parse,
						   const char *query_string,
						   int cursorOptions,
#if PG_VERSION_NUM >= 150000
						   ParamListInfo boundParams,
						   struct ExplainState *es)
#else
							   ParamListInfo boundParams)
#endif
{
	PlannedStmt *result;

	if (parse != NULL &&
		advisor_is_online_query_command(parse->commandType) &&
		!auto_index_advisor_in_internal_query)
	{
		advisor_capture_workload_query(parse, query_string);
		(void) advisor_preplan_current_query(parse, query_string);
	}

	if (prev_planner_hook != NULL)
	{
#if PG_VERSION_NUM >= 150000
		result = prev_planner_hook(parse, query_string, cursorOptions, boundParams, es);
#else
		result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
#endif
		advisor_flush_pending_query_decision();
		return result;
	}

#if PG_VERSION_NUM >= 150000
	result = standard_planner(parse, query_string, cursorOptions, boundParams, es);
#else
	result = standard_planner(parse, query_string, cursorOptions, boundParams);
#endif
	advisor_flush_pending_query_decision();
	return result;
}

static void
auto_index_advisor_ExecutorRun(QueryDesc *queryDesc,
							   ScanDirection direction,
							   uint64 count)
{
	bool		track_timing;
	TimestampTz start_time = 0;

	track_timing = auto_index_advisor_enabled &&
		!auto_index_advisor_in_internal_query &&
		queryDesc != NULL &&
		queryDesc->sourceText != NULL &&
		queryDesc->sourceText[0] != '\0';

	if (track_timing)
	{
		if (!advisor_workflow_exit_registered)
		{
			on_proc_exit(advisor_workflow_summary_on_exit, 0);
			advisor_workflow_exit_registered = true;
		}
		start_time = GetCurrentTimestamp();
	}

	PG_TRY();
	{
		if (prev_ExecutorRun_hook != NULL)
			prev_ExecutorRun_hook(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_CATCH();
	{
		if (track_timing)
			advisor_record_query_execution(start_time, GetCurrentTimestamp());
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (track_timing)
		advisor_record_query_execution(start_time, GetCurrentTimestamp());
}

static double
advisor_explain_cost(const char *query_text)
{
	int			ret;
	double		cost;
	char	   *plan_json;
	StringInfoData sql;
	bool		old_internal_query = auto_index_advisor_in_internal_query;

	initStringInfo(&sql);
	appendStringInfo(&sql, "EXPLAIN (FORMAT JSON) %s", query_text);

	old_internal_query = advisor_enter_internal_query();
	ret = SPI_execute(sql.data, false, 1);
	advisor_leave_internal_query(old_internal_query);
	if ((ret != SPI_OK_SELECT && ret != SPI_OK_UTILITY) || SPI_processed != 1)
	{
		pfree(sql.data);
		return -1.0;
	}

	plan_json = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	cost = advisor_parse_total_cost(plan_json);
	pfree(sql.data);
	return cost;
}

static AdvisorCostResult
advisor_cost_candidate(AdvisorColumnStat *stat,
					   const char *index_sql,
					   double activity_count,
					   double total_count,
					   double row_count,
					   double relpages,
					   AdvisorCostSettings settings)
{
	int			ret;
	AdvisorCostResult result = {0};
	StringInfoData sql;

	result.status = "not_costed";
	if (!auto_index_advisor_enable_hypopg_costing)
	{
		result.status = "disabled";
		return result;
	}

	if (auto_index_advisor_workload_table == NULL ||
		auto_index_advisor_workload_table[0] == '\0')
	{
		result.status = "no_workload_table";
		return result;
	}

	advisor_ensure_workload_table();

	if (!advisor_ensure_hypopg())
	{
		result.status = "hypopg_unavailable";
		return result;
	}

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT query_text FROM ");
	append_qualified_identifier(&sql, auto_index_advisor_workload_table);
	appendStringInfo(&sql, " WHERE enabled ORDER BY query_id");

	ret = SPI_execute(sql.data, true, 0);
	pfree(sql.data);
	if (ret != SPI_OK_SELECT)
	{
		result.status = "workload_query_failed";
		return result;
	}

	if (SPI_processed == 0)
	{
		result.status = "no_workload_queries";
		return result;
	}

	{
		uint64		nqueries = SPI_processed;
		char	  **queries = palloc0(sizeof(char *) * nqueries);

		for (uint64 i = 0; i < nqueries; i++)
			queries[i] = spi_get_cstring(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc,
										 1);

		for (uint64 i = 0; i < nqueries; i++)
		{
			double		baseline_cost;
			double		hypo_cost;
			StringInfoData create_sql;

			SPI_execute("SELECT hypopg_reset()", false, 0);
			baseline_cost = advisor_explain_cost(queries[i]);
			if (baseline_cost < 0.0)
			{
				result.status = "baseline_explain_failed";
				SPI_execute("SELECT hypopg_reset()", false, 0);
				return result;
			}

			initStringInfo(&create_sql);
			appendStringInfo(&create_sql,
							 "SELECT * FROM hypopg_create_index(%s)",
							 quote_literal_cstr(index_sql));
			ret = SPI_execute(create_sql.data, false, 0);
			pfree(create_sql.data);
			if (ret != SPI_OK_SELECT)
			{
				result.status = "hypopg_create_failed";
				SPI_execute("SELECT hypopg_reset()", false, 0);
				return result;
			}

			hypo_cost = advisor_explain_cost(queries[i]);
			if (hypo_cost < 0.0)
			{
				result.status = "hypo_explain_failed";
				SPI_execute("SELECT hypopg_reset()", false, 0);
				return result;
			}

			result.baseline_scan_cost += baseline_cost;
			result.hypo_scan_cost += hypo_cost;
			result.query_count++;

			SPI_execute("SELECT hypopg_reset()", false, 0);
		}
	}

	result.create_index_cost =
		advisor_estimate_btree_create_cost(row_count, relpages, settings);
	result.write_maint_cost =
		advisor_estimate_write_maint_cost(stat, row_count, settings);
	result.expected_without_index = result.baseline_scan_cost *
		Max(total_count, 0.0);
	result.expected_with_index = result.create_index_cost +
		result.hypo_scan_cost * Max(activity_count, 0.0) +
		result.write_maint_cost;
	result.improves_expected =
		result.expected_with_index < result.expected_without_index;
	result.status = "costed";

	return result;
}

static AdvisorCostResult
advisor_cost_current_query_candidate(AdvisorColumnStat *stat,
									 const char *query_string,
									 const char *index_sql,
									 double baseline_cost,
									 double activity_count,
									 double total_count,
									 double row_count,
									 double relpages,
									 AdvisorCostSettings settings)
{
	int			ret;
	AdvisorCostResult result = {0};
	StringInfoData sql;

	result.status = "not_costed";

	if (!auto_index_advisor_enable_hypopg_costing)
	{
		result.status = "disabled";
		return result;
	}

	if (query_string == NULL || query_string[0] == '\0')
	{
		result.status = "no_query_text";
		return result;
	}

	if (!advisor_ensure_hypopg())
	{
		result.status = "hypopg_unavailable";
		return result;
	}

	SPI_execute("SELECT hypopg_reset()", false, 0);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * FROM hypopg_create_index(%s)",
					 quote_literal_cstr(index_sql));
	ret = SPI_execute(sql.data, false, 0);
	pfree(sql.data);
	if (ret != SPI_OK_SELECT)
	{
		result.status = "hypopg_create_failed";
		SPI_execute("SELECT hypopg_reset()", false, 0);
		return result;
	}

	result.hypo_scan_cost = advisor_explain_cost(query_string);
	if (result.hypo_scan_cost < 0.0)
	{
		result.status = "hypo_explain_failed";
		SPI_execute("SELECT hypopg_reset()", false, 0);
		return result;
	}

	SPI_execute("SELECT hypopg_reset()", false, 0);

	result.query_count = 1;
	result.baseline_scan_cost = baseline_cost;
	result.create_index_cost =
		advisor_estimate_btree_create_cost(row_count, relpages, settings);
	result.write_maint_cost =
		advisor_estimate_write_maint_cost(stat, row_count, settings);
	result.expected_without_index = Max(total_count, 0.0) * baseline_cost;
	result.expected_with_index = result.create_index_cost +
		Max(activity_count, 0.0) * result.hypo_scan_cost +
		result.write_maint_cost;
	result.improves_expected =
		result.expected_with_index < result.expected_without_index;
	result.status = "costed";

	return result;
}

static bool
advisor_launch_create_index_worker(Oid relid, AttrNumber attno,
								   const char *column_name)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;
	AdvisorDynamicIndexJob job;
	BgwHandleStatus status;
	pid_t		pid;

	memset(&job, 0, sizeof(job));
	job.dboid = MyDatabaseId;
	job.roleid = GetUserId();
	job.relid = relid;
	job.attno = (int32) attno;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "auto_index_advisor");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "auto_index_advisor_create_index_worker");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "auto_index_advisor create index");
	snprintf(worker.bgw_type, BGW_MAXLEN,
			 "auto_index_advisor create index");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = MyProcPid;
	memcpy(worker.bgw_extra, &job, sizeof(job));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		advisor_logf("PREPLAN_CREATE_INDEX",
					 "PREPLAN_CREATE_INDEX decision=create_failed column=%s reason=worker_registration_failed",
					 column_name ? column_name : "");
		ereport(LOG,
				(errmsg("AUTO_INDEX_ADVISOR_PREPLAN_CREATE status=worker_registration_failed column=%s",
						column_name ? column_name : "")));
		return false;
	}

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
	{
		advisor_logf("PREPLAN_CREATE_INDEX",
					 "PREPLAN_CREATE_INDEX decision=create_failed column=%s reason=worker_start_failed status=%d",
					 column_name ? column_name : "",
					 status);
		return false;
	}

	status = WaitForBackgroundWorkerShutdown(handle);
	AcceptInvalidationMessages();
	if (status != BGWH_STOPPED)
	{
		advisor_logf("PREPLAN_CREATE_INDEX",
					 "PREPLAN_CREATE_INDEX decision=create_failed column=%s reason=worker_shutdown_failed status=%d",
					 column_name ? column_name : "",
					 status);
		return false;
	}

	advisor_logf("PREPLAN_CREATE_INDEX",
				 "PREPLAN_CREATE_INDEX decision=worker_finished column=%s pid=%d",
				 column_name ? column_name : "",
				 (int) pid);
	return true;
}

static bool
advisor_preplan_current_query(Query *parse, const char *query_string)
{
	int			ret;
	uint64		ntup;
	AdvisorColumnStat *stats;
	double		total_accesses = 0.0;
	double		total_activity_count = 0.0;
	double		row_count = 0.0;
	double		relpages = 0.0;
	double		threshold;
	double		baseline_cost;
	double		cost_without_index;
	AdvisorCostSettings cost_settings;
	Oid			target_relid;
	char	   *schema_name;
	char	   *table_name;
	bool		have_best = false;
	uint64		best_index = 0;
	double		best_activity_count = 0.0;
	AdvisorCostResult best_cost = {0};
	bool		best_real_index_exists = false;
	char	   *best_real_index_name = NULL;
	char	   *best_candidate_index_name = NULL;
	bool		index_ready = false;
	bool		old_internal_query;
	uint64		query_no;
	StringInfoData sql;

	if (!auto_index_advisor_enabled ||
		auto_index_advisor_in_internal_query ||
		parse == NULL ||
		!advisor_is_online_query_command(parse->commandType) ||
		query_string == NULL ||
		query_string[0] == '\0' ||
		auto_index_advisor_target_table == NULL ||
		auto_index_advisor_target_table[0] == '\0' ||
		auto_index_advisor_stats_table == NULL ||
		auto_index_advisor_stats_table[0] == '\0')
		return false;

	if (!advisor_query_mentions_target(query_string))
		return false;

	query_no = ++auto_index_advisor_preplan_query_counter;

	old_internal_query = advisor_enter_internal_query();

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		advisor_query_decision_log(query_no,
								   query_string,
								   NULL,
								   false,
								   false,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   "no_index",
								   "spi_connect_failed",
								   NULL);
		advisor_leave_internal_query(old_internal_query);
		return false;
	}

	if (!advisor_relation_exists(auto_index_advisor_stats_table))
	{
		advisor_query_decision_log(query_no,
								   query_string,
								   NULL,
								   false,
								   false,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   "no_index",
								   "stats_table_missing",
								   NULL);
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		return false;
	}

	target_relid = advisor_lookup_target_relid();
	if (!advisor_query_references_target(parse, target_relid))
	{
		advisor_query_decision_log(query_no,
								   query_string,
								   NULL,
								   false,
								   false,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   "no_index",
								   "query_does_not_reference_target",
								   NULL);
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		return false;
	}
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT c.oid, n.nspname::text, c.relname::text, "
					 "       c.reltuples::float8, c.relpages::float8 "
					 "FROM pg_class c "
					 "JOIN pg_namespace n ON n.oid = c.relnamespace "
					 "WHERE c.oid = %u "
					 "  AND c.relkind IN ('r', 'p', 'm')",
					 target_relid);

	ret = SPI_execute(sql.data, true, 1);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		advisor_query_decision_log(query_no,
								   query_string,
								   NULL,
								   false,
								   false,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   "no_index",
								   "target_table_lookup_failed",
								   NULL);
		pfree(sql.data);
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		return false;
	}

	schema_name = spi_get_cstring(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	table_name = spi_get_cstring(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	row_count = Max(spi_get_float8(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4), 1.0);
	relpages = spi_get_float8(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5);

	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT a.attname::text, a.attnum::int, "
					 "       COALESCE(s.access_count, 0)::float8, "
					 "       COALESCE(s.total_count, 0)::float8, "
					 "       COALESCE(s.insert_count, 0)::float8, "
					 "       COALESCE(s.update_count, 0)::float8, "
					 "       COALESCE(s.delete_count, 0)::float8, "
					 "       COALESCE(s.n_distinct, pg.n_distinct, 0)::float8 "
					 "FROM pg_attribute a "
					 "LEFT JOIN pg_stats pg "
					 "  ON pg.schemaname = %s "
					 " AND pg.tablename = %s "
					 " AND pg.attname = a.attname "
					 "LEFT JOIN ",
					 quote_literal_cstr(schema_name),
					 quote_literal_cstr(table_name));
	append_qualified_identifier(&sql, auto_index_advisor_stats_table);
	appendStringInfo(&sql,
					 " s ON lower(s.table_schema) = lower(%s) "
					 "  AND lower(s.table_name) = lower(%s) "
					 "  AND lower(s.column_name::text) = lower(a.attname) "
					 "WHERE a.attrelid = %u "
					 "  AND a.attnum > 0 "
					 "  AND NOT a.attisdropped "
					 "ORDER BY a.attnum",
					 quote_literal_cstr(schema_name),
					 quote_literal_cstr(table_name),
					 target_relid);

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		advisor_query_decision_log(query_no,
								   query_string,
								   NULL,
								   false,
								   false,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   "no_index",
								   "column_stats_lookup_failed",
								   NULL);
		pfree(sql.data);
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		return false;
	}

	ntup = SPI_processed;
	if (ntup == 0)
	{
		advisor_query_decision_log(query_no,
								   query_string,
								   NULL,
								   false,
								   false,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   "no_index",
								   "no_columns",
								   NULL);
		pfree(sql.data);
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		return false;
	}

	stats = palloc0(sizeof(AdvisorColumnStat) * ntup);
	for (uint64 i = 0; i < ntup; i++)
	{
		HeapTuple	tuple = SPI_tuptable->vals[i];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;

		stats[i].column_name = spi_get_cstring(tuple, tupdesc, 1);
		stats[i].attno = (AttrNumber) DatumGetInt32(SPI_getbinval(tuple,
																	tupdesc,
																	2,
																	&(bool){false}));
		stats[i].access_count = spi_get_float8(tuple, tupdesc, 3);
		stats[i].total_count = spi_get_float8(tuple, tupdesc, 4);
		stats[i].insert_count = spi_get_float8(tuple, tupdesc, 5);
		stats[i].update_count = spi_get_float8(tuple, tupdesc, 6);
		stats[i].delete_count = spi_get_float8(tuple, tupdesc, 7);
		stats[i].n_distinct = spi_get_float8(tuple, tupdesc, 8);
		total_activity_count += stats[i].access_count +
			stats[i].insert_count + stats[i].update_count +
			stats[i].delete_count;
	}
	total_accesses = total_activity_count;

	threshold = Max(auto_index_advisor_min_access_fraction,
					2.0 / Max((double) ntup, 1.0));
	cost_settings = advisor_get_cost_settings();
	baseline_cost = advisor_estimate_seq_scan_cost(row_count, relpages,
												   cost_settings);
	cost_without_index = baseline_cost * Max(total_accesses, 0.0);

	advisor_logf("PREPLAN_BEGIN",
				 "PREPLAN_BEGIN target=%s rows=%.0f relpages=%.0f columns=%llu total_count=%.0f threshold=%.4f baseline_cost=%.2f cost_without_index=%.2f",
				 auto_index_advisor_target_table,
				 row_count,
				 relpages,
				 (unsigned long long) ntup,
				 total_accesses,
				 threshold,
				 baseline_cost,
				 cost_without_index);

	for (uint64 i = 0; i < ntup; i++)
	{
		double		distinct_count;
		double		activity_count;
		double		activity_fraction;
		char	   *index_name;
		char	   *index_sql;
		char	   *real_index_name = NULL;
		bool		real_index_exists;
		AdvisorCostResult cost_result = {0};

		stats[i].access_fraction =
			total_accesses > 0.0 ? stats[i].access_count / total_accesses : 0.0;
		stats[i].insert_fraction =
			total_accesses > 0.0 ? stats[i].insert_count / total_accesses : 0.0;
		stats[i].update_fraction =
			total_accesses > 0.0 ? stats[i].update_count / total_accesses : 0.0;
		stats[i].delete_fraction =
			total_accesses > 0.0 ? stats[i].delete_count / total_accesses : 0.0;

		distinct_count = advisor_distinct_count(stats[i].n_distinct, row_count);
		stats[i].distinct_ratio = distinct_count / Max(row_count, 1.0);
		activity_count = stats[i].access_count + stats[i].insert_count +
			stats[i].update_count + stats[i].delete_count;
		activity_fraction = total_accesses > 0.0 ?
			activity_count / total_accesses : 0.0;

		if (total_accesses <= 0.0 || activity_fraction <= threshold)
		{
			advisor_logf("PREPLAN_CANDIDATE_SKIP",
						 "PREPLAN_CANDIDATE_SKIP column=%s reason=access_fraction all_count=%.0f all_fraction=%.4f threshold=%.4f distinct_ratio=%.4f",
						 stats[i].column_name,
						 activity_count,
						 activity_fraction,
						 threshold,
						 stats[i].distinct_ratio);
				advisor_query_column_log(query_no,
										 stats[i].column_name,
										 stats[i].access_count,
										 stats[i].update_count,
										 stats[i].insert_count,
										 stats[i].delete_count,
										 false,
										 activity_count,
										 total_accesses,
									 activity_fraction,
									 threshold,
									 cost_without_index,
									 false,
									 0.0,
									 0.0,
									 0.0,
									 0.0,
									 total_accesses <= 0.0 ?
									 "zero_total_count" : "below_activity_threshold",
									 NULL);
			continue;
		}

		if (stats[i].distinct_ratio <= auto_index_advisor_min_distinct_ratio)
		{
			advisor_logf("PREPLAN_CANDIDATE_SKIP",
						 "PREPLAN_CANDIDATE_SKIP column=%s reason=distinct_ratio all_count=%.0f all_fraction=%.4f threshold=%.4f distinct_ratio=%.4f",
						 stats[i].column_name,
						 activity_count,
						 activity_fraction,
						 threshold,
						 stats[i].distinct_ratio);
				advisor_query_column_log(query_no,
										 stats[i].column_name,
										 stats[i].access_count,
										 stats[i].update_count,
										 stats[i].insert_count,
										 stats[i].delete_count,
										 false,
										 activity_count,
										 total_accesses,
									 activity_fraction,
									 threshold,
									 cost_without_index,
									 false,
									 0.0,
									 0.0,
									 0.0,
									 0.0,
									 "below_distinct_ratio",
									 NULL);
			continue;
		}

		index_name = advisor_index_name(table_name, stats[i].column_name);
		index_sql = advisor_create_index_sql(schema_name,
											 table_name,
											 stats[i].column_name,
											 index_name);
		real_index_exists = advisor_find_single_column_index(target_relid,
															 stats[i].attno,
															 &real_index_name);
		cost_result = advisor_cost_current_query_candidate(&stats[i],
														   query_string,
														   index_sql,
														   baseline_cost,
														   activity_count,
														   total_accesses,
														   row_count,
														   relpages,
														   cost_settings);
		advisor_logf("PREPLAN_CANDIDATE_KEEP",
					 "PREPLAN_CANDIDATE_KEEP column=%s real_index_exists=%s all_count=%.0f all_fraction=%.4f threshold=%.4f distinct_ratio=%.4f cost_status=%s baseline_cost=%.2f hypo_scan_cost=%.2f create_cost=%.2f write_cost=%.2f expected_without_index=%.2f expected_with_index=%.2f improves_expected=%s candidate_index=%s existing_index=%s",
					 stats[i].column_name,
					 real_index_exists ? "true" : "false",
					 activity_count,
					 activity_fraction,
					 threshold,
					 stats[i].distinct_ratio,
					 cost_result.status,
					 cost_result.baseline_scan_cost,
					 cost_result.hypo_scan_cost,
					 cost_result.create_index_cost,
					 cost_result.write_maint_cost,
					 cost_result.expected_without_index,
					 cost_result.expected_with_index,
					 cost_result.improves_expected ? "true" : "false",
					 index_name,
					 real_index_name != NULL ? real_index_name : "");
			advisor_query_column_log(query_no,
									 stats[i].column_name,
									 stats[i].access_count,
									 stats[i].update_count,
									 stats[i].insert_count,
									 stats[i].delete_count,
									 true,
									 activity_count,
								 total_accesses,
								 activity_fraction,
								 threshold,
								 cost_without_index,
								 strcmp(cost_result.status, "costed") == 0,
								 cost_result.expected_with_index,
								 cost_result.create_index_cost,
								 cost_result.hypo_scan_cost * activity_count,
								 cost_result.write_maint_cost,
								 cost_result.status,
								 index_name);

		if (strcmp(cost_result.status, "costed") == 0 &&
			(!have_best ||
			 cost_result.expected_with_index < best_cost.expected_with_index))
		{
			have_best = true;
			best_index = i;
			best_activity_count = activity_count;
			best_cost = cost_result;
			best_real_index_exists = real_index_exists;
			best_real_index_name = real_index_name;
			best_candidate_index_name = index_name;
		}
	}

	if (!have_best)
	{
		advisor_logf("PREPLAN_DECISION",
					 "PREPLAN_DECISION decision=use_normal_planner reason=no_candidate");
		advisor_query_decision_log(query_no,
								   query_string,
								   NULL,
								   true,
								   false,
								   cost_without_index,
								   0.0,
								   0.0,
								   0.0,
								   0.0,
								   "no_index",
								   "no_candidate",
								   NULL);
		pfree(sql.data);
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		return false;
	}

	if (best_cost.expected_with_index >= best_cost.expected_without_index)
	{
		advisor_logf("PREPLAN_DECISION",
					 "PREPLAN_DECISION decision=use_normal_planner reason=without_index_cost_is_lower column=%s without_index_cost=%.2f best_with_index_cost=%.2f best_create_cost=%.2f best_read_cost=%.2f best_write_cost=%.2f",
					 stats[best_index].column_name,
					 best_cost.expected_without_index,
					 best_cost.expected_with_index,
					 best_cost.create_index_cost,
					 best_cost.hypo_scan_cost * best_activity_count,
					 best_cost.write_maint_cost);
		advisor_query_decision_log(query_no,
								   query_string,
								   stats[best_index].column_name,
								   true,
								   true,
								   best_cost.expected_without_index,
								   best_cost.expected_with_index,
								   best_cost.create_index_cost,
								   best_cost.hypo_scan_cost * best_activity_count,
								   best_cost.write_maint_cost,
								   "no_index",
								   "without_index_cost_is_lower",
								   best_candidate_index_name);
		pfree(sql.data);
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		return false;
	}

	if (best_real_index_exists)
	{
		advisor_logf("PREPLAN_DECISION",
					 "PREPLAN_DECISION decision=use_existing_index column=%s existing_index=%s without_index_cost=%.2f best_with_index_cost=%.2f best_create_cost=%.2f best_read_cost=%.2f best_write_cost=%.2f",
					 stats[best_index].column_name,
					 best_real_index_name != NULL ? best_real_index_name : "",
					 best_cost.expected_without_index,
					 best_cost.expected_with_index,
					 best_cost.create_index_cost,
					 best_cost.hypo_scan_cost * best_activity_count,
					 best_cost.write_maint_cost);
		advisor_query_decision_log(query_no,
								   query_string,
								   stats[best_index].column_name,
								   true,
								   true,
								   best_cost.expected_without_index,
								   best_cost.expected_with_index,
								   best_cost.create_index_cost,
								   best_cost.hypo_scan_cost * best_activity_count,
								   best_cost.write_maint_cost,
								   "used_existing_index",
								   "real_index_already_exists",
								   best_real_index_name);
		index_ready = true;
	}
	else
	{
		const char *create_reason = "create_index_failed";

		advisor_logf("PREPLAN_DECISION",
					 "PREPLAN_DECISION decision=create_index column=%s without_index_cost=%.2f best_with_index_cost=%.2f best_create_cost=%.2f best_read_cost=%.2f best_write_cost=%.2f",
					 stats[best_index].column_name,
					 best_cost.expected_without_index,
					 best_cost.expected_with_index,
					 best_cost.create_index_cost,
					 best_cost.hypo_scan_cost * best_activity_count,
					 best_cost.write_maint_cost);
		index_ready = advisor_launch_create_index_worker(target_relid,
														 stats[best_index].attno,
														 stats[best_index].column_name);
		if (index_ready &&
			!advisor_find_single_column_index(target_relid,
											  stats[best_index].attno,
											  NULL))
			{
				advisor_logf("PREPLAN_CREATE_INDEX",
							 "PREPLAN_CREATE_INDEX decision=create_failed column=%s reason=index_not_visible_after_worker",
							 stats[best_index].column_name);
				index_ready = false;
				create_reason = "background_worker_created_index_visible_to_later_queries";
			}
		else if (index_ready)
			create_reason = "created_by_background_worker";
		if (index_ready)
			advisor_logf("PREPLAN_CREATED_INDEXES",
						 "PREPLAN_CREATED_INDEXES indexes=[%s] columns=[%s]",
						 best_candidate_index_name != NULL ?
						 best_candidate_index_name : "",
						 stats[best_index].column_name);
		advisor_query_decision_log(query_no,
								   query_string,
								   stats[best_index].column_name,
								   true,
								   true,
								   best_cost.expected_without_index,
								   best_cost.expected_with_index,
								   best_cost.create_index_cost,
								   best_cost.hypo_scan_cost * best_activity_count,
								   best_cost.write_maint_cost,
								   index_ready ? "created_index" : "no_index",
								   create_reason,
								   best_candidate_index_name);
	}

	pfree(sql.data);
	SPI_finish();
	advisor_leave_internal_query(old_internal_query);
	return index_ready;
}

static void
advisor_ensure_recommendation_table(void)
{
	int			ret;
	StringInfoData sql;

	if (auto_index_advisor_recommendation_table == NULL ||
		auto_index_advisor_recommendation_table[0] == '\0')
		elog(ERROR, "auto_index_advisor.recommendation_table is empty");

	if (!advisor_relation_exists(auto_index_advisor_recommendation_table))
	{
		initStringInfo(&sql);
		appendStringInfo(&sql, "CREATE TABLE ");
		append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
		appendStringInfo(&sql,
						 " ("
						 "table_schema text NOT NULL, "
						 "table_name text NOT NULL, "
						 "column_name text NOT NULL, "
						 "decision text NOT NULL, "
						 "reason text NOT NULL, "
						 "access_count float8 NOT NULL, "
						 "insert_count float8 NOT NULL, "
						 "update_count float8 NOT NULL, "
						 "delete_count float8 NOT NULL, "
						 "all_count float8 NOT NULL, "
						 "access_fraction float8 NOT NULL, "
						 "insert_fraction float8 NOT NULL, "
						 "update_fraction float8 NOT NULL, "
						 "delete_fraction float8 NOT NULL, "
						 "all_fraction float8 NOT NULL, "
						 "threshold float8 NOT NULL, "
						 "n_distinct float8 NOT NULL, "
						 "distinct_ratio float8 NOT NULL, "
						 "recommended_index_name text, "
						 "recommended_index_sql text, "
						 "cost_status text NOT NULL DEFAULT 'not_costed', "
						 "workload_query_count integer NOT NULL DEFAULT 0, "
						 "baseline_scan_cost float8, "
						 "hypo_scan_cost float8, "
						 "create_index_cost float8, "
						 "write_maint_cost float8, "
						 "expected_without_index float8, "
						 "expected_with_index float8, "
						 "improves_expected bool, "
						 "create_status text NOT NULL DEFAULT 'not_applied', "
						 "created_index_name text, "
						 "created_index_sql text, "
						 "created_at timestamptz, "
						 "updated_at timestamptz NOT NULL DEFAULT now(), "
						 "PRIMARY KEY (table_schema, table_name, column_name)"
						 ")");

		ret = SPI_execute(sql.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(ERROR, "auto_index_advisor: could not create recommendation table");
		pfree(sql.data);
		return;
	}

	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "cost_status",
								  "text NOT NULL DEFAULT 'not_costed'");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "workload_query_count",
								  "integer NOT NULL DEFAULT 0");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "baseline_scan_cost",
								  "float8");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "hypo_scan_cost",
								  "float8");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "create_index_cost",
								  "float8");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "write_maint_cost",
								  "float8");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "expected_without_index",
								  "float8");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "expected_with_index",
								  "float8");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "improves_expected",
								  "bool");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "create_status",
								  "text NOT NULL DEFAULT 'not_applied'");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "created_index_name",
								  "text");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "created_index_sql",
								  "text");
	advisor_add_column_if_missing(auto_index_advisor_recommendation_table,
								  "created_at",
								  "timestamptz");
}

static void
advisor_clear_recommendations(const char *schema_name, const char *table_name)
{
	int			ret;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql, "DELETE FROM ");
	append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
	appendStringInfo(&sql,
					 " WHERE lower(table_schema) = lower(%s) "
					 "   AND lower(table_name) = lower(%s)",
					 quote_literal_cstr(schema_name),
					 quote_literal_cstr(table_name));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "auto_index_advisor: could not clear old recommendations");

	pfree(sql.data);
}

static char *
advisor_create_index_if_not_exists_sql(const char *schema_name,
									   const char *table_name,
									   const char *column_name,
									   const char *index_name)
{
	return psprintf("CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s)",
					quote_identifier(index_name),
					quote_identifier(schema_name),
					quote_identifier(table_name),
					quote_identifier(column_name));
}

static bool
advisor_create_index_for_rel_att(Oid relid, AttrNumber attno)
{
	int			ret;
	char	   *schema_name;
	char	   *table_name;
	char	   *column_name;
	char	   *index_name;
	char	   *qualified_index_name;
	char	   *create_sql;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT n.nspname::text, c.relname::text, a.attname::text "
					 "FROM pg_class c "
					 "JOIN pg_namespace n ON n.oid = c.relnamespace "
					 "JOIN pg_attribute a ON a.attrelid = c.oid "
					 "WHERE c.oid = %u "
					 "  AND a.attnum = %d "
					 "  AND NOT a.attisdropped",
					 relid,
					 attno);

	ret = SPI_execute(sql.data, true, 1);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		pfree(sql.data);
		ereport(LOG,
				(errmsg("AUTO_INDEX_ADVISOR_PREPLAN_CREATE_FAILED relid=%u attno=%d reason=lookup_failed",
						relid,
						attno)));
		return false;
	}

	schema_name = spi_get_cstring(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	table_name = spi_get_cstring(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	column_name = spi_get_cstring(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	index_name = advisor_index_name(table_name, column_name);
	qualified_index_name = psprintf("%s.%s", schema_name, index_name);
	create_sql = advisor_create_index_if_not_exists_sql(schema_name,
														table_name,
														column_name,
														index_name);

	if (advisor_relation_exists(qualified_index_name))
	{
		advisor_logf("PREPLAN_CREATE_INDEX",
					 "PREPLAN_CREATE_INDEX decision=already_exists column=%s index=%s sql=%s",
					 column_name,
					 qualified_index_name,
					 create_sql);
		pfree(sql.data);
		return true;
	}

	ret = SPI_execute(create_sql, false, 0);
	if (ret == SPI_OK_UTILITY)
	{
		advisor_logf("PREPLAN_CREATE_INDEX",
					 "PREPLAN_CREATE_INDEX decision=create column=%s index=%s sql=%s",
					 column_name,
					 qualified_index_name,
					 create_sql);
		ereport(LOG,
				(errmsg("AUTO_INDEX_ADVISOR_PREPLAN_CREATE status=created index=%s sql=%s",
						qualified_index_name,
						create_sql)));
		advisor_log_created_index(qualified_index_name, create_sql);
		pfree(sql.data);
		return true;
	}

	advisor_logf("PREPLAN_CREATE_INDEX",
				 "PREPLAN_CREATE_INDEX decision=create_failed column=%s index=%s sql=%s spi_ret=%d",
				 column_name,
				 qualified_index_name,
				 create_sql,
				 ret);
	ereport(LOG,
			(errmsg("AUTO_INDEX_ADVISOR_PREPLAN_CREATE status=create_failed index=%s sql=%s spi_ret=%d",
					qualified_index_name,
					create_sql,
					ret)));
	pfree(sql.data);
	return false;
}

void
auto_index_advisor_create_index_worker(Datum main_arg)
{
	AdvisorDynamicIndexJob job;
	bool		old_internal_query;

	(void) main_arg;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	memset(&job, 0, sizeof(job));
	memcpy(&job, MyBgworkerEntry->bgw_extra, sizeof(job));

	BackgroundWorkerInitializeConnectionByOid(job.dboid, job.roleid, 0);

	old_internal_query = advisor_enter_internal_query();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	(void) advisor_create_index_for_rel_att(job.relid, (AttrNumber) job.attno);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	advisor_leave_internal_query(old_internal_query);

	proc_exit(0);
}

static void
advisor_mark_create_status(AdvisorApplyCandidate *candidate,
						   const char *status,
						   const char *create_sql)
{
	int			ret;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql, "UPDATE ");
	append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
	appendStringInfo(&sql,
					 " SET create_status = %s, "
					 "     created_index_name = %s, "
					 "     created_index_sql = %s, "
					 "     created_at = CASE WHEN %s IN ('created', 'already_exists') "
					 "                       THEN now() ELSE created_at END, "
					 "     updated_at = now() "
					 " WHERE lower(table_schema) = lower(%s) "
					 "   AND lower(table_name) = lower(%s) "
					 "   AND lower(column_name) = lower(%s)",
					 quote_literal_cstr(status),
					 candidate->index_name != NULL ?
					 quote_literal_cstr(candidate->index_name) : "NULL",
					 create_sql != NULL ? quote_literal_cstr(create_sql) : "NULL",
					 quote_literal_cstr(status),
					 quote_literal_cstr(candidate->schema_name),
					 quote_literal_cstr(candidate->table_name),
					 quote_literal_cstr(candidate->column_name));

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "auto_index_advisor: could not update create status");

	pfree(sql.data);
}

static char *
advisor_drop_existing_indexes(bool emit_log, int *dropped_out)
{
	int			ret;
	int			dropped = 0;
	uint64		nindexes;
	char	  **index_names;
	StringInfoData sql;
	StringInfoData result;

	if (auto_index_advisor_target_table == NULL ||
		auto_index_advisor_target_table[0] == '\0')
		return pstrdup("FAIL: auto_index_advisor.target_table is empty");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT in_ns.nspname::text || '.' || i.relname::text "
					 "FROM pg_index x "
					 "JOIN pg_class t ON t.oid = x.indrelid "
					 "JOIN pg_class i ON i.oid = x.indexrelid "
					 "JOIN pg_namespace in_ns ON in_ns.oid = i.relnamespace "
					 "WHERE t.oid = to_regclass(%s) "
					 "  AND i.relname LIKE 'auto_advisor_%%_idx' "
					 "ORDER BY 1",
					 quote_literal_cstr(auto_index_advisor_target_table));

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		pfree(sql.data);
		return pstrdup("FAIL: could not list advisor indexes");
	}

	nindexes = SPI_processed;
	index_names = palloc0(sizeof(char *) * Max(nindexes, 1));
	for (uint64 i = 0; i < nindexes; i++)
		index_names[i] = spi_get_cstring(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc,
										 1);

	for (uint64 i = 0; i < nindexes; i++)
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql, "DROP INDEX IF EXISTS ");
		append_qualified_identifier(&sql, index_names[i]);

		ret = SPI_execute(sql.data, false, 0);
		if (ret == SPI_OK_UTILITY)
		{
			dropped++;
			advisor_logf("DROP_INDEX",
						 "DROP_INDEX index=%s",
						 index_names[i]);
			if (emit_log)
				ereport(LOG,
						(errmsg("AUTO_INDEX_ADVISOR_DROP_INDEX index=%s",
								index_names[i])));
		}
		else if (emit_log)
			ereport(LOG,
					(errmsg("AUTO_INDEX_ADVISOR_DROP_INDEX_FAILED index=%s spi_ret=%d",
							index_names[i],
							ret)));
	}

	if (advisor_relation_exists(auto_index_advisor_recommendation_table))
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql, "UPDATE ");
		append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
		appendStringInfo(&sql,
						 " SET create_status = 'dropped', "
						 "     updated_at = now() "
						 " WHERE created_index_name IS NOT NULL "
						 "   AND create_status IN ('created', 'already_exists')");
		(void) SPI_execute(sql.data, false, 0);
	}

	initStringInfo(&result);
	appendStringInfo(&result,
					 "OK: dropped %d advisor index(es) for target=%s",
					 dropped,
					 auto_index_advisor_target_table);
	advisor_logf("DROP_SUMMARY",
				 "DROP_SUMMARY target=%s dropped=%d",
				 auto_index_advisor_target_table,
				 dropped);
	if (dropped_out != NULL)
		*dropped_out = dropped;

	pfree(sql.data);
	return result.data;
}

pg_attribute_unused()
static char *
advisor_drop_created_indexes(bool emit_log, int *dropped_out)
{
	int			ret;
	int			dropped = 0;
	uint64		nindexes;
	char	  **index_names;
	StringInfoData sql;
	StringInfoData result;

	advisor_ensure_recommendation_table();

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT table_schema || '.' || created_index_name "
					 "FROM ");
	append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
	appendStringInfo(&sql,
					 " WHERE create_status = 'created' "
					 "   AND created_index_name IS NOT NULL "
					 "ORDER BY 1");

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		pfree(sql.data);
		return pstrdup("FAIL: could not list created advisor indexes");
	}

	nindexes = SPI_processed;
	index_names = palloc0(sizeof(char *) * Max(nindexes, 1));
	for (uint64 i = 0; i < nindexes; i++)
		index_names[i] = spi_get_cstring(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc,
										 1);

	for (uint64 i = 0; i < nindexes; i++)
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql, "DROP INDEX IF EXISTS ");
		append_qualified_identifier(&sql, index_names[i]);

		ret = SPI_execute(sql.data, false, 0);
		if (ret == SPI_OK_UTILITY)
		{
			dropped++;
			advisor_logf("DROP_CREATED_INDEX",
						 "DROP_CREATED_INDEX index=%s",
						 index_names[i]);
			if (emit_log)
				ereport(LOG,
						(errmsg("AUTO_INDEX_ADVISOR_DROP_CREATED_INDEX index=%s",
								index_names[i])));
		}
		else if (emit_log)
			ereport(LOG,
					(errmsg("AUTO_INDEX_ADVISOR_DROP_CREATED_INDEX_FAILED index=%s spi_ret=%d",
							index_names[i],
							ret)));
	}

	resetStringInfo(&sql);
	appendStringInfo(&sql, "UPDATE ");
	append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
	appendStringInfo(&sql,
					 " SET create_status = 'dropped', "
					 "     updated_at = now() "
					 " WHERE created_index_name IS NOT NULL "
					 "   AND create_status = 'created'");
	(void) SPI_execute(sql.data, false, 0);

	initStringInfo(&result);
	appendStringInfo(&result,
					 "OK: dropped %d created advisor index(es) for target=%s",
					 dropped,
					 auto_index_advisor_target_table);
	advisor_logf("DROP_CREATED_SUMMARY",
				 "DROP_CREATED_SUMMARY target=%s dropped=%d",
				 auto_index_advisor_target_table,
				 dropped);
	if (dropped_out != NULL)
		*dropped_out = dropped;

	pfree(sql.data);
	return result.data;
}

static char *
advisor_apply_recommendations(bool emit_log, bool force)
{
	int			ret;
	int			created = 0;
	int			already_exists = 0;
	int			skipped = 0;
	int			dropped_after_apply = 0;
	uint64		ncandidates;
	AdvisorApplyCandidate *candidates;
	StringInfoData sql;
	StringInfoData result;
	StringInfoData created_indexes;
	StringInfoData created_columns;

	if (!force && !auto_index_advisor_auto_create)
		return pstrdup("OK: stage4 apply skipped auto_create=off");

	advisor_ensure_recommendation_table();

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT table_schema, table_name, column_name, "
					 "       recommended_index_name "
					 "FROM ");
	append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
	appendStringInfo(&sql,
					 " WHERE decision = 'KEEP' "
					 "   AND cost_status = 'costed' "
					 "   AND improves_expected IS TRUE "
					 "   AND recommended_index_name IS NOT NULL "
					 "   AND create_status NOT IN ('created', 'already_exists') "
					 "ORDER BY expected_with_index NULLS LAST, all_fraction DESC "
					 "LIMIT %d",
					 auto_index_advisor_max_indexes_per_run);

	ret = SPI_execute(sql.data, true, 0);
	pfree(sql.data);
	if (ret != SPI_OK_SELECT)
		return pstrdup("FAIL: stage4 could not read recommendations");

	ncandidates = SPI_processed;
	candidates = palloc0(sizeof(AdvisorApplyCandidate) * Max(ncandidates, 1));
	initStringInfo(&created_indexes);
	initStringInfo(&created_columns);
	for (uint64 i = 0; i < ncandidates; i++)
	{
		candidates[i].schema_name =
			spi_get_cstring(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
		candidates[i].table_name =
			spi_get_cstring(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
		candidates[i].column_name =
			spi_get_cstring(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
		candidates[i].index_name =
			spi_get_cstring(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4);
	}

	for (uint64 i = 0; i < ncandidates; i++)
	{
		char	   *qualified_index_name;
		char	   *create_sql;

		qualified_index_name = psprintf("%s.%s",
										candidates[i].schema_name,
										candidates[i].index_name);
		create_sql = advisor_create_index_if_not_exists_sql(
			candidates[i].schema_name,
			candidates[i].table_name,
			candidates[i].column_name,
			candidates[i].index_name);

		if (advisor_relation_exists(qualified_index_name))
		{
			advisor_mark_create_status(&candidates[i],
									   "already_exists",
									   create_sql);
			already_exists++;
			advisor_logf("CREATE_INDEX_DECISION",
						 "CREATE_INDEX_DECISION decision=already_exists column=%s index=%s sql=%s",
						 candidates[i].column_name,
						 qualified_index_name,
						 create_sql);
			if (emit_log)
				ereport(LOG,
						(errmsg("AUTO_INDEX_ADVISOR_STAGE4_APPLY "
								"status=already_exists index=%s sql=%s",
								qualified_index_name,
								create_sql)));
			continue;
		}

		ret = SPI_execute(create_sql, false, 0);
		if (ret == SPI_OK_UTILITY)
		{
			advisor_mark_create_status(&candidates[i], "created", create_sql);
			created++;
			if (created_indexes.len > 0)
				appendStringInfoString(&created_indexes, ",");
			if (created_columns.len > 0)
				appendStringInfoString(&created_columns, ",");
			appendStringInfoString(&created_indexes, qualified_index_name);
			appendStringInfoString(&created_columns, candidates[i].column_name);
			advisor_logf("CREATE_INDEX_DECISION",
						 "CREATE_INDEX_DECISION decision=create column=%s index=%s sql=%s",
						 candidates[i].column_name,
						 qualified_index_name,
						 create_sql);
			if (emit_log)
				ereport(LOG,
						(errmsg("AUTO_INDEX_ADVISOR_STAGE4_APPLY "
								"status=created index=%s sql=%s",
								qualified_index_name,
								create_sql)));
		}
		else
		{
			advisor_mark_create_status(&candidates[i], "create_failed", create_sql);
			skipped++;
			advisor_logf("CREATE_INDEX_DECISION",
						 "CREATE_INDEX_DECISION decision=create_failed column=%s index=%s sql=%s spi_ret=%d",
						 candidates[i].column_name,
						 qualified_index_name,
						 create_sql,
						 ret);
			if (emit_log)
				ereport(LOG,
						(errmsg("AUTO_INDEX_ADVISOR_STAGE4_APPLY "
									"status=create_failed index=%s sql=%s spi_ret=%d",
									qualified_index_name,
									create_sql,
								ret)));
		}
	}

	if (auto_index_advisor_drop_indexes_after_apply &&
		(created > 0 || already_exists > 0))
	{
		advisor_logf("DROP_CREATED_INDEX",
					 "DROP_CREATED_INDEX skipped reason=persist_advisor_indexes created=%d already_exists=%d",
					 created,
					 already_exists);
		if (emit_log)
			ereport(LOG,
					(errmsg("AUTO_INDEX_ADVISOR_STAGE4_APPLY "
							"cleanup=skipped reason=persist_advisor_indexes")));
	}

	initStringInfo(&result);
	appendStringInfo(&result,
					 "OK: stage4 apply force=%s auto_create=%s candidates=%llu "
					 "created=%d already_exists=%d failed=%d "
					 "dropped_after_apply=%d max_indexes_per_run=%d "
					 "created_indexes=[%s] created_columns=[%s]",
					 force ? "on" : "off",
					 auto_index_advisor_auto_create ? "on" : "off",
					 (unsigned long long) ncandidates,
					 created,
					 already_exists,
					 skipped,
					 dropped_after_apply,
					 auto_index_advisor_max_indexes_per_run,
					 created_indexes.data,
					 created_columns.data);
	advisor_logf("STAGE4_SUMMARY",
				 "STAGE4_SUMMARY force=%s auto_create=%s candidates=%llu created=%d already_exists=%d failed=%d dropped_after_apply=%d max_indexes_per_run=%d created_indexes=[%s] created_columns=[%s]",
				 force ? "on" : "off",
				 auto_index_advisor_auto_create ? "on" : "off",
				 (unsigned long long) ncandidates,
				 created,
				 already_exists,
				 skipped,
				 dropped_after_apply,
				 auto_index_advisor_max_indexes_per_run,
				 created_indexes.data,
				 created_columns.data);

	return result.data;
}

static char *
advisor_apply_recommendations_with_cleanup(bool emit_log, bool force)
{
	return advisor_apply_recommendations(emit_log, force);
}

static void
advisor_insert_recommendation(const char *schema_name,
							  const char *table_name,
							  AdvisorColumnStat *stat,
							  const char *decision,
							  const char *reason,
							  double activity_count,
							  double activity_fraction,
							  double threshold,
							  const char *index_name,
							  const char *index_sql,
							  AdvisorCostResult *cost_result)
{
	int			ret;
	bool		index_exists = false;
	char	   *qualified_index_name = NULL;
	char	   *create_status = "not_applied";
	char	   *created_index_name = NULL;
	char	   *created_index_sql = NULL;
	StringInfoData sql;

	if (index_name != NULL)
	{
		qualified_index_name = psprintf("%s.%s", schema_name, index_name);
		index_exists = advisor_relation_exists(qualified_index_name);
		if (index_exists)
		{
			create_status = "already_exists";
			created_index_name = (char *) index_name;
			created_index_sql = advisor_create_index_if_not_exists_sql(
				schema_name,
				table_name,
				stat->column_name,
				index_name);
		}
	}

	initStringInfo(&sql);
	appendStringInfo(&sql, "INSERT INTO ");
	append_qualified_identifier(&sql, auto_index_advisor_recommendation_table);
	appendStringInfo(&sql,
					 " (table_schema, table_name, column_name, decision, reason, "
					 "access_count, insert_count, update_count, delete_count, all_count, "
					 "access_fraction, insert_fraction, update_fraction, delete_fraction, "
					 "all_fraction, threshold, n_distinct, distinct_ratio, "
					 "recommended_index_name, recommended_index_sql, "
					 "cost_status, workload_query_count, baseline_scan_cost, "
					 "hypo_scan_cost, create_index_cost, write_maint_cost, "
					 "expected_without_index, expected_with_index, improves_expected, "
					 "create_status, created_index_name, created_index_sql, created_at, "
					 "updated_at) "
					 "VALUES (%s, %s, %s, %s, %s, "
					 "%.17g, %.17g, %.17g, %.17g, %.17g, "
					 "%.17g, %.17g, %.17g, %.17g, "
					 "%.17g, %.17g, %.17g, %.17g, "
					 "%s, %s, %s, %d, "
					 "%.17g, %.17g, %.17g, %.17g, %.17g, %.17g, %s, "
					 "%s, %s, %s, %s, now())",
					 quote_literal_cstr(schema_name),
					 quote_literal_cstr(table_name),
					 quote_literal_cstr(stat->column_name),
					 quote_literal_cstr(decision),
					 quote_literal_cstr(reason),
					 stat->access_count,
					 stat->insert_count,
					 stat->update_count,
					 stat->delete_count,
					 activity_count,
					 stat->access_fraction,
					 stat->insert_fraction,
					 stat->update_fraction,
					 stat->delete_fraction,
					 activity_fraction,
					 threshold,
					 stat->n_distinct,
					 stat->distinct_ratio,
					 index_name != NULL ? quote_literal_cstr(index_name) : "NULL",
					 index_sql != NULL ? quote_literal_cstr(index_sql) : "NULL",
					 quote_literal_cstr(cost_result->status),
					 cost_result->query_count,
					 cost_result->baseline_scan_cost,
					 cost_result->hypo_scan_cost,
					 cost_result->create_index_cost,
					 cost_result->write_maint_cost,
					 cost_result->expected_without_index,
					 cost_result->expected_with_index,
					 cost_result->improves_expected ? "true" : "false",
					 quote_literal_cstr(create_status),
					 created_index_name != NULL ? quote_literal_cstr(created_index_name) : "NULL",
					 created_index_sql != NULL ? quote_literal_cstr(created_index_sql) : "NULL",
					 index_exists ? "now()" : "NULL");

	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "auto_index_advisor: could not insert recommendation");

	pfree(sql.data);
}

static char *
advisor_run_once(bool emit_log, bool force)
{
	int			ret;
	uint64		ntup;
	AdvisorColumnStat *stats;
	double		total_accesses = 0.0;
	double		total_activity_count = 0.0;
	double		row_count = 0.0;
	double		relpages = 0.0;
	double		threshold;
	AdvisorCostSettings cost_settings;
	int			kept = 0;
	int			skipped = 0;
	int			costed = 0;
	StringInfoData sql;
	StringInfoData qstats;
	StringInfoData result;
	char	   *schema_name;
	char	   *table_name;
	Oid			relid;

	if (!force && !auto_index_advisor_enabled)
		return pstrdup("OK: auto_index_advisor.enabled is off");

	if (auto_index_advisor_target_table == NULL ||
		auto_index_advisor_target_table[0] == '\0')
		return pstrdup("FAIL: auto_index_advisor.target_table is empty");

	if (auto_index_advisor_stats_table == NULL ||
		auto_index_advisor_stats_table[0] == '\0')
		return pstrdup("FAIL: auto_index_advisor.stats_table is empty");

	if (auto_index_advisor_recommendation_table == NULL ||
		auto_index_advisor_recommendation_table[0] == '\0')
		return pstrdup("FAIL: auto_index_advisor.recommendation_table is empty");

	if (!advisor_relation_exists(auto_index_advisor_stats_table))
	{
		initStringInfo(&result);
		appendStringInfo(&result,
						 "FAIL: stats table %s does not exist",
						 auto_index_advisor_stats_table);
		return result.data;
	}

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT c.oid, n.nspname::text, c.relname::text, "
					 "       c.reltuples::float8, c.relpages::float8 "
					 "FROM pg_class c "
					 "JOIN pg_namespace n ON n.oid = c.relnamespace "
					 "WHERE c.oid = to_regclass(%s) "
					 "  AND c.relkind IN ('r', 'p', 'm')",
					 quote_literal_cstr(auto_index_advisor_target_table));

	ret = SPI_execute(sql.data, true, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "auto_index_advisor: target table lookup failed");

	if (SPI_processed != 1)
	{
		initStringInfo(&result);
		appendStringInfo(&result,
						 "FAIL: target table %s was not found",
						 auto_index_advisor_target_table);
		pfree(sql.data);
		return result.data;
	}

	relid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc,
										   1,
										   &(bool){false}));
	schema_name = spi_get_cstring(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	table_name = spi_get_cstring(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	row_count = Max(spi_get_float8(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4), 1.0);
	relpages = spi_get_float8(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5);

	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT a.attname::text, "
					 "       COALESCE(s.access_count, 0)::float8, "
					 "       COALESCE(s.total_count, 0)::float8, "
					 "       COALESCE(s.insert_count, 0)::float8, "
					 "       COALESCE(s.update_count, 0)::float8, "
					 "       COALESCE(s.delete_count, 0)::float8, "
					 "       COALESCE(s.n_distinct, pg.n_distinct, 0)::float8 "
					 "FROM pg_attribute a "
					 "LEFT JOIN pg_stats pg "
					 "  ON pg.schemaname = %s "
					 " AND pg.tablename = %s "
					 " AND pg.attname = a.attname "
					 "LEFT JOIN ",
					 quote_literal_cstr(schema_name),
					 quote_literal_cstr(table_name));
	append_qualified_identifier(&sql, auto_index_advisor_stats_table);
	appendStringInfo(&sql,
					 " s ON lower(s.table_schema) = lower(%s) "
					 "  AND lower(s.table_name) = lower(%s) "
					 "  AND lower(s.column_name::text) = lower(a.attname) "
					 "WHERE a.attrelid = %u "
					 "  AND a.attnum > 0 "
					 "  AND NOT a.attisdropped "
					 "ORDER BY a.attnum",
					 quote_literal_cstr(schema_name),
					 quote_literal_cstr(table_name),
					 relid);

	ret = SPI_execute(sql.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "auto_index_advisor: column stats lookup failed");

	ntup = SPI_processed;
	stats = palloc0(sizeof(AdvisorColumnStat) * Max(ntup, 1));

	for (uint64 i = 0; i < ntup; i++)
	{
		HeapTuple	tuple = SPI_tuptable->vals[i];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;

		stats[i].column_name = spi_get_cstring(tuple, tupdesc, 1);
		stats[i].access_count = spi_get_float8(tuple, tupdesc, 2);
		stats[i].total_count = spi_get_float8(tuple, tupdesc, 3);
		stats[i].insert_count = spi_get_float8(tuple, tupdesc, 4);
		stats[i].update_count = spi_get_float8(tuple, tupdesc, 5);
		stats[i].delete_count = spi_get_float8(tuple, tupdesc, 6);
		stats[i].n_distinct = spi_get_float8(tuple, tupdesc, 7);

		total_activity_count += stats[i].access_count +
			stats[i].insert_count + stats[i].update_count +
			stats[i].delete_count;
	}
	total_accesses = total_activity_count;

	threshold = Max(auto_index_advisor_min_access_fraction,
					2.0 / Max((double) ntup, 1.0));
	cost_settings = advisor_get_cost_settings();

	advisor_ensure_recommendation_table();
	advisor_ensure_workload_table();
	if (advisor_workload_query_count() <= 0)
		return pstrdup("FAIL: workload table has 0 enabled queries; run workload queries against the target table first");
	advisor_ensure_log_table();
	advisor_logf("RUN_BEGIN",
				 "RUN_BEGIN target=%s stats_table=%s workload_table=%s recommendation_table=%s",
				 auto_index_advisor_target_table,
				 auto_index_advisor_stats_table,
				 auto_index_advisor_workload_table,
				 auto_index_advisor_recommendation_table);
	if (auto_index_advisor_drop_indexes_before_run)
		advisor_logf("DROP_STARTUP_ADVISOR_INDEX",
					 "DROP_STARTUP_ADVISOR_INDEX skipped reason=persist_advisor_indexes");
	advisor_clear_recommendations(schema_name, table_name);

	initStringInfo(&qstats);
	for (uint64 i = 0; i < ntup; i++)
	{
		double		distinct_count;
		double		activity_count;
		double		activity_fraction;
		const char *decision;
		const char *reason = "candidate";
		char	   *index_name = NULL;
		char	   *index_sql = NULL;
		AdvisorCostResult cost_result = {0};

		cost_result.status = "not_costed";

		stats[i].access_fraction =
			total_accesses > 0.0 ? stats[i].access_count / total_accesses : 0.0;
		stats[i].insert_fraction =
			total_accesses > 0.0 ? stats[i].insert_count / total_accesses : 0.0;
		stats[i].update_fraction =
			total_accesses > 0.0 ? stats[i].update_count / total_accesses : 0.0;
		stats[i].delete_fraction =
			total_accesses > 0.0 ? stats[i].delete_count / total_accesses : 0.0;

		distinct_count = advisor_distinct_count(stats[i].n_distinct, row_count);
		stats[i].distinct_ratio = distinct_count / Max(row_count, 1.0);

		activity_count = stats[i].access_count + stats[i].insert_count +
			stats[i].update_count + stats[i].delete_count;
		activity_fraction = stats[i].access_fraction + stats[i].insert_fraction +
			stats[i].update_fraction + stats[i].delete_fraction;

		if (total_accesses <= 0.0 || activity_fraction <= threshold)
		{
			decision = "SKIP";
			reason = "access_fraction";
			skipped++;
		}
		else if (stats[i].distinct_ratio <= auto_index_advisor_min_distinct_ratio)
		{
			decision = "SKIP";
			reason = "distinct_ratio";
			skipped++;
		}
		else
		{
			decision = "KEEP";
			index_name = advisor_index_name(table_name, stats[i].column_name);
			index_sql = advisor_create_index_sql(schema_name,
												 table_name,
												 stats[i].column_name,
												 index_name);
			cost_result = advisor_cost_candidate(&stats[i],
												 index_sql,
												 activity_count,
												 total_accesses,
												 row_count,
												 relpages,
												 cost_settings);
			if (strcmp(cost_result.status, "costed") == 0)
				costed++;
			kept++;
		}

		advisor_insert_recommendation(schema_name,
									  table_name,
									  &stats[i],
									  decision,
									  reason,
									  activity_count,
									  activity_fraction,
									  threshold,
									  index_name,
									  index_sql,
									  &cost_result);

		advisor_logf(strcmp(decision, "KEEP") == 0 ? "CANDIDATE_KEEP" : "CANDIDATE_SKIP",
					 "CANDIDATE_%s column=%s reason=%s all_count=%.0f all_fraction=%.4f threshold=%.4f distinct_ratio=%.4f cost_status=%s workload_queries=%d baseline_scan_cost=%.2f hypo_scan_cost=%.2f create_cost=%.2f write_cost=%.2f expected_without_index=%.2f expected_with_index=%.2f improves_expected=%s recommended_index=%s",
					 decision,
					 stats[i].column_name,
					 reason,
					 activity_count,
					 activity_fraction,
					 threshold,
					 stats[i].distinct_ratio,
					 cost_result.status,
					 cost_result.query_count,
					 cost_result.baseline_scan_cost,
					 cost_result.hypo_scan_cost,
					 cost_result.create_index_cost,
					 cost_result.write_maint_cost,
					 cost_result.expected_without_index,
					 cost_result.expected_with_index,
					 cost_result.improves_expected ? "true" : "false",
					 index_name != NULL ? index_name : "");

		if (emit_log)
		{
			ereport(LOG,
					(errmsg("AUTO_INDEX_ADVISOR_%s table=%s column=%s "
							"reason=%s access_count=%.0f insert_count=%.0f "
							"update_count=%.0f delete_count=%.0f all_count=%.0f "
							"access_fraction=%.4f insert_fraction=%.4f "
							"update_fraction=%.4f delete_fraction=%.4f "
							"all_fraction=%.4f threshold=%.4f "
							"n_distinct=%.4f distinct_ratio=%.4f "
							"recommended_index=%s recommended_sql=%s "
							"cost_status=%s workload_queries=%d "
							"baseline_scan_cost=%.2f hypo_scan_cost=%.2f "
							"create_cost=%.2f write_cost=%.2f "
							"expected_without_index=%.2f expected_with_index=%.2f "
							"improves_expected=%s",
							decision,
							auto_index_advisor_target_table,
							stats[i].column_name,
							reason,
							stats[i].access_count,
							stats[i].insert_count,
							stats[i].update_count,
							stats[i].delete_count,
							activity_count,
							stats[i].access_fraction,
							stats[i].insert_fraction,
							stats[i].update_fraction,
							stats[i].delete_fraction,
							activity_fraction,
							threshold,
							stats[i].n_distinct,
							stats[i].distinct_ratio,
							index_name != NULL ? index_name : "",
							index_sql != NULL ? index_sql : "",
							cost_result.status,
							cost_result.query_count,
							cost_result.baseline_scan_cost,
							cost_result.hypo_scan_cost,
							cost_result.create_index_cost,
							cost_result.write_maint_cost,
							cost_result.expected_without_index,
							cost_result.expected_with_index,
							cost_result.improves_expected ? "true" : "false")));
		}

		appendStringInfo(&qstats,
						 "%s:%s all_fraction=%.4f distinct_ratio=%.4f; ",
						 decision, stats[i].column_name, activity_fraction,
						 stats[i].distinct_ratio);
	}

	if (emit_log)
		ereport(LOG,
				(errmsg("AUTO_INDEX_ADVISOR_STAGE3_SUMMARY table=%s "
						"stats_table=%s columns=%llu total_count=%.0f "
						"kept=%d skipped=%d costed=%d threshold=%.4f "
						"rows=%.0f relpages=%.0f recommendation_table=%s "
						"workload_table=%s mode=hypopg_costing",
						auto_index_advisor_target_table,
						auto_index_advisor_stats_table,
						(unsigned long long) ntup,
						total_accesses,
						kept,
						skipped,
						costed,
						threshold,
						row_count,
						relpages,
						auto_index_advisor_recommendation_table,
						auto_index_advisor_workload_table)));
	advisor_logf("STAGE3_SUMMARY",
				 "STAGE3_SUMMARY target=%s columns=%llu total_count=%.0f kept=%d skipped=%d costed=%d threshold=%.4f rows=%.0f relpages=%.0f workload_table=%s recommendation_table=%s",
				 auto_index_advisor_target_table,
				 (unsigned long long) ntup,
				 total_accesses,
				 kept,
				 skipped,
				 costed,
				 threshold,
				 row_count,
				 relpages,
				 auto_index_advisor_workload_table,
				 auto_index_advisor_recommendation_table);

	initStringInfo(&result);
	appendStringInfo(&result,
					 "OK: stage3 hypopg_costing target=%s stats_table=%s "
					 "recommendation_table=%s workload_table=%s "
					 "columns=%llu total_count=%.0f kept=%d skipped=%d costed=%d "
					 "threshold=%.4f details=[%s]",
					 auto_index_advisor_target_table,
					 auto_index_advisor_stats_table,
					 auto_index_advisor_recommendation_table,
					 auto_index_advisor_workload_table,
					 (unsigned long long) ntup,
					 total_accesses,
					 kept,
					 skipped,
					 costed,
					 threshold,
					 qstats.data);

	pfree(sql.data);
	pfree(qstats.data);
	return result.data;
}

static char *
advisor_run_once_in_transaction(bool emit_log)
{
	char	   *result;
	char	   *result_copy;
	MemoryContext oldcontext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	advisor_take_xact_lock();

	result = advisor_run_once(emit_log, false);
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	result_copy = pstrdup(result);
	MemoryContextSwitchTo(oldcontext);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	advisor_leave_internal_query(old_internal_query);

	return result_copy;
}

void
auto_index_advisor_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(auto_index_advisor_database,
										 auto_index_advisor_role,
										 0);

	ereport(LOG,
			(errmsg("auto_index_advisor background worker started: "
					"database=%s target_table=%s stats_table=%s "
					"recommendation_table=%s workload_table=%s naptime=%d",
					auto_index_advisor_database ? auto_index_advisor_database : "",
					auto_index_advisor_target_table ? auto_index_advisor_target_table : "",
					auto_index_advisor_stats_table ? auto_index_advisor_stats_table : "",
					auto_index_advisor_recommendation_table ? auto_index_advisor_recommendation_table : "",
					auto_index_advisor_workload_table ? auto_index_advisor_workload_table : "",
					auto_index_advisor_naptime)));

	for (;;)
	{
		int			rc;
		char	   *result;
		char	   *apply_result;

		CHECK_FOR_INTERRUPTS();
		ProcessConfigFile(PGC_SIGHUP);

		pgstat_report_activity(STATE_RUNNING, "auto_index_advisor stage3 hypopg costing");
		result = advisor_run_once_in_transaction(true);
		ereport(LOG, (errmsg("AUTO_INDEX_ADVISOR_CHECK %s", result)));
		if (auto_index_advisor_enabled &&
			auto_index_advisor_auto_create &&
			strncmp(result, "OK: stage", strlen("OK: stage")) == 0)
		{
			bool		old_internal_query;
			MemoryContext oldcontext;

			old_internal_query = advisor_enter_internal_query();
			pgstat_report_activity(STATE_RUNNING, "auto_index_advisor stage4 apply");
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			advisor_take_xact_lock();
			apply_result = advisor_apply_recommendations_with_cleanup(true, false);
			oldcontext = MemoryContextSwitchTo(TopMemoryContext);
			apply_result = pstrdup(apply_result);
			MemoryContextSwitchTo(oldcontext);
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			advisor_leave_internal_query(old_internal_query);
			ereport(LOG, (errmsg("AUTO_INDEX_ADVISOR_APPLY %s", apply_result)));

			if (auto_index_advisor_drop_indexes_after_apply)
				ereport(LOG,
						(errmsg("AUTO_INDEX_ADVISOR_CLEANUP skipped reason=persist_advisor_indexes")));
		}

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   auto_index_advisor_naptime * 1000L,
					   advisor_wait_event_main());
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

Datum
auto_index_advisor_check(PG_FUNCTION_ARGS)
{
	char	   *result;
	text	   *retval;
	MemoryContext caller_context = CurrentMemoryContext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SPI_connect();
	advisor_take_xact_lock();
	result = advisor_run_once(true, true);
	MemoryContextSwitchTo(caller_context);
	retval = cstring_to_text(result);
	SPI_finish();
	advisor_leave_internal_query(old_internal_query);

	PG_RETURN_TEXT_P(retval);
}

Datum
auto_index_advisor_refresh_recommendations(PG_FUNCTION_ARGS)
{
	char	   *result;
	text	   *retval;
	MemoryContext caller_context = CurrentMemoryContext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SPI_connect();
	advisor_take_xact_lock();
	result = advisor_run_once(true, true);
	MemoryContextSwitchTo(caller_context);
	retval = cstring_to_text(result);
	SPI_finish();
	advisor_leave_internal_query(old_internal_query);

	PG_RETURN_TEXT_P(retval);
}

Datum
auto_index_advisor_refresh_costing(PG_FUNCTION_ARGS)
{
	char	   *result;
	text	   *retval;
	MemoryContext caller_context = CurrentMemoryContext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SPI_connect();
	advisor_take_xact_lock();
	result = advisor_run_once(true, true);
	MemoryContextSwitchTo(caller_context);
	retval = cstring_to_text(result);
	SPI_finish();
	advisor_leave_internal_query(old_internal_query);

	PG_RETURN_TEXT_P(retval);
}

Datum
auto_index_advisor_apply_recommendations(PG_FUNCTION_ARGS)
{
	char	   *result;
	text	   *retval;
	MemoryContext caller_context = CurrentMemoryContext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SPI_connect();
	advisor_take_xact_lock();
	result = advisor_apply_recommendations_with_cleanup(true, true);
	MemoryContextSwitchTo(caller_context);
	retval = cstring_to_text(result);
	SPI_finish();
	advisor_leave_internal_query(old_internal_query);

	PG_RETURN_TEXT_P(retval);
}

Datum
auto_index_advisor_drop_indexes(PG_FUNCTION_ARGS)
{
	char	   *result;
	text	   *retval;
	MemoryContext caller_context = CurrentMemoryContext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SPI_connect();
	advisor_take_xact_lock();
	result = advisor_drop_existing_indexes(true, NULL);
	MemoryContextSwitchTo(caller_context);
	retval = cstring_to_text(result);
	SPI_finish();
	advisor_leave_internal_query(old_internal_query);

	PG_RETURN_TEXT_P(retval);
}

Datum
auto_index_advisor_run_test_cycle(PG_FUNCTION_ARGS)
{
	char	   *before_drop;
	char	   *refresh_result;
	char	   *apply_result;
	char	   *after_drop;
	text	   *retval;
	int			workload_count;
	StringInfoData result;
	MemoryContext caller_context = CurrentMemoryContext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SPI_connect();
	advisor_take_xact_lock();

	workload_count = advisor_workload_query_count();
	if (workload_count <= 0)
	{
		MemoryContextSwitchTo(caller_context);
		retval = cstring_to_text(
			"FAIL: workload table has 0 enabled queries; run workload queries against the target table first");
		SPI_finish();
		advisor_leave_internal_query(old_internal_query);
		PG_RETURN_TEXT_P(retval);
	}

	before_drop = pstrdup("SKIPPED: advisor-created indexes are retained");
	refresh_result = advisor_run_once(true, true);
	apply_result = pstrdup("DEFERRED: run SELECT auto_index_advisor_apply_recommendations(); "
						   "as a separate statement so Stage 4 runs in a separate transaction");
	after_drop = pstrdup("SKIPPED: advisor-created indexes are retained");

	MemoryContextSwitchTo(caller_context);
	initStringInfo(&result);
	appendStringInfo(&result,
					 "OK: test_cycle workload_queries=%d before=[%s] "
					 "refresh=[%s] apply=[%s] after=[%s]",
					 workload_count,
					 before_drop,
					 refresh_result,
					 apply_result,
					 after_drop);
	retval = cstring_to_text(result.data);

	SPI_finish();
	advisor_leave_internal_query(old_internal_query);

	PG_RETURN_TEXT_P(retval);
}

Datum
auto_index_advisor_run_full_cycle(PG_FUNCTION_ARGS)
{
	char	   *result;
	char	   *result_copy;
	char	   *log_path = NULL;
	char	   *log_path_copy = NULL;
	text	   *retval;
	MemoryContext caller_context = CurrentMemoryContext;
	bool		old_internal_query;

	old_internal_query = advisor_enter_internal_query();
	SPI_connect();
	advisor_take_xact_lock();
	result = advisor_run_full_cycle_internal(&log_path);
	MemoryContextSwitchTo(caller_context);
	result_copy = pstrdup(result);
	if (log_path != NULL)
		log_path_copy = pstrdup(log_path);
	retval = cstring_to_text(result_copy);
	SPI_finish();
	advisor_leave_internal_query(old_internal_query);

	if (log_path_copy != NULL)
		ereport(NOTICE,
				(errmsg("Logs: %s", log_path_copy)));
	ereport(NOTICE,
			(errmsg("AUTO_INDEX_ADVISOR_FULL_CYCLE %s", result_copy)));

	PG_RETURN_TEXT_P(retval);
}

void
_PG_init(void)
{
	BackgroundWorker worker;

	DefineCustomBoolVariable("auto_index_advisor.enabled",
							 "Enables the auto index advisor worker.",
							 NULL,
							 &auto_index_advisor_enabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("auto_index_advisor.naptime",
							"Seconds between advisor checks.",
							NULL,
							&auto_index_advisor_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.database",
							   "Database the background worker connects to.",
							   NULL,
							   &auto_index_advisor_database,
							   "postgres",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.role",
							   "Role the background worker connects as.",
							   NULL,
							   &auto_index_advisor_role,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.target_table",
							   "Table observed by the advisor.",
							   NULL,
							   &auto_index_advisor_target_table,
							   "public.online_retail",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.stats_table",
							   "Advisor stats table populated by the Python prototype.",
							   NULL,
							   &auto_index_advisor_stats_table,
							   "public.auto_index_column_stats",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.recommendation_table",
							   "Table where advisor recommendations and costs are persisted.",
							   NULL,
							   &auto_index_advisor_recommendation_table,
							   "public.auto_index_advisor_recommendations",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.workload_table",
							   "Table containing workload SQL text used for Stage-3 HypoPG costing.",
							   NULL,
							   &auto_index_advisor_workload_table,
							   "public.auto_index_advisor_workload",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.log_table",
							   "Table where advisor run logs are persisted.",
							   NULL,
							   &auto_index_advisor_log_table,
							   "public.auto_index_advisor_log",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.log_file_path",
							   "Filesystem path where the internal advisor log export is written.",
							   NULL,
							   &auto_index_advisor_log_file_path,
							   "/tmp/auto_index_advisor.log",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("auto_index_advisor.query_decision_log_file_path",
							   "Filesystem path where per-query advisor decisions are written.",
							   NULL,
							   &auto_index_advisor_query_decision_log_file_path,
							   "/tmp/auto_index_advisor_query_decisions.log",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_index_advisor.enable_hypopg_costing",
							 "Enables Stage-3 HypoPG costing for recommended indexes.",
							 NULL,
							 &auto_index_advisor_enable_hypopg_costing,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_index_advisor.auto_create",
							 "Enables Stage-4 automatic real index creation from winning recommendations.",
							 NULL,
							 &auto_index_advisor_auto_create,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_index_advisor.drop_indexes_before_run",
							 "Drops existing auto_advisor indexes before each advisor costing run.",
							 NULL,
							 &auto_index_advisor_drop_indexes_before_run,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_index_advisor.drop_indexes_after_apply",
							 "Drops auto_advisor indexes after Stage-4 apply finishes.",
							 NULL,
							 &auto_index_advisor_drop_indexes_after_apply,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_index_advisor.capture_workload",
							 "Captures live SELECT statements touching the target table into the advisor workload table.",
							 NULL,
							 &auto_index_advisor_capture_workload,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("auto_index_advisor.max_indexes_per_run",
							"Maximum number of real indexes Stage 4 may create per advisor run.",
							NULL,
							&auto_index_advisor_max_indexes_per_run,
							1,
							1,
							100,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomRealVariable("auto_index_advisor.min_access_fraction",
							 "Minimum combined access/write fraction for a candidate.",
							 NULL,
							 &auto_index_advisor_min_access_fraction,
							 0.05,
							 0.0,
							 1.0,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomRealVariable("auto_index_advisor.min_distinct_ratio",
							 "Minimum distinct-count ratio for a candidate.",
							 NULL,
							 &auto_index_advisor_min_distinct_ratio,
							 0.0,
							 0.0,
							 1.0,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

#if PG_VERSION_NUM >= 150000
	MarkGUCPrefixReserved("auto_index_advisor");
#else
	EmitWarningsOnPlaceholders("auto_index_advisor");
#endif

	prev_planner_hook = planner_hook;
	planner_hook = auto_index_advisor_planner;
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = auto_index_advisor_ExecutorRun;

	if (!process_shared_preload_libraries_in_progress)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "auto_index_advisor");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "auto_index_advisor_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "auto_index_advisor worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "auto_index_advisor");
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}
