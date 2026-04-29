#!/usr/bin/env python3
"""
Try every btree index column subset for a table with HypoPG and log estimated
costs for the no-index baseline and every hypothetical index candidate.
"""

import argparse
import csv
import itertools
import json
import math
import re
import subprocess
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path


def quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value):
    return "'" + value.replace("'", "''") + "'"


def run_psql(args, sql):
    cmd = [
        args.psql,
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        args.dbname,
        "-At",
        "-c",
        sql,
    ]
    if args.host:
        cmd.extend(["-h", args.host])
    if args.port:
        cmd.extend(["-p", str(args.port)])
    if args.user:
        cmd.extend(["-U", args.user])

    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "psql failed\n"
            f"SQL:\n{sql}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc.stdout.strip()


def read_queries(path):
    text = Path(path).read_text(encoding="utf-8")
    queries = []
    current = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            query = "\n".join(current).strip()
            queries.append(query)
            current = []
    if current:
        queries.append("\n".join(current).strip().rstrip(";") + ";")
    return queries


def extract_explain_json(output):
    start = output.find("[")
    end = output.rfind("]")
    if start < 0 or end < start:
        raise ValueError(f"Could not find EXPLAIN JSON in output:\n{output}")
    return json.loads(output[start : end + 1])


def collect_plan_nodes(plan):
    nodes = []

    def walk(node, depth):
        item = {
            "depth": depth,
            "node_type": node.get("Node Type"),
            "startup_cost": node.get("Startup Cost"),
            "total_cost": node.get("Total Cost"),
            "plan_rows": node.get("Plan Rows"),
        }
        if "Index Name" in node:
            item["index_name"] = node["Index Name"]
        if "Relation Name" in node:
            item["relation_name"] = node["Relation Name"]
        nodes.append(item)
        for child in node.get("Plans", []):
            walk(child, depth + 1)

    walk(plan, 0)
    return nodes


def format_plan_nodes(nodes):
    parts = []
    for node in nodes:
        prefix = ">" * node["depth"]
        label = f"{prefix}{node['node_type']}"
        if node.get("index_name"):
            label += f"[{node['index_name']}]"
        parts.append(label)
    return " -> ".join(parts)


def explain_plan(args, query, analyze=False, enable_seqscan=None, hypo_index_sql=None):
    options = "ANALYZE, BUFFERS, FORMAT JSON" if analyze else "FORMAT JSON"
    prefix = ""
    if hypo_index_sql is not None:
        prefix += (
            "SELECT hypopg_reset(); "
            f"SELECT * FROM hypopg_create_index({quote_literal(hypo_index_sql)}); "
        )
    if enable_seqscan is not None:
        setting = "on" if enable_seqscan else "off"
        prefix += f"SET enable_seqscan = {setting}; "
    output = run_psql(args, f"{prefix}EXPLAIN ({options}) {query}")
    doc = extract_explain_json(output)
    root = doc[0]["Plan"]
    plan_nodes = collect_plan_nodes(root)
    result = {
        "node_type": root.get("Node Type"),
        "startup_cost": root.get("Startup Cost"),
        "total_cost": root.get("Total Cost"),
        "plan_rows": root.get("Plan Rows"),
        "plan_width": root.get("Plan Width"),
        "plan_nodes": plan_nodes,
        "plan_summary": format_plan_nodes(plan_nodes),
    }
    if analyze:
        result.update(
            {
                "actual_startup_ms": root.get("Actual Startup Time"),
                "actual_total_ms": root.get("Actual Total Time"),
                "actual_rows": root.get("Actual Rows"),
                "execution_time_ms": doc[0].get("Execution Time"),
                "planning_time_ms": doc[0].get("Planning Time"),
            }
        )
    return result


def get_table_columns(args):
    schema, table = split_table_name(args.table)
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = {quote_literal(schema)}
          AND table_name = {quote_literal(table)}
        ORDER BY ordinal_position;
    """
    output = run_psql(args, sql)
    return [line for line in output.splitlines() if line]


def split_table_name(table_name):
    parts = table_name.split(".", 1)
    if len(parts) == 1:
        return "public", parts[0]
    return parts[0], parts[1]


def get_stats(args, columns):
    schema, table = split_table_name(args.table)
    sql = f"""
        SELECT attname || '|' || n_distinct::text
        FROM pg_stats
        WHERE schemaname = {quote_literal(schema)}
          AND tablename = {quote_literal(table)}
          AND attname = ANY (ARRAY[{",".join(quote_literal(c) for c in columns)}])
        ORDER BY attname;
    """
    output = run_psql(args, sql)
    stats = {}
    for line in output.splitlines():
        if "|" in line:
            name, n_distinct = line.split("|", 1)
            stats[name] = n_distinct
    return stats


def get_row_count(args):
    return int(run_psql(args, f"SELECT count(*) FROM {qualified_table(args.table)};"))


def get_table_catalog_stats(args):
    schema, table = split_table_name(args.table)
    sql = f"""
        SELECT c.relpages::text || '|' || c.reltuples::text
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = {quote_literal(schema)}
          AND c.relname = {quote_literal(table)};
    """
    output = run_psql(args, sql)
    if not output or "|" not in output:
        return {"relpages": 0.0, "reltuples": 0.0}
    relpages, reltuples = output.split("|", 1)
    return {"relpages": float(relpages), "reltuples": float(reltuples)}


def get_planner_cost_settings(args):
    names = [
        "seq_page_cost",
        "random_page_cost",
        "cpu_tuple_cost",
        "cpu_operator_cost",
    ]
    sql = "SELECT name || '=' || setting FROM pg_settings WHERE name IN (" + ",".join(
        quote_literal(name) for name in names
    ) + ");"
    output = run_psql(args, sql)
    settings = {}
    for line in output.splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            settings[name] = float(value)
    return settings


def estimate_btree_create_cost(row_count, relpages, column_count, cost_settings):
    """
    Approximate CREATE INDEX cost in planner cost units.

    PostgreSQL/HypoPG do not expose a CREATE INDEX cost estimate.  This is a
    transparent heuristic: one sequential table pass plus CPU work for extracting
    keys and sorting/building a btree, scaled by indexed column count.
    """
    rows = max(float(row_count), 1.0)
    pages = max(float(relpages), 1.0)
    seq_page_cost = cost_settings.get("seq_page_cost", 1.0)
    cpu_tuple_cost = cost_settings.get("cpu_tuple_cost", 0.01)
    cpu_operator_cost = cost_settings.get("cpu_operator_cost", 0.0025)

    table_scan_cost = pages * seq_page_cost + rows * cpu_tuple_cost
    key_cpu_cost = rows * max(column_count, 1) * cpu_operator_cost
    sort_build_cost = rows * math.log2(max(rows, 2.0)) * cpu_operator_cost
    return table_scan_cost + key_cpu_cost + sort_build_cost


def estimate_seq_scan_cost(row_count, relpages, cost_settings):
    rows = max(float(row_count), 1.0)
    pages = max(float(relpages), 1.0)
    seq_page_cost = cost_settings.get("seq_page_cost", 1.0)
    cpu_tuple_cost = cost_settings.get("cpu_tuple_cost", 0.01)
    return pages * seq_page_cost + rows * cpu_tuple_cost


def estimate_write_maintenance_cost(stat, row_count, cost_settings):
    rows = max(float(row_count), 1.0)
    cpu_tuple_cost = cost_settings.get("cpu_tuple_cost", 0.01)
    cpu_operator_cost = cost_settings.get("cpu_operator_cost", 0.0025)
    insert_count = max(float(stat.get("insert_count", 0)), 0.0)
    update_count = max(float(stat.get("update_count", 0)), 0.0)
    delete_count = max(float(stat.get("delete_count", 0)), 0.0)

    c_insert = math.log2(max(rows, 2.0)) * cpu_operator_cost + cpu_tuple_cost
    c_update = 2.0 * c_insert
    c_delete = c_insert
    index_maint_cost = (
        insert_count * c_insert
        + update_count * c_update
        + delete_count * c_delete
    )
    stats_maint_cost = (
        insert_count + update_count + delete_count
    ) * cpu_operator_cost
    return {
        "insert_count": insert_count,
        "update_count": update_count,
        "delete_count": delete_count,
        "c_insert": c_insert,
        "c_update": c_update,
        "c_delete": c_delete,
        "write_index_maint_cost": index_maint_cost,
        "stats_maint_cost": stats_maint_cost,
        "write_maint_cost": index_maint_cost ,
    }


def parse_pg_array_text(value):
    if not value:
        return []
    text = value.strip()
    if not (text.startswith("{") and text.endswith("}")):
        return [text]
    inner = text[1:-1]
    if not inner:
        return []
    reader = csv.reader(StringIO(inner), quotechar='"', escapechar="\\")
    return [item for item in next(reader, [])]


def parse_pg_float_array_text(value):
    result = []
    for item in parse_pg_array_text(value):
        try:
            result.append(float(item))
        except ValueError:
            continue
    return result


def extract_equality_values(query, columns):
    result = {}
    normalized = normalize_column_map(columns)
    for lower_name, original_name in normalized.items():
        pattern = (
            r"(?<![a-zA-Z0-9_\.])"
            + re.escape(lower_name)
            + r"(?![a-zA-Z0-9_])\s*=\s*"
            r"('(?:''|[^'])*'|[-+]?\d+(?:\.\d+)?|[a-zA-Z_][a-zA-Z0-9_]*)"
        )
        match = re.search(pattern, query, flags=re.IGNORECASE)
        if not match:
            continue
        raw = match.group(1).strip()
        if raw.startswith("'") and raw.endswith("'"):
            raw = raw[1:-1].replace("''", "'")
        result[original_name.lower()] = raw
    return result


def estimate_matching_rows(row_count, stat, equality_value):
    rows = max(float(row_count), 1.0)
    distinct_count = max(float(stat.get("distinct_count", 0.0)), 1.0)
    mcv_values = parse_pg_array_text(stat.get("most_common_vals", ""))
    mcv_freqs = parse_pg_float_array_text(stat.get("most_common_freqs", ""))

    if equality_value is not None:
        equality_text = str(equality_value)
        for value, freq in zip(mcv_values, mcv_freqs):
            if str(value) == equality_text or str(value).lower() == equality_text.lower():
                estimated = rows * max(freq, 0.0)
                return max(1.0, min(estimated, rows)), "most_common_value"

    mcv_freq_sum = min(max(sum(mcv_freqs), 0.0), 1.0)
    non_mcv_rows = max(rows * (1.0 - mcv_freq_sum), 1.0)
    non_mcv_distinct = max(distinct_count - len(mcv_values), 1.0)
    estimated = non_mcv_rows / non_mcv_distinct
    return max(1.0, min(estimated, rows)), "non_mcv_distinct_average"


def qualified_table(table_name):
    schema, table = split_table_name(table_name)
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def candidate_name(table_name, columns):
    _, table = split_table_name(table_name)
    raw = "auto_advisor_" + table + "_" + "_".join(columns) + "_idx"
    clean = re.sub(r"[^a-zA-Z0-9_]+", "_", raw).lower()
    return clean[:63]


def create_hypo_index(args, columns):
    index_sql = (
        f"CREATE INDEX ON {qualified_table(args.table)} "
        f"({', '.join(quote_ident(c) for c in columns)})"
    )
    output = run_psql(
        args,
        "SELECT indexrelid::text || '|' || indexname "
        f"FROM hypopg_create_index({quote_literal(index_sql)});",
    )
    indexrelid, indexname = output.split("|", 1)
    return {
        "index_sql": index_sql,
        "hypopg_indexrelid": indexrelid,
        "hypopg_indexname": indexname,
    }


def create_real_index(args, columns):
    index_name = candidate_name(args.table, columns)
    concurrently = " CONCURRENTLY" if args.create_concurrently else ""
    sql = (
        f"CREATE INDEX{concurrently} IF NOT EXISTS {quote_ident(index_name)} "
        f"ON {qualified_table(args.table)} "
        f"({', '.join(quote_ident(c) for c in columns)});"
    )
    run_psql(args, sql)
    return index_name, sql


def drop_real_index(args, index_name):
    if "." in index_name:
        sql = f"DROP INDEX IF EXISTS {qualified_table(index_name)};"
    else:
        sql = f"DROP INDEX IF EXISTS {quote_ident(index_name)};"
    run_psql(args, sql)
    return sql


def get_existing_advisor_indexes(args):
    schema, table = split_table_name(args.table)
    prefix = candidate_name(args.table, [""]).removesuffix("__idx")
    sql = f"""
        SELECT n.nspname || '.' || c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace tn ON tn.oid = t.relnamespace
        WHERE c.relkind = 'i'
          AND tn.nspname = {quote_literal(schema)}
          AND t.relname = {quote_literal(table)}
          AND c.relname LIKE {quote_literal(prefix + '%_idx')};
    """
    output = run_psql(args, sql)
    return [line.strip() for line in output.splitlines() if line.strip()]


def drop_existing_advisor_indexes(args, log):
    dropped = []
    for index_name in get_existing_advisor_indexes(args):
        drop_sql = drop_real_index(args, index_name)
        dropped.append(index_name)
        log.write(f"DROP_STARTUP_ADVISOR_INDEX index={index_name} sql={drop_sql}\n")
    log.write(f"DROPPED_STARTUP_ADVISOR_INDEXES indexes={dropped}\n")
    return dropped


def get_existing_single_column_indexes(args):
    schema, table = split_table_name(args.table)
    sql = f"""
        SELECT lower(a.attname)
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE n.nspname = {quote_literal(schema)}
          AND t.relname = {quote_literal(table)}
          AND i.indisvalid
          AND i.indisready
          AND i.indnatts = 1
          AND k.ord = 1;
    """
    output = run_psql(args, sql)
    return {line.strip().lower() for line in output.splitlines() if line.strip()}


def normalize_column_map(columns):
    return {column.lower(): column for column in columns}


def extract_query_columns(query, columns):
    result = set()
    for lower_name, original_name in normalize_column_map(columns).items():
        pattern = (
            r"(?<![a-zA-Z0-9_\.])"
            + re.escape(lower_name)
            + r"(?![a-zA-Z0-9_])\s*(=|<=|>=|<>|!=|<|>|between\b|in\b|like\b)"
        )
        if re.search(pattern, query.lower()):
            result.add(original_name)
    return result


def classify_query(query):
    match = re.match(r"\s*([a-zA-Z]+)", query)
    return match.group(1).upper() if match else "UNKNOWN"


def extract_update_set_columns(query, columns):
    match = re.search(r"\bset\b(.*?)\bwhere\b", query, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        match = re.search(r"\bset\b(.*?)(?:\breturning\b|;|$)", query, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return set()
    set_clause = match.group(1)
    result = set()
    for lower_name, original_name in normalize_column_map(columns).items():
        pattern = r"(?<![a-zA-Z0-9_\.])" + re.escape(lower_name) + r"(?![a-zA-Z0-9_])\s*="
        if re.search(pattern, set_clause, flags=re.IGNORECASE):
            result.add(original_name)
    return result


def count_insert_values_rows(query):
    values_match = re.search(r"\bvalues\b", query, flags=re.IGNORECASE)
    if not values_match:
        return None
    text = query[values_match.end():]
    depth = 0
    count = 0
    in_quote = False
    i = 0
    while i < len(text):
        char = text[i]
        if in_quote:
            if char == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            if char == "'":
                in_quote = False
        else:
            if char == "'":
                in_quote = True
            elif char == "(":
                if depth == 0:
                    count += 1
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
        i += 1
    return count if count > 0 else None


def estimate_statement_rows(args, query, query_kind):
    if query_kind == "INSERT":
        values_count = count_insert_values_rows(query)
        if values_count is not None:
            return float(values_count), "values_row_count"
    try:
        plan = explain_plan(args, query)
    except Exception:
        return 1.0, "fallback_one"
    rows = max((node.get("plan_rows") or 0 for node in plan["plan_nodes"]), default=0)
    return max(float(rows), 1.0), "explain_plan_rows"


def pg_stats_distinct_count(n_distinct, row_count):
    value = float(n_distinct)
    if value < 0:
        return abs(value) * row_count
    return value


def get_pg_column_stats(args, columns, row_count):
    schema, table = split_table_name(args.table)
    sql = f"""
        SELECT attname || '|' ||
               n_distinct::text || '|' ||
               COALESCE(most_common_vals::text, '') || '|' ||
               COALESCE(most_common_freqs::text, '')
        FROM pg_stats
        WHERE schemaname = {quote_literal(schema)}
          AND tablename = {quote_literal(table)}
          AND attname = ANY (ARRAY[{",".join(quote_literal(c) for c in columns)}])
        ORDER BY attname;
    """
    output = run_psql(args, sql)
    stats = {}
    for line in output.splitlines():
        parts = line.split("|", 3)
        if len(parts) != 4:
            continue
        column, n_distinct, most_common_vals, most_common_freqs = parts
        distinct_count = pg_stats_distinct_count(float(n_distinct), row_count)
        stats[column.lower()] = {
            "column_name": column,
            "access_count": 0,
            "total_count": 0,
            "insert_count": 0,
            "update_count": 0,
            "delete_count": 0,
            "n_distinct": float(n_distinct),
            "distinct_count": distinct_count,
            "distinct_ratio": distinct_count / max(row_count, 1),
            "most_common_vals": most_common_vals,
            "most_common_freqs": most_common_freqs,
            "source": "pg_stats",
        }
    return stats


def relation_exists(args, relname):
    output = run_psql(args, f"SELECT to_regclass({quote_literal(relname)}) IS NOT NULL;")
    return output.strip().lower() in {"t", "true"}


def get_relation_column_names(args, relname):
    sql = f"""
        SELECT lower(a.attname)
        FROM pg_attribute a
        WHERE a.attrelid = to_regclass({quote_literal(relname)})
          AND a.attnum > 0
          AND NOT a.attisdropped;
    """
    output = run_psql(args, sql)
    return {line.strip().lower() for line in output.splitlines() if line.strip()}


def ensure_stats_table(args):
    if not args.stats_table:
        return

    schema, table = split_table_name(args.stats_table)
    run_psql(args, f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)};")
    run_psql(
        args,
        f"""
        CREATE TABLE IF NOT EXISTS {args.stats_table} (
            table_schema text NOT NULL,
            table_name text NOT NULL,
            {quote_ident(args.stats_column_col)} text NOT NULL,
            {quote_ident(args.stats_access_col)} bigint NOT NULL DEFAULT 0,
            {quote_ident(args.stats_total_col)} bigint NOT NULL DEFAULT 0,
            insert_count bigint NOT NULL DEFAULT 0,
            update_count bigint NOT NULL DEFAULT 0,
            delete_count bigint NOT NULL DEFAULT 0,
            {quote_ident(args.stats_ndistinct_col)} float8 NOT NULL DEFAULT 0,
            most_common_vals text,
            most_common_freqs text,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (table_schema, table_name, {quote_ident(args.stats_column_col)})
        );
        """,
    )

    available = get_relation_column_names(args, args.stats_table)
    required_columns = {
        "table_schema": "text",
        "table_name": "text",
        args.stats_column_col: "text",
        args.stats_access_col: "bigint NOT NULL DEFAULT 0",
        args.stats_ndistinct_col: "float8 NOT NULL DEFAULT 0",
        args.stats_total_col: "bigint NOT NULL DEFAULT 0",
        "insert_count": "bigint NOT NULL DEFAULT 0",
        "update_count": "bigint NOT NULL DEFAULT 0",
        "delete_count": "bigint NOT NULL DEFAULT 0",
        "most_common_vals": "text",
        "most_common_freqs": "text",
        "updated_at": "timestamptz NOT NULL DEFAULT now()",
    }
    for column, definition in required_columns.items():
        if column.lower() not in available:
            run_psql(
                args,
                f"ALTER TABLE {args.stats_table} "
                f"ADD COLUMN {quote_ident(column)} {definition};",
            )


def reset_stats_table(args, log):
    if not args.stats_table:
        return

    schema, table = split_table_name(args.table)
    run_psql(
        args,
        f"""
        UPDATE {args.stats_table}
        SET {quote_ident(args.stats_access_col)} = 0,
            {quote_ident(args.stats_total_col)} = 0,
            insert_count = 0,
            update_count = 0,
            delete_count = 0,
            updated_at = now()
        WHERE lower(table_schema) = lower({quote_literal(schema)})
          AND lower(table_name) = lower({quote_literal(table)});
        """,
    )
    log.write(
        f"STATS_RESET table={args.stats_table} target={schema}.{table} "
        f"access_count=0 total_count=0 insert_count=0 update_count=0 delete_count=0\n"
    )


def load_stats_table(args, columns, row_count, log):
    if not args.stats_table:
        log.write("STATS_TABLE none configured; no collected access counts available\n")
        return {}

    if not relation_exists(args, args.stats_table):
        log.write(f"STATS_TABLE missing table={args.stats_table}; no collected access counts available\n")
        return {}

    available = get_relation_column_names(args, args.stats_table)
    required = {
        args.stats_column_col.lower(),
        args.stats_access_col.lower(),
        args.stats_ndistinct_col.lower(),
    }
    if not required.issubset(available):
        log.write(
            f"STATS_TABLE unusable table={args.stats_table} "
            f"required_columns={sorted(required)} available_columns={sorted(available)}; "
            "no collected access counts available\n"
        )
        return {}

    schema, table = split_table_name(args.table)
    where_parts = []
    if "table_schema" in available:
        where_parts.append(f"lower(table_schema) = lower({quote_literal(schema)})")
    if "table_name" in available:
        where_parts.append(f"lower(table_name) = lower({quote_literal(table)})")
    if "relname" in available:
        where_parts.append(f"lower(relname) = lower({quote_literal(table)})")
    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    vals_expr = "COALESCE(most_common_vals::text, '')" if "most_common_vals" in available else "''"
    freqs_expr = "COALESCE(most_common_freqs::text, '')" if "most_common_freqs" in available else "''"
    total_expr = (
        f"{quote_ident(args.stats_total_col)}::bigint::text"
        if args.stats_total_col.lower() in available
        else "'0'"
    )
    insert_expr = "insert_count::bigint::text" if "insert_count" in available else "'0'"
    update_expr = "update_count::bigint::text" if "update_count" in available else "'0'"
    delete_expr = "delete_count::bigint::text" if "delete_count" in available else "'0'"
    sql = f"""
        SELECT lower({quote_ident(args.stats_column_col)}::text) || '|' ||
               {quote_ident(args.stats_access_col)}::bigint::text || '|' ||
               {total_expr} || '|' ||
               {insert_expr} || '|' ||
               {update_expr} || '|' ||
               {delete_expr} || '|' ||
               {quote_ident(args.stats_ndistinct_col)}::float8::text || '|' ||
               {vals_expr} || '|' ||
               {freqs_expr}
        FROM {args.stats_table}
        {where_sql};
    """
    output = run_psql(args, sql)
    table_stats = {}
    valid_columns = normalize_column_map(columns)
    for line in output.splitlines():
        parts = line.split("|", 8)
        if len(parts) != 9:
            continue
        (
            column,
            access_count,
            total_count,
            insert_count,
            update_count,
            delete_count,
            n_distinct,
            most_common_vals,
            most_common_freqs,
        ) = parts
        if column not in valid_columns:
            log.write(f"STATS_TABLE skip_unknown_column column={column}\n")
            continue
        distinct_count = pg_stats_distinct_count(float(n_distinct), row_count)
        table_stats[column] = {
            "column_name": valid_columns[column],
            "access_count": int(access_count),
            "total_count": int(total_count),
            "insert_count": int(insert_count),
            "update_count": int(update_count),
            "delete_count": int(delete_count),
            "n_distinct": float(n_distinct),
            "distinct_count": distinct_count,
            "distinct_ratio": distinct_count / max(row_count, 1),
            "most_common_vals": most_common_vals,
            "most_common_freqs": most_common_freqs,
            "source": args.stats_table,
        }
    log.write(f"STATS_TABLE loaded table={args.stats_table} rows={len(table_stats)}\n")
    return table_stats


def upsert_column_stats(
    args,
    columns,
    referenced_columns,
    modified_columns,
    query_kind,
    affected_rows,
    row_count,
    log,
):
    if not args.stats_table:
        log.write("STATS_UPDATE skipped reason=no_stats_table_configured\n")
        return

    schema, table = split_table_name(args.table)
    pg_stats = get_pg_column_stats(args, columns, row_count)
    referenced = {column.lower() for column in referenced_columns}
    modified = {column.lower() for column in modified_columns}
    total_increment = len(referenced)
    affected = max(int(math.ceil(float(affected_rows))), 0)

    for column in columns:
        key = column.lower()
        stat = pg_stats.get(key, {})
        access_increment = 1 if key in referenced else 0
        insert_increment = affected if query_kind == "INSERT" else 0
        update_increment = affected if query_kind == "UPDATE" and key in modified else 0
        delete_increment = affected if query_kind == "DELETE" else 0
        n_distinct = float(stat.get("n_distinct", 0.0))
        most_common_vals = stat.get("most_common_vals", "")
        most_common_freqs = stat.get("most_common_freqs", "")
        sql = f"""
            UPDATE {args.stats_table}
            SET {quote_ident(args.stats_access_col)} = {quote_ident(args.stats_access_col)} + {access_increment},
                {quote_ident(args.stats_total_col)} = {quote_ident(args.stats_total_col)} + {total_increment},
                insert_count = insert_count + {insert_increment},
                update_count = update_count + {update_increment},
                delete_count = delete_count + {delete_increment},
                {quote_ident(args.stats_ndistinct_col)} = {n_distinct},
                most_common_vals = {quote_literal(str(most_common_vals))},
                most_common_freqs = {quote_literal(str(most_common_freqs))},
                updated_at = now()
            WHERE lower(table_schema) = lower({quote_literal(schema)})
              AND lower(table_name) = lower({quote_literal(table)})
              AND lower({quote_ident(args.stats_column_col)}::text) = lower({quote_literal(column)});

            INSERT INTO {args.stats_table} (
                table_schema,
                table_name,
                {quote_ident(args.stats_column_col)},
                {quote_ident(args.stats_access_col)},
                {quote_ident(args.stats_total_col)},
                insert_count,
                update_count,
                delete_count,
                {quote_ident(args.stats_ndistinct_col)},
                most_common_vals,
                most_common_freqs,
                updated_at
            )
            SELECT
                {quote_literal(schema)},
                {quote_literal(table)},
                {quote_literal(column)},
                {access_increment},
                {total_increment},
                {insert_increment},
                {update_increment},
                {delete_increment},
                {n_distinct},
                {quote_literal(str(most_common_vals))},
                {quote_literal(str(most_common_freqs))},
                now()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {args.stats_table}
                WHERE lower(table_schema) = lower({quote_literal(schema)})
                  AND lower(table_name) = lower({quote_literal(table)})
                  AND lower({quote_ident(args.stats_column_col)}::text) = lower({quote_literal(column)})
            );
        """
        run_psql(args, sql)

    log.write(
        f"STATS_UPDATE table={args.stats_table} total_count_increment={total_increment} "
        f"query_kind={query_kind} affected_rows={affected:.2f} "
        f"referenced_columns={sorted(referenced)} modified_columns={sorted(modified)}\n"
    )


def build_column_stats(args, columns, row_count, log):
    stats = get_pg_column_stats(args, columns, row_count)
    table_stats = load_stats_table(args, columns, row_count, log)
    stats.update(table_stats)

    total_accesses = max((item.get("total_count", 0) for item in stats.values()), default=0)
    for item in stats.values():
        item["access_fraction"] = (
            item["access_count"] / total_accesses if total_accesses > 0 else 0.0
        )
        item["insert_fraction"] = (
            item["insert_count"] / total_accesses if total_accesses > 0 else 0.0
        )
        item["update_fraction"] = (
            item["update_count"] / total_accesses if total_accesses > 0 else 0.0
        )
        item["delete_fraction"] = (
            item["delete_count"] / total_accesses if total_accesses > 0 else 0.0
        )
    return stats, total_accesses


def choose_candidate_columns(args, columns, column_stats, total_accesses, existing_indexes, log):
    candidates = []
    access_threshold = max(args.min_access_fraction, 2.0 / max(len(columns), 1))
    log.write(
        "CANDIDATE_SELECTION source=stats_table_or_pg_stats "
        f"query_reference_required=false access_threshold={access_threshold:.4f}\n"
    )

    for column in columns:
        key = column.lower()
        stat = column_stats.get(key)
        if stat is None:
            log.write(f"CANDIDATE_SKIP column={column} reason=no_stats\n")
            continue
        activity_count = (
            stat["access_count"]
            + stat["update_count"]
            + stat["insert_count"]
            + stat["delete_count"]
        )
        activity_fraction = (
            stat["access_fraction"]
            + stat["update_fraction"]
            + stat["insert_fraction"]
            + stat["delete_fraction"]
        )
        if total_accesses <= 0 or activity_fraction <= access_threshold:
            log.write(
                f"CANDIDATE_SKIP column={column} reason=access_fraction "
                f"all_count={activity_count} all_fraction={activity_fraction:.4f} "
                f"threshold={access_threshold:.4f}\n"
            )
            continue
        if stat["distinct_ratio"] < args.min_distinct_ratio:
            log.write(
                f"CANDIDATE_SKIP column={column} reason=distinct_ratio "
                f"distinct_ratio={stat['distinct_ratio']:.4f} threshold={args.min_distinct_ratio:.4f}\n"
            )
            continue
        log.write(
            f"CANDIDATE_KEEP column={column} access_count={stat['access_count']} "
            f"real_index_exists={key in existing_indexes} "
            f"access_fraction={stat['access_fraction']:.4f} n_distinct={stat['n_distinct']} "
            f"distinct_ratio={stat['distinct_ratio']:.4f} source={stat['source']} "
            f"most_common_vals={stat['most_common_vals']} most_common_freqs={stat['most_common_freqs']}\n"
        )
        candidates.append(column)
    return candidates


def execute_query(args, query):
    return run_psql(args, query)


def run_online_advisor(args):
    queries = read_queries(args.queries)
    if not queries:
        raise SystemExit(f"No queries found in {args.queries}")

    log_path = Path(args.log)
    jsonl_path = Path(args.jsonl)

    run_psql(args, "CREATE EXTENSION IF NOT EXISTS hypopg;")
    ensure_stats_table(args)
    run_psql(args, f"ANALYZE {qualified_table(args.table)};")

    columns = get_table_columns(args)
    row_count = get_row_count(args)
    catalog_stats = get_table_catalog_stats(args)
    cost_settings = get_planner_cost_settings(args)

    with log_path.open("w", encoding="utf-8") as log, jsonl_path.open(
        "w", encoding="utf-8"
    ) as jsonl:
        log.write(f"Online HypoPG advisor run: {datetime.now().isoformat()}\n")
        reset_stats_table(args, log)
        startup_dropped_indexes = drop_existing_advisor_indexes(args, log)
        log.write("FORMULA\n")
        log.write("  after each executed query, update public.auto_index_column_stats for the table columns\n")
        log.write("  total_count means total predicate-column accesses processed for this table so far\n")
        log.write("  per query: total_count += number of predicate columns referenced by that query\n")
        log.write("  access_count is read only from the PostgreSQL-collected stats table, never precomputed from queries.sql before execution\n")
        log.write("  eligible_column = access_count+update_count+insert_count+delete_count / total_count > max(0.05, 2/total_columns) AND distinct_count / row_count > 0.00\n")
        log.write("  candidate selection does not require the column to appear in the current query\n")
        log.write("  candidate selection includes columns that already have real indexes\n")
        log.write("  explain_baseline_scan_cost = EXPLAIN cost with no hypothetical index, logged for comparison\n")
        log.write("  hypo_scan_cost = EXPLAIN cost after hypopg_reset() + hypopg_create_index(candidate), logged for comparison\n")
        log.write("  baseline_cost = relpages*seq_page_cost + rows*cpu_tuple_cost\n")
        log.write("  create_cost = relpages*seq_page_cost + rows*cpu_tuple_cost + rows*index_columns*cpu_operator_cost + rows*log2(rows)*cpu_operator_cost\n")
        log.write("  expected_without_index = access_count * baseline_cost\n")
        log.write("  write_index_maint_cost = insert_count*C_insert + update_count*C_update + delete_count*C_delete\n")
        log.write("  C_insert = log2(rows)*cpu_operator_cost + cpu_tuple_cost\n")
        log.write("  C_update = 2*C_insert, C_delete = C_insert\n")
        log.write("  stats_maint_cost = (insert_count + update_count + delete_count) * cpu_operator_cost\n")
        log.write("  write_maint_cost = write_index_maint_cost + stats_maint_cost\n")
        log.write("  expected_with_index = create_cost + access_count * hypo_scan_cost + write_maint_cost\n")
        log.write("  INSERT increments insert_count for all columns by estimated inserted rows\n")
        log.write("  UPDATE increments access_count for predicate columns and update_count for modified columns by EXPLAIN-estimated rows\n")
        log.write("  DELETE increments access_count for predicate columns and delete_count for all columns by EXPLAIN-estimated rows\n")
        log.write(f"  ANALYZE runs every {args.analyze_every_writes} write query/queries\n")
        log.write("  choose least expected_with_index among candidates, then compare with expected_without_index\n")
        log.write("  if best with-index cost wins: use existing index if present, otherwise create it\n")
        log.write("  if without-index cost wins: curr_index_pointer=no_index\n\n")
        log.write(f"table={args.table} rows={row_count} columns={columns}\n")
        log.write(f"catalog_stats relpages={catalog_stats['relpages']} reltuples={catalog_stats['reltuples']}\n")
        log.write(f"planner_cost_settings={cost_settings}\n")
        log.write(f"startup_dropped_indexes={startup_dropped_indexes}\n")
        log.write(
            f"thresholds min_access_fraction_floor={args.min_access_fraction} "
            f"dynamic_access_fraction={max(args.min_access_fraction, 2.0 / max(len(columns), 1)):.4f} "
            f"min_distinct_ratio={args.min_distinct_ratio}\n"
        )
        log.write(f"queries={len(queries)}\n\n")

        created_indexes = []
        writes_since_analyze = 0
        for query_number, query in enumerate(queries, start=1):
            query_kind = classify_query(query)
            referenced_columns = extract_query_columns(query, columns)
            modified_columns = extract_update_set_columns(query, columns) if query_kind == "UPDATE" else set()
            affected_rows, affected_rows_source = estimate_statement_rows(args, query, query_kind)
            existing_indexes = get_existing_single_column_indexes(args)
            log.write(f"QUERY_BEGIN number={query_number} sql={query}\n")
            log.write(
                f"QUERY_KIND kind={query_kind} affected_rows_estimate={affected_rows:.2f} "
                f"affected_rows_source={affected_rows_source} "
                f"referenced_columns={sorted(referenced_columns)} "
                f"modified_columns={sorted(modified_columns)}\n"
            )
            log.write(f"EXISTING_SINGLE_COLUMN_INDEXES columns={sorted(existing_indexes)}\n")

            column_stats, total_accesses = build_column_stats(args, columns, row_count, log)
            log.write(f"TOTAL_COLUMN_ACCESSES total_count={total_accesses}\n")
            for column in columns:
                stat = column_stats.get(column.lower())
                if stat:
                    log.write(
                        f"COLUMN_STATS column={column} access_count={stat['access_count']} "
                        f"total_count={stat.get('total_count', 0)} "
                        f"insert_count={stat.get('insert_count', 0)} "
                        f"update_count={stat.get('update_count', 0)} "
                        f"delete_count={stat.get('delete_count', 0)} "
                        f"access_fraction={stat['access_fraction']:.4f} "
                        f"insert_fraction={stat['insert_fraction']:.4f} "
                        f"update_fraction={stat['update_fraction']:.4f} "
                        f"delete_fraction={stat['delete_fraction']:.4f} "
                        f"n_distinct={stat['n_distinct']} "
                        f"distinct_count={stat['distinct_count']:.2f} distinct_ratio={stat['distinct_ratio']:.4f} "
                        f"source={stat['source']} most_common_vals={stat['most_common_vals']} "
                        f"most_common_freqs={stat['most_common_freqs']}\n"
                    )

            run_psql(args, "SELECT hypopg_reset();")
            baseline_plan = explain_plan(args, query)
            explain_baseline_cost = float(baseline_plan["total_cost"])
            baseline_cost = estimate_seq_scan_cost(
                row_count,
                catalog_stats["relpages"],
                cost_settings,
            )
            log.write(
                f"BASELINE_SCAN node={baseline_plan['node_type']} explain_cost={explain_baseline_cost:.2f} "
                f"formula_cost={baseline_cost:.2f} "
                f"rows={baseline_plan['plan_rows']} plan={baseline_plan['plan_summary']}\n"
            )

            candidate_columns = choose_candidate_columns(
                args, columns, column_stats, total_accesses, existing_indexes, log
            )
            if not candidate_columns:
                message = (
                    f"Query {query_number}: no HypoPG candidates passed stats filters "
                    f"(total_count={total_accesses}, "
                    f"access_threshold={max(args.min_access_fraction, 2.0 / max(len(columns), 1)):.2f}, "
                    f"min_distinct_ratio={args.min_distinct_ratio:.2f})"
                )
                log.write(f"NO_CANDIDATES {message}\n")
                log.write(
                    f"QUERY_COST_SUMMARY query_number={query_number} column=none "
                    f"read_cost_without_index={baseline_cost:.2f} "
                    f"estimated_without_index_cost=not_applicable_no_candidate "
                    f"create_index_cost=not_applicable_no_candidate "
                    f"read_cost_with_index=not_applicable_no_candidate "
                    f"write_cost=not_applicable_no_candidate "
                    f"estimated_with_index_cost=not_applicable_no_candidate\n"
                )

            best = None
            for column in candidate_columns:
                stat = column_stats[column.lower()]
                real_index_exists = column.lower() in existing_indexes
                access_count = max(int(stat["access_count"]), 1)
                create_cost = estimate_btree_create_cost(
                    row_count,
                    catalog_stats["relpages"],
                    1,
                    cost_settings,
                )
                run_psql(args, "SELECT hypopg_reset();")
                hypo = create_hypo_index(args, [column])
                hypo_plan = explain_plan(args, query, hypo_index_sql=hypo["index_sql"])
                hypo_cost = float(hypo_plan["total_cost"])
                maint_costs = estimate_write_maintenance_cost(
                    stat,
                    row_count,
                    cost_settings,
                )
                expected_without = baseline_cost * access_count
                expected_with = (
                    create_cost
                    + hypo_cost * access_count
                    + maint_costs["write_maint_cost"]
                )
                read_cost_without_index = baseline_cost * access_count
                read_cost_with_index = hypo_cost * access_count
                write_cost = maint_costs["write_maint_cost"]
                improves_expected = expected_with < expected_without
                record = {
                    "kind": "online_candidate",
                    "query_number": query_number,
                    "query": query,
                    "column": column,
                    "real_index_exists": real_index_exists,
                    "access_count": access_count,
                    "access_fraction": stat["access_fraction"],
                    "distinct_ratio": stat["distinct_ratio"],
                    "baseline_scan_cost": explain_baseline_cost,
                    "formula_baseline_cost": baseline_cost,
                    "hypo_scan_cost": hypo_cost,
                    "estimated_create_index_cost": create_cost,
                    "insert_count": maint_costs["insert_count"],
                    "update_count": maint_costs["update_count"],
                    "delete_count": maint_costs["delete_count"],
                    "c_insert": maint_costs["c_insert"],
                    "c_update": maint_costs["c_update"],
                    "c_delete": maint_costs["c_delete"],
                    "write_index_maint_cost": maint_costs["write_index_maint_cost"],
                    "stats_maint_cost": maint_costs["stats_maint_cost"],
                    "write_maint_cost": maint_costs["write_maint_cost"],
                    "expected_without_index": expected_without,
                    "expected_with_index": expected_with,
                    "improves_expected": improves_expected,
                    "hypopg_indexname": hypo["hypopg_indexname"],
                    "index_sql": hypo["index_sql"],
                    "baseline_plan": baseline_plan,
                    "hypo_plan": hypo_plan,
                }
                jsonl.write(json.dumps(record, sort_keys=True) + "\n")
                log.write(
                    f"HYPOPG_RESULT column={column} hypopg_index={hypo['hypopg_indexname']} "
                    f"real_index_exists={real_index_exists} "
                    f"explain_baseline_scan_cost={explain_baseline_cost:.2f} "
                    f"formula_baseline_cost={baseline_cost:.2f} hypo_scan_cost={hypo_cost:.2f} "
                    f"create_cost={create_cost:.2f} expected_without_index={expected_without:.2f} "
                    f"write_index_maint_cost={maint_costs['write_index_maint_cost']:.2f} "
                    f"stats_maint_cost={maint_costs['stats_maint_cost']:.2f} "
                    f"write_maint_cost={maint_costs['write_maint_cost']:.2f} "
                    f"expected_with_index={expected_with:.2f} improves_expected={improves_expected} "
                    f"plan={hypo_plan['plan_summary']}\n"
                )
                log.write(
                    f"QUERY_COST_SUMMARY query_number={query_number} column={column} "
                    f"real_index_exists={real_index_exists} "
                    f"access_count={access_count} "
                    f"read_cost_without_index={read_cost_without_index:.2f} "
                    f"estimated_without_index_cost={expected_without:.2f} "
                    f"create_index_cost={create_cost:.2f} "
                    f"read_cost_with_index={read_cost_with_index:.2f} "
                    f"write_cost={write_cost:.2f} "
                    f"estimated_with_index_cost={expected_with:.2f}\n"
                )
                if best is None or expected_with < best["expected_with_index"]:
                    best = record

            run_psql(args, "SELECT hypopg_reset();")

            created_index = None
            query_index_decision = "use_no_index"
            curr_index_pointer = "no_index"
            if best is None:
                query_index_decision = "use_no_index"
                log.write(
                    "QUERY_INDEX_DECISION "
                    f"query_number={query_number} decision=use_no_index "
                    f"curr_index_pointer={curr_index_pointer} "
                    "reason=no_candidate "
                    "without_index_cost=not_applicable_no_candidate "
                    "best_with_index_cost=not_applicable_no_candidate "
                    "best_create_cost=not_applicable_no_candidate "
                    "best_read_cost=not_applicable_no_candidate "
                    "best_write_cost=not_applicable_no_candidate\n"
                )
                log.write("CREATE_INDEX_DECISION decision=do_not_create reason=no_candidate\n")
            elif best["expected_with_index"] < best["expected_without_index"]:
                curr_index_pointer = best["column"]
                best_read_cost = best["hypo_scan_cost"] * best["access_count"]
                best_write_cost = best["write_maint_cost"]
                if best["real_index_exists"]:
                    query_index_decision = "use_existing_index"
                    log.write(
                        "QUERY_INDEX_DECISION "
                        f"query_number={query_number} decision=use_existing_index "
                        f"curr_index_pointer={curr_index_pointer} "
                        f"without_index_cost={best['expected_without_index']:.2f} "
                        f"best_with_index_cost={best['expected_with_index']:.2f} "
                        f"best_create_cost={best['estimated_create_index_cost']:.2f} "
                        f"best_read_cost={best_read_cost:.2f} "
                        f"best_write_cost={best_write_cost:.2f}\n"
                    )
                    log.write(
                        f"CREATE_INDEX_DECISION decision=use_existing column={best['column']} "
                        f"reason=real_index_already_exists\n"
                    )
                else:
                    query_index_decision = "create_and_use_index"
                    log.write(
                        "QUERY_INDEX_DECISION "
                        f"query_number={query_number} decision=create_and_use_index "
                        f"curr_index_pointer={curr_index_pointer} "
                        f"without_index_cost={best['expected_without_index']:.2f} "
                        f"best_with_index_cost={best['expected_with_index']:.2f} "
                        f"best_create_cost={best['estimated_create_index_cost']:.2f} "
                        f"best_read_cost={best_read_cost:.2f} "
                        f"best_write_cost={best_write_cost:.2f}\n"
                    )
                    created_index, real_index_sql = create_real_index(args, [best["column"]])
                    created_indexes.append(created_index)
                    run_psql(args, f"ANALYZE {qualified_table(args.table)};")
                    log.write(
                        f"CREATE_INDEX_DECISION decision=create column={best['column']} "
                        f"index={created_index} sql={real_index_sql}\n"
                    )
            else:
                query_index_decision = "use_no_index"
                curr_index_pointer = "no_index"
                best_read_cost = best["hypo_scan_cost"] * best["access_count"]
                best_write_cost = best["write_maint_cost"]
                log.write(
                    "QUERY_INDEX_DECISION "
                    f"query_number={query_number} decision=use_no_index "
                    f"curr_index_pointer={curr_index_pointer} "
                    f"without_index_cost={best['expected_without_index']:.2f} "
                    f"best_with_index_cost={best['expected_with_index']:.2f} "
                    f"best_create_cost={best['estimated_create_index_cost']:.2f} "
                    f"best_read_cost={best_read_cost:.2f} "
                    f"best_write_cost={best_write_cost:.2f}\n"
                )
                log.write("CREATE_INDEX_DECISION decision=do_not_create reason=without_index_cost_is_lower\n")

            final_plan = explain_plan(args, query)
            log.write(
                f"FINAL_PLAN node={final_plan['node_type']} cost={final_plan['total_cost']} "
                f"rows={final_plan['plan_rows']} plan={final_plan['plan_summary']}\n"
            )

            if args.execute_queries:
                output = execute_query(args, query)
                rows_returned = 0 if output == "" else len(output.splitlines())
                log.write(f"QUERY_EXECUTED number={query_number} rows_returned={rows_returned}\n")
                upsert_column_stats(
                    args,
                    columns,
                    referenced_columns,
                    modified_columns,
                    query_kind,
                    affected_rows,
                    row_count,
                    log,
                )
                total_increment = len(referenced_columns)
                log.write(
                    f"Updated stats after query {query_number}: "
                    f"total_count +{total_increment}, "
                    f"access_count +1 for {sorted(referenced_columns) or 'no predicate columns'}\n"
                )
                if query_kind in {"INSERT", "UPDATE", "DELETE"}:
                    writes_since_analyze += 1
                    if writes_since_analyze >= args.analyze_every_writes:
                        run_psql(args, f"ANALYZE {qualified_table(args.table)};")
                        row_count = get_row_count(args)
                        catalog_stats = get_table_catalog_stats(args)
                        upsert_column_stats(
                            args,
                            columns,
                            set(),
                            set(),
                            "REFRESH",
                            0,
                            row_count,
                            log,
                        )
                        writes_since_analyze = 0
                        log.write(
                            f"WRITE_ANALYZE query_number={query_number} "
                            f"analyze_every_writes={args.analyze_every_writes} "
                            f"rows={row_count} relpages={catalog_stats['relpages']}\n"
                        )
            else:
                log.write(f"QUERY_NOT_EXECUTED number={query_number} reason=--no-execute-queries\n")

            jsonl.write(
                json.dumps(
                    {
                        "kind": "online_query_summary",
                        "query_number": query_number,
                        "query": query,
                        "created_index": created_index,
                        "query_index_decision": query_index_decision,
                        "curr_index_pointer": curr_index_pointer,
                        "final_plan": final_plan,
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            log.write(f"QUERY_END number={query_number}\n\n")

        log.write(f"CREATED_INDEXES indexes={created_indexes}\n")
        dropped_indexes = []
        for index_name in created_indexes:
            drop_sql = drop_real_index(args, index_name)
            dropped_indexes.append(index_name)
            log.write(f"DROP_CREATED_INDEX index={index_name} sql={drop_sql}\n")
        log.write(f"DROPPED_CREATED_INDEXES indexes={dropped_indexes}\n")

    print(f"Created indexes then dropped: {created_indexes or 'none'}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--dbname", default="postgres")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--user")
    parser.add_argument("--table", default="public.online_retail")
    parser.add_argument("--queries", default="queries.sql")
    parser.add_argument("--log", default="hypopg_advisor.log")
    parser.add_argument("--jsonl", default="hypopg_advisor_candidates.jsonl")
    parser.add_argument("--max-index-columns", type=int)
    parser.add_argument("--stats-table", default="public.auto_index_column_stats")
    parser.add_argument("--stats-column-col", default="column_name")
    parser.add_argument("--stats-access-col", default="access_count")
    parser.add_argument("--stats-ndistinct-col", default="n_distinct")
    parser.add_argument("--stats-total-col", default="total_count")
    parser.add_argument("--min-access-fraction", type=float, default=0.05)
    parser.add_argument("--min-distinct-ratio", type=float, default=0.00)
    parser.add_argument("--analyze-every-writes", type=int, default=1)
    parser.add_argument("--create-concurrently", action="store_true")
    parser.add_argument("--no-execute-queries", dest="execute_queries", action="store_false")
    parser.set_defaults(execute_queries=True)
    parser.add_argument(
        "--legacy-exhaustive",
        action="store_true",
        help="Run the old exhaustive workload advisor instead of the online per-query advisor.",
    )
    parser.add_argument(
        "--create-best",
        action="store_true",
        help="Create the lowest-cost real index after logging all candidates.",
    )
    args = parser.parse_args()
    args.analyze_every_writes = max(args.analyze_every_writes, 1)

    if not args.legacy_exhaustive:
        run_online_advisor(args)
        return

    queries = read_queries(args.queries)
    if not queries:
        raise SystemExit(f"No queries found in {args.queries}")

    log_path = Path(args.log)
    jsonl_path = Path(args.jsonl)

    run_psql(args, "CREATE EXTENSION IF NOT EXISTS hypopg;")
    run_psql(args, f"ANALYZE {qualified_table(args.table)};")

    columns = get_table_columns(args)
    if args.max_index_columns:
        max_width = args.max_index_columns
    else:
        max_width = len(columns)
    expected_candidates = sum(
        len(list(itertools.combinations(columns, width)))
        for width in range(1, max_width + 1)
    )

    row_count = get_row_count(args)
    stats = get_stats(args, columns)
    catalog_stats = get_table_catalog_stats(args)
    cost_settings = get_planner_cost_settings(args)

    with log_path.open("w", encoding="utf-8") as log, jsonl_path.open(
        "w", encoding="utf-8"
    ) as jsonl:
        log.write(f"HypoPG advisor run: {datetime.now().isoformat()}\n")
        log.write(f"table={args.table} rows={row_count}\n")
        log.write(f"columns={columns}\n")
        log.write(f"n_distinct={stats}\n")
        log.write(
            "create_index_cost_model="
            "relpages*seq_page_cost + rows*cpu_tuple_cost + "
            "rows*index_columns*cpu_operator_cost + "
            "rows*log2(rows)*cpu_operator_cost\n"
        )
        log.write(
            f"catalog_stats relpages={catalog_stats['relpages']} "
            f"reltuples={catalog_stats['reltuples']}\n"
        )
        log.write(f"planner_cost_settings={cost_settings}\n")
        log.write(f"queries={len(queries)}\n\n")
        log.write(
            f"candidate_count={expected_candidates} "
            f"(all non-empty column subsets up to width {max_width})\n\n"
        )

        run_psql(args, "SELECT hypopg_reset();")
        baseline_cost = 0.0
        baseline_plans = []
        log.write("BASELINE no_hypothetical_index\n")
        for query in queries:
            plan = explain_plan(args, query)
            baseline_cost += float(plan["total_cost"])
            baseline_plans.append({"query": query, "plan": plan})
            log.write(
                "  "
                f"node={plan['node_type']} "
                f"cost={plan['total_cost']} "
                f"rows={plan['plan_rows']} "
                f"plan={plan['plan_summary']} "
                f"query={query}\n"
            )
        log.write(f"BASELINE total_cost={baseline_cost:.2f}\n\n")
        jsonl.write(
            json.dumps(
                {
                    "kind": "baseline_no_index",
                    "columns": [],
                    "total_cost": baseline_cost,
                    "query_plans": baseline_plans,
                },
                sort_keys=True,
            )
            + "\n"
        )

        best_index = None
        total_candidates = 0

        for width in range(1, max_width + 1):
            for columns_tuple in itertools.combinations(columns, width):
                candidate_columns = list(columns_tuple)
                total_candidates += 1
                run_psql(args, "SELECT hypopg_reset();")
                hypo = create_hypo_index(args, candidate_columns)

                forced_query_plans = []
                forced_total_cost = 0.0
                create_cost = estimate_btree_create_cost(
                    row_count,
                    catalog_stats["relpages"],
                    len(candidate_columns),
                    cost_settings,
                )
                for query in queries:
                    forced_plan = explain_plan(
                        args,
                        query,
                        enable_seqscan=False,
                        hypo_index_sql=hypo["index_sql"],
                    )
                    forced_total_cost += float(forced_plan["total_cost"])
                    forced_query_plans.append({"query": query, "plan": forced_plan})

                record = {
                    "columns": candidate_columns,
                    "index_sql": hypo["index_sql"],
                    "hypopg_indexname": hypo["hypopg_indexname"],
                    "estimated_create_index_cost": create_cost,
                    "total_cost_if_index_chosen": forced_total_cost,
                    "create_plus_scan_total_cost": create_cost + forced_total_cost,
                    "forced_query_plans": forced_query_plans,
                    "is_no_index": False,
                }
                jsonl.write(json.dumps(record, sort_keys=True) + "\n")
                log.write(
                    f"CANDIDATE columns={candidate_columns} "
                    f"estimated_create_index_cost={create_cost:.2f} "
                    f"total_cost_if_index_chosen={forced_total_cost:.2f} "
                    f"create_plus_scan_total_cost={create_cost + forced_total_cost:.2f} "
                    f"hypopg_index={hypo['hypopg_indexname']}\n"
                )
                for query_plan in forced_query_plans:
                    plan = query_plan["plan"]
                    log.write(
                        "  "
                        f"node={plan['node_type']} "
                        f"cost={plan['total_cost']} "
                        f"rows={plan['plan_rows']} "
                        f"plan={plan['plan_summary']} "
                        f"query={query_plan['query']}\n"
                    )

                if best_index is None or (
                    create_cost + forced_total_cost
                    < best_index["create_plus_scan_total_cost"]
                ):
                    best_index = record

        run_psql(args, "SELECT hypopg_reset();")

        log.write("\n")
        log.write(f"tested_candidates={total_candidates}\n")
        index_name = None
        log.write(f"NO_INDEX total_cost={baseline_cost:.2f}\n")
        if best_index:
            log.write(
                "MIN_INDEX_CANDIDATE "
                f"columns={best_index['columns']} "
                f"estimated_create_index_cost={best_index['estimated_create_index_cost']:.2f} "
                f"total_cost_if_index_chosen={best_index['total_cost_if_index_chosen']:.2f} "
                f"create_plus_scan_total_cost={best_index['create_plus_scan_total_cost']:.2f}\n"
            )
            log.write(f"MIN_INDEX_CANDIDATE hypothetical_sql={best_index['index_sql']}\n")

            if args.create_best:
                index_name, real_index_sql = create_real_index(args, best_index["columns"])
                run_psql(args, f"ANALYZE {qualified_table(args.table)};")
                log.write(f"CREATED real_index={index_name}\n")
                log.write(f"CREATED sql={real_index_sql}\n\n")
            else:
                log.write("CREATED real_index=none; pass --create-best to create it\n\n")

        if args.create_best:
            for query in queries:
                plan = explain_plan(args, query, analyze=True)
                log.write(
                    "AFTER_CREATE "
                    f"node={plan['node_type']} "
                    f"cost={plan['total_cost']} "
                    f"rows={plan['plan_rows']} "
                    f"actual_rows={plan['actual_rows']} "
                    f"planning_ms={plan['planning_time_ms']} "
                    f"execution_ms={plan['execution_time_ms']} "
                    f"plan={plan['plan_summary']} "
                    f"query={query}\n"
                )

    print(f"Tested {total_candidates} hypothetical indexes.")
    print(f"No-index total cost: {baseline_cost:.2f}")
    if best_index:
        print(f"Lowest create+scan index columns: {best_index['columns']}")
        print(
            "Estimated create-index cost: "
            f"{best_index['estimated_create_index_cost']:.2f}"
        )
        print(
            "Lowest forced-index total cost: "
            f"{best_index['total_cost_if_index_chosen']:.2f}"
        )
        print(
            "Lowest create+scan total cost: "
            f"{best_index['create_plus_scan_total_cost']:.2f}"
        )
    print(f"Created real index: {index_name or 'none'}")
    print(f"Logs: {log_path}")
    print(f"Candidate JSONL: {jsonl_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
