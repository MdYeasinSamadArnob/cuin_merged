"""
CUIN v2 - Doris 4 Capability Probe (migration plan Phase 0)

Connects to a real Doris instance over the MySQL protocol and
executes one assertion per SQL construct engine.ports.doris_dialect
depends on. Writes a pass/fail matrix to docs/doris_compatibility.md.
Nothing downstream of Phase 0 in the migration plan should be trusted
until every row here is green or has a documented workaround -- do
not hand-wave function availability.

Usage:
    python tools/doris_probe.py --host 127.0.0.1 --port 9130 --user root

Requires a Doris FE+BE reachable over the MySQL protocol. Standard
requirements for the BE to start at all (undocumented until you hit
them, so noting here): host kernel `vm.max_map_count >= 2000000`, and
swap disabled OR the BE launched with `SKIP_CHECK_ULIMIT=true` -- see
docs/doris_compatibility.md "Environment notes".
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@dataclass
class ProbeResult:
    category: str
    name: str
    sql: str
    passed: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    note: str = ""


@dataclass
class ProbeReport:
    doris_version: str = ""
    results: List[ProbeResult] = field(default_factory=list)

    def add(self, category: str, name: str, sql: str, conn, expect=None, note: str = "") -> ProbeResult:
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                value = row[0] if row else None
            passed = True if expect is None else (value == expect)
            r = ProbeResult(category=category, name=name, sql=sql, passed=passed, result=value, note=note)
        except Exception as e:
            r = ProbeResult(category=category, name=name, sql=sql, passed=False, error=str(e), note=note)
        self.results.append(r)
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {category}/{name}: {sql!r} -> {r.result if r.passed else r.error}")
        return r


def run_probe(host: str, port: int, user: str, password: str) -> ProbeReport:
    import pymysql

    conn = pymysql.connect(host=host, port=port, user=user, password=password, autocommit=True)
    report = ProbeReport()

    with conn.cursor() as cur:
        cur.execute("SELECT VERSION()")
        report.doris_version = cur.fetchone()[0]
    print(f"Connected to Doris, VERSION() = {report.doris_version}")

    A = report.add

    # ---- arrays -------------------------------------------------------
    A("array", "array_intersect", "SELECT array_intersect(['a','b','c'], ['b','c','d'])", conn)
    A("array", "array_union+distinct", "SELECT array_distinct(array_union(['a','b'], ['b','c']))", conn)
    A("array", "array_distinct", "SELECT array_distinct(['a','a','b'])", conn)
    A("array", "array_sort", "SELECT array_sort(['c','a','b'])", conn)
    A("array", "array_contains", "SELECT array_contains(['a','b'], 'a')", conn, expect=1)
    A("array", "array_size", "SELECT array_size(['a','b','c'])", conn, expect=3)
    A("array", "array_join", "SELECT array_join(['a','b'], '|')", conn, expect="a|b")
    A("array", "array_filter_lambda", "SELECT array_filter(x -> x != '', ['a','','b'])", conn,
      note="Lambda comes FIRST, array second -- opposite of DuckDB's list_filter(array, lambda)")
    A("array", "element_at_1based", "SELECT element_at(['x','y','z'], 1)", conn, expect="x")
    A("array", "array_slice_from", "SELECT array_slice(['a','b','c','d'], 2)", conn)
    A("array", "collect_set_aggregate", "SELECT array_sort(collect_set(x)) FROM (SELECT 'b' AS x UNION ALL SELECT 'a' UNION ALL SELECT 'a') t", conn)

    # ---- strings / regex ------------------------------------------------
    A("string", "concat_not_pipe_pipe", "SELECT CONCAT('a', ':', 'b')", conn, expect="a:b",
      note="CRITICAL: Doris's || is LOGICAL OR (MySQL default sql_mode), not string concat -- "
           "'a' || 'b' silently returns NULL, not 'ab'. engine.rules.compiler must use "
           "dialect.concat(), never a literal || for string building.")
    A("string", "pipe_pipe_is_not_concat", "SELECT ('a' || 'b') IS NULL AS pipe_is_not_concat", conn, expect=1,
      note="Confirms the above negatively -- this documents the footgun so it can't silently regress")
    A("string", "backslash_escape_in_literal", r"SELECT regexp_replace('a b', '\\s', 'X')", conn, expect="aXb",
      note="CRITICAL: Doris string literals process backslash escapes (MySQL-style) -- '\\s' in SQL parses to "
           "literal 's' (backslash silently dropped), so \\s/\\d/\\b/\\.-style regex patterns need the "
           "backslash DOUBLED to survive to the regex engine. DuckDB string literals do NOT do this. "
           "engine.ports.doris_dialect.DorisDialect.quote_str() doubles every backslash for exactly this reason.")
    A("string", "backslash_single_is_stripped", r"SELECT regexp_replace('a b', '\s', 'X')", conn, expect="a b",
      note="Negative control for the row above -- a single backslash is a silent no-op, not an error, which is "
           "what made this dangerous: it fails quietly rather than raising.")
    A("string", "split_by_string", "SELECT split_by_string('a,b,c', ',')", conn)
    A("string", "char_length", "SELECT char_length('hello')", conn, expect=5)
    A("string", "regexp_operator", "SELECT ('01712345678' REGEXP '^01[3-9][0-9]{8}$')", conn, expect=1)
    A("string", "regexp_replace_global_default", "SELECT regexp_replace('a1b2c3', '[0-9]', '')", conn, expect="abc",
      note="Confirms Doris regexp_replace has no separate 'g' flag -- it always replaces all matches")
    A("string", "regexp_extract_group", "SELECT regexp_extract('TIN:12345', '^([A-Z]+):(.*)$', 2)", conn, expect="12345")
    A("string", "translate_fn", "SELECT translate('café', 'é', 'e')", conn, expect="cafe",
      note="Used as the strip_accents() fallback -- see doris_dialect.py")
    A("string", "md5", "SELECT md5('hello')", conn)
    A("string", "substr_1based", "SELECT substr('hello', 2, 3)", conn, expect="ell")
    A("string", "select_star_except", "SELECT t.* EXCEPT (b) FROM (SELECT 1 AS a, 2 AS b, 3 AS c) t", conn)

    # ---- scalar / date --------------------------------------------------
    A("scalar", "least", "SELECT least('B','A')", conn, expect="A")
    A("scalar", "greatest", "SELECT greatest('B','A')", conn, expect="B")
    A("date", "year_fn", "SELECT year('2026-03-05')", conn, expect=2026)
    A("date", "str_to_date", "SELECT str_to_date('2026-03-05T10:20:30', '%Y-%m-%dT%H:%i:%s')", conn)
    A("date", "date_format", "SELECT date_format(str_to_date('2026-03-05','%Y-%m-%d'), '%Y-%m-%d')", conn, expect="2026-03-05")

    # ---- relational fragments -------------------------------------------
    A("relational", "lateral_view_explode",
      "SELECT t.customer_code, _lv_v.v FROM (SELECT 'C1' AS customer_code, ['a','b'] AS arr) t "
      "LATERAL VIEW EXPLODE(t.arr) _lv_v AS v LIMIT 1", conn, expect="C1",
      note="Exploded column MUST be qualified by its LATERAL VIEW alias -- bare 'v' fails to resolve")
    A("relational", "ctas_no_distribution",
      "SELECT 1", conn, note="CTAS itself is validated by doris_ddl smoke test, not a scalar probe")

    # ---- array + lambda combined (mirrors explode.py's list_extract/list_filter chain)
    A("combo", "array_filter_then_element_at",
      "SELECT element_at(array_filter(v -> v IS NOT NULL, ['x', NULL, 'y']), 1)", conn, expect="x")

    return report


def render_markdown(report: ProbeReport) -> str:
    lines = [
        "# Doris SQL Compatibility Matrix",
        "",
        f"Generated by `backend/tools/doris_probe.py` against a live Doris instance.",
        f"`SELECT VERSION()` reported: `{report.doris_version}` "
        f"(Doris reports a MySQL-protocol-compatible version string, not its own release "
        f"number -- the container image tag is the source of truth for the actual Doris version).",
        "",
        "## Environment notes",
        "",
        "The Doris BE (storage/compute node) refuses to start unless:",
        "- Host kernel `vm.max_map_count >= 2000000` (`sysctl -w vm.max_map_count=2000000`)",
        "- Swap is disabled, OR the BE is launched with `SKIP_CHECK_ULIMIT=true` in its environment",
        "",
        "Both are standard, documented Doris production requirements (not specific to this "
        "probe) -- provision them via a host sysctl / init container in any real deployment.",
        "",
        "## Results",
        "",
        "| Category | Capability | Status | SQL | Result / Error | Note |",
        "|---|---|---|---|---|---|",
    ]
    for r in report.results:
        status = "PASS" if r.passed else "**FAIL**"
        detail = str(r.result) if r.passed else f"`{r.error}`"
        sql_escaped = r.sql.replace("|", "\\|")
        lines.append(f"| {r.category} | {r.name} | {status} | `{sql_escaped}` | {detail} | {r.note} |")

    n_pass = sum(1 for r in report.results if r.passed)
    n_total = len(report.results)
    lines += [
        "",
        f"**{n_pass}/{n_total} capabilities confirmed.**",
        "",
        "## Mapping to engine.ports.doris_dialect.DorisDialect",
        "",
        "Every row above corresponds 1:1 to a method in `DorisDialect` -- see that module's "
        "docstring. If a row is FAIL, the corresponding dialect method must not be trusted "
        "until a workaround is added and this probe re-run.",
    ]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Probe a live Doris instance's SQL capabilities")
    parser.add_argument("--host", default=os.environ.get("DORIS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DORIS_PORT", "9130")))
    parser.add_argument("--user", default=os.environ.get("DORIS_USER", "root"))
    parser.add_argument("--password", default=os.environ.get("DORIS_PASSWORD", ""))
    parser.add_argument(
        "--out", default=os.path.join(os.path.dirname(__file__), "..", "..", "docs", "doris_compatibility.md")
    )
    parser.add_argument("--json-out", default=None, help="Optional path to also dump raw results as JSON")
    args = parser.parse_args()

    report = run_probe(args.host, args.port, args.user, args.password)

    md = render_markdown(report)
    out_path = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        f.write(md)
    print(f"\nWrote {out_path}")

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump([r.__dict__ for r in report.results], f, indent=2, default=str)

    n_fail = sum(1 for r in report.results if not r.passed)
    if n_fail:
        print(f"\n{n_fail} capability check(s) FAILED -- see {out_path}")
        sys.exit(1)
    print("\nAll capability checks passed.")


if __name__ == "__main__":
    main()
