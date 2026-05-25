#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["libsql-experimental"]
# ///
"""Integration tests for libsql full-text search (FTS5).

Mirrors libsql-graph-bolt/tests/test_bolt.py, but for the SQL side: instead of
talking to the Bolt server with the neo4j driver, this opens an *embedded*
libsql database directly via the `libsql-experimental` Python package and
exercises the FTS5 virtual-table module (CREATE VIRTUAL TABLE ... USING fts5).

No server needed -- libsql runs in-process. Run it with uv:

    uv run scripts/test_libsql_fts.py

If your default interpreter has no prebuilt wheel, pin one that does:

    uv run --python 3.12 scripts/test_libsql_fts.py
"""

import os
import sys
import tempfile

import libsql_experimental as libsql

# (title, body) corpus. rowids are assigned 1..N in insert order.
DOCS = [
    ("Rust Programming", "Rust is a systems programming language focused on memory safety"),
    ("SQLite Database", "SQLite is an embedded sql database engine used everywhere"),
    ("libsql Fork", "libsql is a fork of sqlite adding a server and replication"),
    ("Graph Engine", "a property graph engine supports the cypher query language"),
    ("Full Text Search", "fts5 enables full text search over documents stored in sqlite"),
]


def setup():
    """Open a fresh embedded libsql db, create + populate the FTS5 table."""
    path = os.path.join(tempfile.mkdtemp(prefix="libsql_fts_"), "fts.db")
    con = libsql.connect(path)
    cur = con.cursor()
    cur.execute("CREATE VIRTUAL TABLE docs USING fts5(title, body)")
    cur.executemany("INSERT INTO docs(title, body) VALUES (?, ?)", DOCS)
    con.commit()
    return con


def titles(con, match):
    rows = con.execute(
        "SELECT title FROM docs WHERE docs MATCH ? ORDER BY rowid", (match,)
    ).fetchall()
    return [r[0] for r in rows]


def test_basic_match(con):
    # 'sqlite' (case-insensitive via unicode61) -> docs 2, 3, 5
    res = titles(con, "sqlite")
    assert res == ["SQLite Database", "libsql Fork", "Full Text Search"], res


def test_phrase_query(con):
    # Quoted = adjacent phrase. Only doc 1 has "programming language".
    res = titles(con, '"programming language"')
    assert res == ["Rust Programming"], res


def test_prefix_query(con):
    # Prefix token. Only "programming" starts with "progr".
    res = titles(con, "progr*")
    assert res == ["Rust Programming"], res


def test_boolean_or(con):
    # cypher -> doc 4, memory -> doc 1
    res = titles(con, "cypher OR memory")
    assert res == ["Rust Programming", "Graph Engine"], res


def test_boolean_and(con):
    # 'sql' AND 'embedded' both only in doc 2 ('sqlite' is a different token).
    res = titles(con, "sql AND embedded")
    assert res == ["SQLite Database"], res


def test_boolean_not(con):
    # sqlite docs {2,3,5} minus those containing 'fork' (doc 3) -> {2,5}
    res = titles(con, "sqlite NOT fork")
    assert res == ["SQLite Database", "Full Text Search"], res


def test_column_filter(con):
    # Restrict the match to the `title` column: only doc 4's title has 'graph'.
    res = titles(con, "title : graph")
    assert res == ["Graph Engine"], res


def test_bm25_ranking(con):
    # bm25() returns relevance (more negative = better). ORDER BY ascending
    # must yield monotonically non-decreasing scores.
    rows = con.execute(
        "SELECT title, bm25(docs) FROM docs WHERE docs MATCH 'sqlite' "
        "ORDER BY bm25(docs)"
    ).fetchall()
    scores = [r[1] for r in rows]
    assert len(scores) == 3, rows
    assert scores == sorted(scores), scores


def test_highlight_and_snippet(con):
    # highlight() wraps matched terms in the named column (col index 1 = body).
    hi = con.execute(
        "SELECT highlight(docs, 1, '[', ']') FROM docs "
        "WHERE docs MATCH 'search' "
    ).fetchall()
    assert any("[" in r[0] and "]" in r[0] for r in hi), hi
    # snippet() returns an excerpt with the same markers.
    sn = con.execute(
        "SELECT snippet(docs, 1, '<b>', '</b>', '...', 8) FROM docs "
        "WHERE docs MATCH 'search'"
    ).fetchall()
    assert any("<b>" in r[0] for r in sn), sn


def test_delete_reindexes(con):
    # Deleting a row must remove it from subsequent matches.
    before = titles(con, "sqlite")
    assert "SQLite Database" in before, before
    con.execute("DELETE FROM docs WHERE rowid = 2")
    con.commit()
    after = titles(con, "sqlite")
    assert after == ["libsql Fork", "Full Text Search"], after


def main():
    con = setup()
    tests = [
        test_basic_match,
        test_phrase_query,
        test_prefix_query,
        test_boolean_or,
        test_boolean_and,
        test_boolean_not,
        test_column_filter,
        test_bm25_ranking,
        test_highlight_and_snippet,
        test_delete_reindexes,  # mutates the table -> keep last
    ]

    passed = 0
    failed = 0
    for test in tests:
        name = test.__name__
        try:
            test(con)
            print(f"  PASS  {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}", file=sys.stderr)
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
