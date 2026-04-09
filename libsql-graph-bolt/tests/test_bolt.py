#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["neo4j>=5.0"]
# ///
"""Integration tests for libsql-graph Bolt v4.4 server."""

import os
import sys

from neo4j import GraphDatabase

BOLT_URI = f"bolt://localhost:{os.environ.get('BOLT_PORT', '7687')}"
AUTH = ("neo4j", "test")


def test_create_and_query(driver):
    with driver.session() as s:
        s.run("CREATE (n:Person {name: 'Alice', age: 30})").consume()
        s.run("CREATE (n:Person {name: 'Bob', age: 25})").consume()

        records = list(s.run("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name"))
        assert len(records) == 2
        assert records[0]["n.name"] == "Alice"
        assert records[0]["n.age"] == 30
        assert records[1]["n.name"] == "Bob"
        assert records[1]["n.age"] == 25


def test_filtered_query(driver):
    with driver.session() as s:
        records = list(s.run("MATCH (n:Person {name: 'Alice'}) RETURN n.name, n.age"))
        assert len(records) == 1
        assert records[0]["n.name"] == "Alice"
        assert records[0]["n.age"] == 30


def test_parameterized_query(driver):
    with driver.session() as s:
        records = list(s.run(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.age",
            name="Bob",
        ))
        assert len(records) == 1
        assert records[0]["n.age"] == 25


def test_transaction_rollback(driver):
    with driver.session() as s:
        with s.begin_transaction() as tx:
            tx.run("CREATE (n:Person {name: 'Ghost', age: 99})")
            tx.rollback()

        records = list(s.run("MATCH (n:Person {name: 'Ghost'}) RETURN n"))
        assert len(records) == 0


def test_transaction_commit(driver):
    with driver.session() as s:
        with s.begin_transaction() as tx:
            tx.run("CREATE (n:Person {name: 'Charlie', age: 35})")
            tx.commit()

        records = list(s.run("MATCH (n:Person {name: 'Charlie'}) RETURN n.age"))
        assert len(records) == 1
        assert records[0]["n.age"] == 35


def test_multiple_queries_one_session(driver):
    with driver.session() as s:
        for i in range(5):
            s.run(f"CREATE (n:Counter {{val: {i}}})").consume()

        records = list(s.run("MATCH (n:Counter) RETURN n.val ORDER BY n.val"))
        assert len(records) == 5
        for i, r in enumerate(records):
            assert r["n.val"] == i


def test_syntax_error(driver):
    with driver.session() as s:
        try:
            s.run("THIS IS NOT CYPHER").consume()
            assert False, "Should have raised"
        except Exception:
            pass


def main():
    driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
    tests = [
        test_create_and_query,
        test_filtered_query,
        test_parameterized_query,
        test_transaction_rollback,
        test_transaction_commit,
        test_multiple_queries_one_session,
        test_syntax_error,
    ]

    passed = 0
    failed = 0
    for test in tests:
        name = test.__name__
        try:
            test(driver)
            print(f"  PASS  {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}", file=sys.stderr)
            failed += 1

    driver.close()
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
