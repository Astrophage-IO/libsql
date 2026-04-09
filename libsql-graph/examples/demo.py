#!/usr/bin/env python3
"""
libsql-graph Python demo — data ingestion and Cypher queries.

Usage:
    cargo build -p libsql-graph --bin graph_cli
    python3 libsql-graph/examples/demo.py
"""

import subprocess
import json
import os

CLI = os.path.join(os.path.dirname(__file__), "../../target/debug/graph_cli")
DB = "/tmp/libsql_graph_demo.db"


class GraphSession:
    def __init__(self, db_path):
        self.proc = subprocess.Popen(
            [CLI, db_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _send(self, cmd):
        self.proc.stdin.write(cmd.strip() + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline().strip()
        if not line:
            raise RuntimeError("CLI returned empty response")
        return json.loads(line)

    def query(self, cypher):
        r = self._send(cypher)
        if "error" in r:
            raise RuntimeError(r["error"])
        return r

    def create_rel(self, src_id, dst_id, rel_type):
        r = self._send(f"!rel {src_id} {dst_id} {rel_type}")
        if "error" in r:
            raise RuntimeError(r["error"])
        return r["rel_id"]

    def close(self):
        self.proc.stdin.close()
        self.proc.wait()


def print_table(label, r):
    print(f"\n  {label}")
    if not r["rows"]:
        print("  (no results)")
        return
    cols = r["columns"]
    widths = [len(c) for c in cols]
    for row in r["rows"]:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))
    header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    print(f"  {header}")
    print(f"  {sep}")
    for row in r["rows"]:
        line = " | ".join(str(v).ljust(w) for v, w in zip(row, widths))
        print(f"  {line}")


# ─── Cleanup ───
for f in [DB, DB + "-wal"]:
    if os.path.exists(f):
        os.remove(f)

print("=" * 60)
print("  libsql-graph Python Demo")
print("  Cypher data ingestion + queries")
print("=" * 60)

session = GraphSession(DB)

# ─── 1. Ingest nodes via Cypher ───
print("\n### Phase 1: Create Nodes via Cypher ###")

people = [
    ("Alice", 30, "Engineering"),
    ("Bob", 25, "Marketing"),
    ("Charlie", 35, "Engineering"),
    ("Diana", 28, "Sales"),
    ("Eve", 32, "Engineering"),
    ("Frank", 40, "Marketing"),
    ("Grace", 27, "Sales"),
    ("Hank", 33, "Engineering"),
]
node_ids = {}
for i, (name, age, dept) in enumerate(people):
    session.query(f"CREATE (p:Person {{name: '{name}', age: {age}, department: '{dept}'}})")
    node_ids[name] = i
    print(f"  + Person: {name} (id={i}, age={age}, dept={dept})")

for i, name in enumerate(["Acme Corp", "Globex Inc", "Initech"]):
    session.query(f"CREATE (c:Company {{name: '{name}'}})")
    node_ids[name] = len(people) + i
    print(f"  + Company: {name} (id={node_ids[name]})")

for i, name in enumerate(["New York", "London", "Tokyo", "Berlin"]):
    session.query(f"CREATE (c:City {{name: '{name}'}})")
    node_ids[name] = len(people) + 3 + i
    print(f"  + City: {name} (id={node_ids[name]})")

r = session.query("MATCH (n) RETURN count(n)")
print(f"\n  Total nodes: {r['rows'][0][0]}")

# ─── 2. Create relationships via native API ───
print("\n### Phase 2: Create Relationships ###")

knows = [
    ("Alice", "Bob"), ("Alice", "Charlie"), ("Bob", "Diana"),
    ("Charlie", "Eve"), ("Diana", "Frank"), ("Eve", "Grace"),
    ("Frank", "Hank"), ("Grace", "Alice"), ("Hank", "Bob"),
    ("Alice", "Eve"), ("Charlie", "Frank"),
]
for a, b in knows:
    session.create_rel(node_ids[a], node_ids[b], "KNOWS")
print(f"  Created {len(knows)} KNOWS relationships")

works_at = [
    ("Alice", "Acme Corp"), ("Bob", "Acme Corp"), ("Charlie", "Globex Inc"),
    ("Diana", "Globex Inc"), ("Eve", "Initech"), ("Frank", "Acme Corp"),
    ("Grace", "Initech"), ("Hank", "Globex Inc"),
]
for person, company in works_at:
    session.create_rel(node_ids[person], node_ids[company], "WORKS_AT")
print(f"  Created {len(works_at)} WORKS_AT relationships")

lives_in = [
    ("Alice", "New York"), ("Bob", "New York"), ("Charlie", "London"),
    ("Diana", "Tokyo"), ("Eve", "Berlin"), ("Frank", "London"),
    ("Grace", "Tokyo"), ("Hank", "Berlin"),
]
for person, city in lives_in:
    session.create_rel(node_ids[person], node_ids[city], "LIVES_IN")
print(f"  Created {len(lives_in)} LIVES_IN relationships")

r = session.query("MATCH ()-[r]->() RETURN count(r)")
print(f"\n  Total relationships: {r['rows'][0][0]}")

session.close()

# ─── 3. Query in a NEW session (proves persistence across close/reopen) ───
print("\n### Phase 3: Cypher Queries (new session = persistence proof) ###")

session = GraphSession(DB)

print_table(
    "All people (sorted by age)",
    session.query("MATCH (p:Person) RETURN p.name, p.age, p.department ORDER BY p.age"),
)

print_table(
    "Alice's direct friends",
    session.query("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name, b.age"),
)

print_table(
    "Friends-of-friends of Alice (2-hop)",
    session.query(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) "
        "WHERE a <> c RETURN DISTINCT c.name"
    ),
)

print_table(
    "Employees per company",
    session.query(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN c.name, count(p) ORDER BY count(p) DESC"
    ),
)

print_table(
    "People per city",
    session.query(
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) "
        "RETURN c.name, count(p) ORDER BY count(p) DESC"
    ),
)

print_table(
    "Department stats (count + avg age)",
    session.query("MATCH (p:Person) RETURN p.department, count(p), avg(p.age)"),
)

print_table(
    "People over 30",
    session.query("MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age ORDER BY p.age DESC"),
)

print_table(
    "Acme Corp employees",
    session.query(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: 'Acme Corp'}) "
        "RETURN p.name, p.department"
    ),
)

print_table(
    "London residents",
    session.query("MATCH (p:Person)-[:LIVES_IN]->(c:City {name: 'London'}) RETURN p.name"),
)

print_table(
    "Who does everyone know? (full social graph)",
    session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name"),
)

session.close()

# ─── Cleanup ───
for f in [DB, DB + "-wal"]:
    if os.path.exists(f):
        os.remove(f)

print("\n" + "=" * 60)
print("  All phases passed!")
print("=" * 60)
