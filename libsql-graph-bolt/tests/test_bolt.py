from neo4j import GraphDatabase
import sys

def main():
    import os
port = os.environ.get("BOLT_PORT", "7687")
driver = GraphDatabase.driver(f"bolt://localhost:{port}", auth=("neo4j", "test"))

    try:
        with driver.session() as s:
            s.run("CREATE (n:Person {name: 'Alice', age: 30})").consume()
            print("OK: Created Alice")

            s.run("CREATE (n:Person {name: 'Bob', age: 25})").consume()
            print("OK: Created Bob")

            result = s.run("MATCH (n:Person) RETURN n.name, n.age")
            records = list(result)
            print(f"OK: Found {len(records)} people")
            for r in records:
                print(f"  {r['n.name']} age {r['n.age']}")
            assert len(records) == 2, f"Expected 2 records, got {len(records)}"

        with driver.session() as s:
            result = s.run("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
            records = list(result)
            assert len(records) == 1
            assert records[0]["n.name"] == "Alice"
            print("OK: Queried Alice specifically")

        with driver.session() as s:
            tx = s.begin_transaction()
            tx.run("CREATE (n:Person {name: 'Charlie', age: 35})")
            tx.rollback()
            print("OK: Rolled back Charlie")

            result = s.run("MATCH (n:Person) RETURN n.name")
            records = list(result)
            assert len(records) == 2, f"Expected 2 after rollback, got {len(records)}"
            print("OK: Rollback verified - still 2 people")

        with driver.session() as s:
            tx = s.begin_transaction()
            tx.run("CREATE (n:Person {name: 'Diana', age: 28})")
            tx.commit()
            print("OK: Committed Diana")

            result = s.run("MATCH (n:Person) RETURN n.name")
            records = list(result)
            assert len(records) == 3, f"Expected 3 after commit, got {len(records)}"
            print("OK: Commit verified - 3 people")

        print("\nAll tests passed!")
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
