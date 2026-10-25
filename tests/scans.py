"""Native scan pagination, own-intent merging, and predicate protection."""
import uuid
import psycopg
from replication import connect
from transaction_protocol import begin, call, resolve

def scan(identifier, token, table, shard):
    after = None
    rows = []
    while True:
        result = call(shard, "tx_scan", identifier, table, after, 2, token)
        rows.extend(result[0])
        after = result[1]
        if after is None: return rows

def main():
    table = "scan_" + uuid.uuid4().hex[:8]
    with connect() as connection:
        connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        for key in range(10): connection.execute(f"INSERT INTO {table} VALUES (%s, %s)", (key, str(key)))
        identifier, tokens = begin()
        call(0, "tx_mutate", identifier, table, 2, "delete", None, tokens[0])
        call(0, "tx_mutate", identifier, table, 4, "update", [["=", 2, "changed"]], tokens[0])
        call(0, "tx_mutate", identifier, table, 20, "insert", [20, "new"], tokens[0])
        assert scan(identifier, tokens[0], table, 0) == [[0, "0"], [4, "changed"], [6, "6"], [8, "8"], [20, "new"]]
        observer, observer_tokens = begin()
        try:
            scan(observer, observer_tokens[0], table, 0)
        except Exception as error:
            assert "[CAIRN:CONFLICT]" in str(error), str(error)
        else:
            raise AssertionError("scan bypassed a writer that had upgraded from IX to scan/write")
        resolve(observer, "ABORT", [])
        assert scan(identifier, tokens[1], table, 1) == [[key, str(key)] for key in [1, 3, 5, 7, 9]]
        try: connection.execute(f"INSERT INTO {table} VALUES (22, 'phantom')")
        except psycopg.errors.SerializationFailure: pass
        else: raise AssertionError("a write bypassed the scan's predicate lock")
        prepared = [call(shard, "tx_prepare", identifier, tokens[shard])[0] for shard in range(2)]
        resolve(identifier, "COMMIT", prepared)
        connection.execute(f"INSERT INTO {table} VALUES (22, 'after release')")
        a, at = begin()
        b, bt = begin()
        assert scan(a, at[0], table, 0) == scan(b, bt[0], table, 0)
        try:
            call(0, "tx_mutate", a, table, 0, "update", [["=",2,"unsafe"]], at[0])
        except Exception as error:
            assert "[CAIRN:CONFLICT]" in str(error), str(error)
        else:
            raise AssertionError("scan-to-writer upgrade bypassed another shared scan")
        resolve(a, "ABORT", [])
        resolve(b, "ABORT", [])
    print("scans: pagination, own intents, phantoms, shared scans, both scan/write upgrades and release passed", flush=True)

if __name__ == "__main__": main()
