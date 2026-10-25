"""Single-shard atomic commit, replay fencing, and multi-shard guard checks."""
from contextlib import closing
import uuid
import tarantool
from replication import connect, leader, native
from transaction_protocol import begin, call, resolve

def main():
    table = "localcommit_" + uuid.uuid4().hex[:8]
    with connect() as connection:
        connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        for key in (0,1,2): connection.execute(f"INSERT INTO {table} VALUES (%s,'before')", (key,))
        identifier = str(uuid.uuid4())
        call(0,"tx_begin",identifier)
        token = call(0,"tx_enlist",identifier,0)[0]
        for key in (0,2): call(0,"tx_mutate",identifier,table,key,"update",[["=",2,"local"]],token)
        with closing(native(leader(0)["node"])) as direct:
            assert direct.select(table,[0]).data == [[0,"before"]]
            assert direct.select(table,[2]).data == [[2,"before"]]
        record = call(0,"tx_commit_local",identifier)[0]
        assert record[1:3] == ["COMMIT",[0]] and record[4] and record[5], record
        assert call(0,"tx_commit_local",identifier)[0] == record
        for key in (0,2):
            assert connection.execute(f"SELECT * FROM {table} WHERE id=%s",(key,)).fetchone() == (key,"local")
        connection.execute(f"UPDATE {table} SET value='later' WHERE id=0")
        call(0,"tx_commit_local",identifier)
        call(0,"tx_apply",identifier,"COMMIT",[0],record[6])
        assert connection.execute(f"SELECT value FROM {table} WHERE id=0").fetchone() == ("later",)
        identifier,tokens = begin()
        for shard in range(2): call(shard,"tx_mutate",identifier,table,shard,"delete",None,tokens[shard])
        try: call(0,"tx_commit_local",identifier)
        except tarantool.DatabaseError: pass
        else: raise AssertionError("local commit accepted two participants")
        resolve(identifier,"ABORT",[])
        assert connection.execute(f"SELECT * FROM {table} WHERE id=1").fetchone() == (1,"before")
    print("local commit: atomic rows/decision/cleanup, idempotence, replay fencing and multi-shard rejection passed",flush=True)

if __name__ == "__main__": main()
