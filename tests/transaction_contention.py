"""Concurrent simple-query control with prepared mutations and rollback reuse."""
from concurrent.futures import ThreadPoolExecutor
import uuid
import psycopg
from psycopg import pq
from replication import connect

class ControlError(Exception):
    def __init__(self,result):
        self.sqlstate=(result.error_field(pq.DiagnosticField.SQLSTATE) or b'').decode()
        super().__init__(result.error_message.decode())

def control(c,sql):
    result=c.pgconn.exec_(sql.encode())
    if result.status!=pq.ExecStatus.COMMAND_OK: raise ControlError(result)

def main():
    table='contention_'+uuid.uuid4().hex[:9]
    workers,rounds=16,30
    with connect() as c:
        c.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY,value BIGINT NOT NULL)')
        for key in range(workers*2): c.execute(f'INSERT INTO {table} VALUES (%s,1000000)',(key,))
    def work(index):
        retries=0
        with connect() as c:
            for _ in range(rounds):
                for attempt in range(64):
                    control(c,'BEGIN')
                    try:
                        c.execute(f'UPDATE {table} SET value=value-1 WHERE id=%s',(index*2,))
                        c.execute(f'UPDATE {table} SET value=value+1 WHERE id=%s',(index*2+1,))
                        control(c,'COMMIT')
                    except (psycopg.Error,ControlError) as error:
                        # Never begin a new block without successful rollback.
                        control(c,'ROLLBACK')
                        assert c.pgconn.transaction_status==pq.TransactionStatus.IDLE
                        if error.sqlstate not in ('40001','40P01'): raise
                        retries+=1
                    else:
                        assert c.pgconn.transaction_status==pq.TransactionStatus.IDLE
                        break
                else: raise AssertionError('transaction retry limit exhausted')
        return retries
    with ThreadPoolExecutor(max_workers=workers) as pool: retries=sum(pool.map(work,range(workers)))
    with connect() as c:
        expected=[(key,1000000+(-rounds if key%2==0 else rounds)) for key in range(workers*2)]
        assert c.execute(f'SELECT * FROM {table} ORDER BY id').fetchall()==expected
    print(f'transaction contention: {workers*rounds} disjoint cross-shard transfers, per-key values, simple/prepared protocol reuse and idle-after-rollback passed; {retries} retries',flush=True)

if __name__=='__main__': main()
