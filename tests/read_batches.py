"""Concurrent response association, acknowledged-write freshness and large rows."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import threading
import time
import uuid
import psycopg
from replication import connect,leader,native

def read(connection,query,parameters):
    deadline=time.monotonic()+5
    while True:
        try: return connection.execute(query,parameters).fetchone()
        except psycopg.errors.SerializationFailure:
            if time.monotonic()>=deadline: raise
            time.sleep(.001)

def main():
    suffix=uuid.uuid4().hex[:8]
    tables=["batch_a_"+suffix,"batch_b_"+suffix]
    keys=[-(2**63),-2,-1,0,1,2,2**63-1]
    large="batch_large_"+suffix
    with connect() as connection:
        for table in tables:
            connection.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY,value TEXT NOT NULL,version BIGINT NOT NULL)")
            for key in keys:
                connection.execute(f"INSERT INTO {table} VALUES (%s,%s,0)",(key,f"{table}:{key}"))
        connection.execute(f"CREATE TABLE {large} (id BIGINT PRIMARY KEY,value TEXT NOT NULL)")
        payload="x"*(192*1024)
        connection.execute(f"INSERT INTO {large} VALUES (0,%s)",(payload,))
    barrier=threading.Barrier(16)
    def worker(index):
        with connect() as connection:
            barrier.wait()
            for step in range(100):
                table=tables[(index+step)%2]
                key=keys[(index*3+step)%len(keys)]
                assert read(connection,f"SELECT * FROM {table} WHERE id=%s",(key,))==(key,f"{table}:{key}",0)
    with ThreadPoolExecutor(max_workers=16) as workers:
        list(workers.map(worker,range(16)))
    # Every read starts after an acknowledged update, while other reads compete
    # for the same actor and storage barrier.
    with ThreadPoolExecutor(max_workers=8) as workers,connect() as writer:
        def after_ack(version):
            with connect() as connection:
                row=read(connection,f"SELECT version FROM {tables[0]} WHERE id=0",None)
                assert row[0]>=version,(version,row)
        for version in range(1,33):
            writer.execute(f"UPDATE {tables[0]} SET version=%s WHERE id=0",(version,))
            list(workers.map(after_ack,[version]*8))
    # A combined reply can exceed the iproto budget even when each read fits;
    # the gateway must split that batch and preserve each caller's result.
    large_barrier=threading.Barrier(64)
    def large_read(_):
        with connect() as connection:
            large_barrier.wait()
            assert read(connection,f"SELECT value FROM {large} WHERE id=0",None)==(payload,)
    with ThreadPoolExecutor(max_workers=64) as workers:
        list(workers.map(large_read,range(64)))
    info=leader(0)
    with closing(native(info["node"])) as connection:
        requests=[[tables[0],0],[tables[1],2],[tables[0],10]]
        result=connection.call("cairn.get_many",requests,info["term"]).data[0]
        assert result==[[tables[0],0,[0,f"{tables[0]}:0",32]],[tables[1],2,[2,f"{tables[1]}:2",0]],[tables[0],10,[]]],result
    print("read batches: concurrent tables/keys, signed boundaries, read-after-ack freshness, large reply splitting and native access passed",flush=True)

if __name__=="__main__": main()
