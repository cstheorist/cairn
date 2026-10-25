"""Combined coordinator/participant start: atomic failure and replay fencing."""
import uuid
import tarantool
from replication import connect
from transaction_protocol import call

def identifier(home):
    value=bytearray(uuid.uuid4().bytes)
    value[:2]=home.to_bytes(2,'big')
    value[6]=(value[6]&15)|128
    return str(uuid.UUID(bytes=bytes(value)))

def rejects(function):
    try: function()
    except tarantool.DatabaseError: return
    raise AssertionError('operation unexpectedly succeeded')

def main():
    table='txstart_'+uuid.uuid4().hex[:8]
    with connect() as connection:
        connection.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY,value TEXT NOT NULL)')
        for key in (0,1): connection.execute(f"INSERT INTO {table} VALUES (%s,'before')",(key,))
        for shard in (0,1):
            tx=identifier(shard)
            token,result=call(shard,'tx_start',tx,'cairn.tx_read',[table,shard])
            assert result==[[shard,'before']],result
            record=call(shard,'tx_status',tx)[0]
            assert record[1]=='OPEN' and record[2]==[shard],record
            assert call(shard,'tx_read',tx,table,shard,token)==[[shard,'before']]
            call(shard,'tx_commit_local',tx)
            rejects(lambda:call(shard,'tx_start',tx,'cairn.tx_read',[table,shard]))
        owner=identifier(0)
        _,result=call(0,'tx_start',owner,'cairn.tx_mutate',[table,0,'update',[['=',2,'after']]])
        assert result==[True],result
        blocked=identifier(0)
        rejects(lambda:call(0,'tx_start',blocked,'cairn.tx_read',[table,0]))
        rejects(lambda:call(0,'tx_status',blocked))
        aborted=call(0,'tx_decide',blocked,'ABORT',[])[0]
        assert aborted[1:3]==['ABORT',[]],aborted
        rejects(lambda:call(0,'tx_start',blocked,'cairn.tx_read',[table,0]))
        call(0,'tx_complete',blocked,[])
        call(0,'tx_clean',blocked,[])
        call(0,'tx_commit_local',owner)
        assert connection.execute(f'SELECT value FROM {table} WHERE id=0').fetchone()==('after',)
        # A supplied home mismatch cannot create metadata on another shard.
        wrong=identifier(1)
        rejects(lambda:call(0,'tx_start',wrong,'cairn.tx_read',[table,0]))
        rejects(lambda:call(1,'tx_status',wrong))
    print('transaction start: both homes, first read/write, atomic conflict rollback, coordinator fencing and replay rejection passed',flush=True)

if __name__=='__main__': main()
