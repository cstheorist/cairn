"""Atomic local application/unlock after an irreversible global decision."""
import psycopg
from replication import connect
from transaction_protocol import call
from vote_protocol import start,finish,values,rejects

def prepared(connection,fast=True):
    tx,table,tokens=start(connection,fast=fast)
    receipts=[call(shard,'tx_prepare',tx,tokens[shard])[0] for shard in (0,1)]
    record=call(0,'tx_decide',tx,'COMMIT',receipts)[0]
    return tx,table,record

def main():
    with connect() as connection:
        tx,table,record=prepared(connection)
        assert record[12],record
        rejects(lambda:call(0,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[10]))
        first=call(0,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[12])[0]
        assert connection.execute(f'SELECT value FROM {table} WHERE id=0').fetchone()==('after',)
        with connection.cursor() as cursor:
            try: cursor.execute(f'SELECT * FROM {table} ORDER BY id')
            except psycopg.errors.SerializationFailure: pass
            else: raise AssertionError('distributed query read an unfinished participant')
        # The unfinished shard still rejects old-version reads.
        rejects(lambda:call(1,'get',table,1))
        # A later transaction can serialize after this global decision on the
        # finished shard, even before the first transaction's final response.
        connection.execute(f"UPDATE {table} SET value='later' WHERE id=0")
        assert call(0,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[12])[0]==first
        second=call(1,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[12])[0]
        call(0,'tx_clean',tx,[first,second])
        assert values(connection,table)==[(0,'later'),(1,'after')]
        assert call(0,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[12])[0]==first
        assert values(connection,table)==[(0,'later'),(1,'after')]

        # An existing barrier-mode vote scope cannot authorize local finish.
        tx,table,record=prepared(connection,fast=False)
        assert not record[12]
        rejects(lambda:call(0,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[10]))
        finish(0,record)

        # Abort/unlock can finish independently and preserves original tuples.
        tx,table,tokens=start(connection,fast=True)
        record=call(0,'tx_decide',tx,'ABORT',[])[0]
        released=[call(shard,'tx_finish_vote',tx,'ABORT',[0,1],record[6],record[12])[0] for shard in (0,1)]
        call(0,'tx_clean',tx,released)
        assert values(connection,table)==[(0,'before'),(1,'before')]

        # A participant already installed through the older primitive can
        # complete without replaying its row changes after releasing locks.
        tx,table,record=prepared(connection)
        call(0,'tx_apply',tx,'COMMIT',[0,1],record[6])
        released=[call(shard,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[12])[0] for shard in (0,1)]
        call(0,'tx_clean',tx,released)
        assert values(connection,table)==[(0,'after'),(1,'after')]
    print('local finish: atomic apply/unlock, blocked old versions, distributed query consistency, capability separation, abort and replay fencing passed',flush=True)

if __name__=='__main__': main()
