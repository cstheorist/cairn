"""Durable vote certificates: unanimity, veto, frozen membership and replay."""
import uuid
import tarantool
from replication import connect
from transaction_protocol import call
from transaction_start import identifier

def rejects(operation):
    try: operation()
    except tarantool.DatabaseError: return
    raise AssertionError('unsafe protocol operation succeeded')

def start(connection,home=0,both=True,fast=False):
    table='votes_'+uuid.uuid4().hex[:8]
    connection.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY,value TEXT NOT NULL)')
    connection.execute(f"INSERT INTO {table} VALUES (0,'before'),(1,'before')")
    tx=identifier(home)
    token,_=call(home,'tx_start_finished' if fast else 'tx_start_voted',tx,'cairn.tx_mutate',[table,home,'update',[['=',2,'after']]])
    tokens={home:token}
    if both:
        peer=1-home
        tokens[peer]=call(home,'tx_enlist',tx,peer)[0]
        call(peer,'tx_mutate',tx,table,peer,'update',[['=',2,'after']],tokens[peer])
    return tx,table,tokens

def finish(home,record):
    tx,kind,members,token=record[0],record[1],record[2],record[6]
    applied=[call(shard,'tx_apply',tx,kind,members,token)[0] for shard in members]
    complete=call(home,'tx_complete',tx,applied)[0]
    released=[call(shard,'tx_release',tx,kind,members,complete[7])[0] for shard in members]
    call(home,'tx_clean',tx,released)
    return call(home,'tx_status',tx)[0]

def values(connection,table):
    return connection.execute(f'SELECT * FROM {table} ORDER BY id').fetchall()

def main():
    with connect() as connection:
        # A remote YES cannot force commit while the coordinator can still veto.
        tx,table,tokens=start(connection)
        remote_yes=call(1,'tx_prepare',tx,tokens[1])[0]
        aborted=call(0,'tx_decide',tx,'ABORT',[])[0]
        assert aborted[1]=='ABORT',aborted
        rejects(lambda:call(0,'tx_prepare',tx,tokens[0]))
        finish(0,aborted)
        assert call(1,'tx_vote_status',tx,[0,1],aborted[10])[0]==['YES',remote_yes]
        assert values(connection,table)==[(0,'before'),(1,'before')]

        # A participant aborted before voting has no YES/NO receipt. Querying
        # that abstention after cleanup must retain the completed tombstone.
        tx,table,tokens=start(connection)
        aborted=call(0,'tx_decide',tx,'ABORT',[])[0]
        finish(0,aborted)
        assert call(1,'tx_vote_status',tx,[0,1],aborted[10])[0]==['OPEN','']
        assert call(1,'tx_vote_abort',tx,[0,1],aborted[10])[0]==['OPEN','']
        assert values(connection,table)==[(0,'before'),(1,'before')]

        # Membership freezes with the home YES, even for a sole participant.
        tx,table,tokens=start(connection,both=False)
        yes=call(0,'tx_prepare',tx,tokens[0])[0]
        frozen=call(0,'tx_status',tx)[0]
        assert frozen[8] and frozen[9] and frozen[10],frozen
        rejects(lambda:call(0,'tx_enlist',tx,1))
        assert call(0,'tx_decide',tx,'ABORT',[])[0][1]=='OPEN'
        assert call(0,'tx_vote_abort',tx,[0],frozen[10])[0]==['YES',yes]
        committed=call(0,'tx_decide',tx,'COMMIT',[yes])[0]
        assert committed[1]=='COMMIT' and call(0,'tx_status',tx)[0][1]=='OPEN'
        finish(0,committed)
        assert values(connection,table)==[(0,'after'),(1,'before')]

        # Resolving a lost local-commit response must stay idempotent even
        # though this atomic fast path needed no frozen vote scope.
        tx,table,tokens=start(connection,both=False)
        committed=call(0,'tx_commit_local',tx)[0]
        assert committed[1]=='COMMIT' and committed[4] and committed[5] and not committed[9]
        connection.execute(f"UPDATE {table} SET value='later' WHERE id=0")
        finish(0,call(0,'tx_decide',tx,'ABORT',[])[0])
        assert values(connection,table)==[(0,'later'),(1,'before')]

        # A real NO on the remaining open participant makes abort irreversible.
        tx,table,tokens=start(connection)
        home_yes=call(0,'tx_prepare',tx,tokens[0])[0]
        frozen=call(0,'tx_status',tx)[0]
        rejects(lambda:call(1,'tx_vote_abort',tx,[0,1],'forged'))
        no=call(1,'tx_vote_abort',tx,[0,1],frozen[10])[0]
        assert no[0]=='NO',no
        rejects(lambda:call(1,'tx_prepare',tx,tokens[1]))
        rejects(lambda:call(0,'tx_decide',tx,'COMMIT',[home_yes,no[1]]))
        aborted=call(0,'tx_decide',tx,'ABORT',[1,no[1]])[0]
        finish(0,aborted)
        assert call(0,'tx_vote_status',tx,[0,1],frozen[10])[0]==['YES',home_yes]
        assert call(1,'tx_vote_status',tx,[0,1],frozen[10])[0]==no
        assert values(connection,table)==[(0,'before'),(1,'before')]

        for home in (0,1):
            tx,table,tokens=start(connection,home)
            prepared=[call(shard,'tx_prepare',tx,tokens[shard])[0] for shard in (0,1)]
            frozen=call(home,'tx_status',tx)[0]
            assert call(home,'tx_decide',tx,'ABORT',[])[0][1]=='OPEN'
            for shard in (0,1):
                assert call(shard,'tx_vote_abort',tx,[0,1],frozen[10])[0]==['YES',prepared[shard]]
                assert call(shard,'tx_prepare',tx,tokens[shard])[0]==prepared[shard]
            committed=call(home,'tx_decide',tx,'COMMIT',prepared)[0]
            first=call(0,'tx_apply',tx,'COMMIT',[0,1],committed[6])[0]
            rejects(lambda:call(0,'tx_release',tx,'COMMIT',[0,1],committed[6]))
            rejects(lambda:call(home,'tx_complete',tx,[first,'forged']))
            second=call(1,'tx_apply',tx,'COMMIT',[0,1],committed[6])[0]
            complete=call(home,'tx_complete',tx,[first,second])[0]
            assert complete[1]=='COMMIT' and complete[4]
            stored=call(home,'tx_status',tx)[0]
            assert stored[1]=='OPEN' and not stored[4]
            released=[call(shard,'tx_release',tx,'COMMIT',[0,1],complete[7])[0] for shard in (0,1)]
            call(home,'tx_clean',tx,released)
            stored=call(home,'tx_status',tx)[0]
            assert stored[1]=='COMMIT' and stored[4] and stored[5]
            assert values(connection,table)==[(0,'after'),(1,'after')]
            connection.execute(f"UPDATE {table} SET value='later' WHERE id=0")
            call(0,'tx_apply',tx,'COMMIT',[0,1],committed[6])
            assert values(connection,table)==[(0,'later'),(1,'after')]
    print('vote commit: both homes, frozen membership, immutable YES/NO, forged proof rejection, complete-before-release and replay fencing passed',flush=True)

if __name__=='__main__': main()
