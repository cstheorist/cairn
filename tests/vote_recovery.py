"""Recover immutable vote outcomes after coordinator failover, before timeout."""
import json
from pathlib import Path
import sys
import time
import psycopg
from contextlib import closing
from replication import connect,leader,native
from transaction_protocol import call
from vote_protocol import start,values,rejects

STATE=Path('/artifacts/vote-recovery.json')

def main(mode,fast=False):
    if mode in ['prepare_yes','prepare_no','prepare_open']:
        with connect() as connection:
            tx,table,tokens=start(connection,fast=fast)
            yes=call(0,'tx_prepare',tx,tokens[0])[0]
            frozen=call(0,'tx_status',tx)[0]
            if mode=='prepare_yes':
                other=call(1,'tx_prepare',tx,tokens[1])[0]
            elif mode=='prepare_no':
                other=call(1,'tx_vote_abort',tx,[0,1],frozen[10])[0][1]
            else:
                other=''
            assert call(0,'tx_status',tx)[0][1]=='OPEN'
            if fast and mode=='prepare_yes':
                committed=call(0,'tx_decide',tx,'COMMIT',[yes,other])[0]
                call(0,'tx_finish_vote',tx,'COMMIT',[0,1],committed[6],committed[12])
            STATE.write_text(json.dumps({'id':tx,'table':table,'yes':yes,'other':other,'scope':frozen[10],
                'expected':'COMMIT' if mode=='prepare_yes' else 'ABORT','coordinator':leader(0)['node']}))
        print(f'{mode}: durable participant votes; no materialized coordinator decision',flush=True)
        return
    state=json.loads(STATE.read_text())
    expected=state['expected']
    if mode=='check_incomplete':
        deadline=time.monotonic()+2
        while time.monotonic()<deadline:
            assert call(0,'tx_status',state['id'])[0][1]=='OPEN'
            assert call(1,'tx_vote_status',state['id'],[0,1],state['scope'])[0]==['OPEN','']
            time.sleep(.1)
        # Advance only this test transaction's coordinator deadline; do not
        # change cluster clocks or wait for the full development timeout.
        with closing(native(leader(0)['node'],'operator')) as admin:
            admin.eval("local id=...; return box.atomic(function() return box.space._cairn_tx:update({id},{{'=',4,0}}) end)",state['id'])
        print('incomplete votes: recovery waited without aborting a live transaction; test deadline advanced',flush=True)
        return
    rows=[(0,'after'),(1,'after')] if expected=='COMMIT' else [(0,'before'),(1,'before')]
    with connect() as connection:
        deadline=time.monotonic()+15
        while True:
            try:
                actual=values(connection,state['table'])
                if actual==rows: break
            except psycopg.Error as error:
                assert error.sqlstate in ['40001','08006','57P03','XX000'],error
            if time.monotonic()>deadline: raise AssertionError(f'vote recovery did not produce {expected} before the OPEN deadline')
            time.sleep(.1)
        record=call(0,'tx_status',state['id'])[0]
        assert record[1]==expected and record[4] and record[5],record
        assert call(0,'tx_vote_status',state['id'],[0,1],state['scope'])[0]==['YES',state['yes']]
        if expected=='COMMIT':
            assert call(0,'tx_decide',state['id'],'ABORT',[])[0][1]=='COMMIT'
            connection.execute(f"UPDATE {state['table']} SET value='later' WHERE id=0")
            call(0,'tx_apply',state['id'],'COMMIT',[0,1],record[6])
            assert values(connection,state['table'])==[(0,'later'),(1,'after')]
        else:
            rejects(lambda:call(1,'tx_prepare',state['id'],'stale'))
            vote=call(1,'tx_vote_status',state['id'],[0,1],state['scope'])[0]
            assert vote[0]=='NO' and (not state['other'] or vote[1]==state['other']),vote
    print(f'vote recovery: coordinator failover inferred {expected}, applied all shards, released locks and retained votes before expiry',flush=True)

if __name__=='__main__': main(sys.argv[1],len(sys.argv)>2 and sys.argv[2]=='fast')
