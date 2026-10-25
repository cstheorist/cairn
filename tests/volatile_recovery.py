"""Fault injection contracts for volatile OPEN, orphan YES and local commit."""
import json
from pathlib import Path
import sys
import time
import psycopg
from replication import connect,leader
from transaction_protocol import call
from vote_protocol import rejects
from volatile_open import start,join,freeze,prepare,finish,values,epoch,operator

STATE=Path('/artifacts/volatile-recovery.json')

def wait_rows(c,table,expected):
    deadline=time.monotonic()+15
    while True:
        try:
            if values(c,table)==expected: return
        except psycopg.Error as error:
            assert error.sqlstate in ('40001','08006','57P03','XX000'),error
        if time.monotonic()>deadline: raise AssertionError(('volatile recovery timeout',table,expected))
        time.sleep(.1)

def main(mode,case):
    if mode=='seed':
        with connect() as c:
            tx,table,tokens=start(c)
            if case!='local': join(tx,table,tokens,1)
            row=None
            if case in ('orphan','wait','partial','partial_remote','no'):
                if case=='no': call(1,'tx_open_abort',tx,tokens[1],False)
                row=freeze(tx)
                if case=='no': rejects(lambda:prepare(tx,tokens,row,1))
                else: remote=prepare(tx,tokens,row,1)
                if case in ('partial','partial_remote'):
                    home=prepare(tx,tokens,row,0)
                    if case=='partial_remote':
                        call(1,'tx_open_finish',tx,row[14],row[15],[home,remote])
                    else:
                        decided=call(0,'tx_decide',tx,'COMMIT',[home,remote])[0]
                        call(0,'tx_finish_vote',tx,'COMMIT',[0,1],decided[6],decided[12])
            elif case=='local':
                row=call(0,'tx_open_local',tx)[0]
                assert row[1]=='COMMIT' and row[5]
            killed_shard=1 if case=='remote' else 0
            STATE.write_text(json.dumps({'id':tx,'table':table,'tokens':tokens,'scope':row,
                'dead':leader(killed_shard)['node'],'shard':killed_shard,'epoch':epoch(killed_shard)}))
        print(f'{case}: acknowledged operations staged; fault boundary recorded',flush=True)
        return
    state=json.loads(STATE.read_text())
    tx,table,row=state['id'],state['table'],state['scope']
    tokens={int(key):value for key,value in state['tokens'].items()}
    if case not in ('wait','no'): assert epoch(state['shard'])>state['epoch'],'new leader reused the old temporary incarnation'
    with connect() as c:
        if case=='home':
            rejects(lambda:call(0,'tx_read',tx,table,0,tokens[0]))
            rejects(lambda:call(0,'tx_open_start',tx,'cairn.tx_read',[table,0]))
            rejects(lambda:call(0,'tx_open_freeze',tx))
            for shard in (0,1): call(shard,'tx_open_abort',tx,tokens[shard],False)
            wait_rows(c,table,[(0,'before'),(1,'before')])
        elif case=='remote':
            rejects(lambda:call(1,'tx_read',tx,table,1,tokens[1]))
            rejects(lambda:call(0,'tx_open_enlist',tx,1,format(epoch(1),'016x')))
            row=freeze(tx)
            prepare(tx,tokens,row,0)
            rejects(lambda:prepare(tx,tokens,row,1))
            wait_rows(c,table,[(0,'before'),(1,'before')])
        elif case=='wait':
            time.sleep(1)
            record=call(0,'tx_open_status',tx)[0]
            assert record[1]=='OPEN' and record[9],record
            assert operator(0,'local id=...; return box.space._cairn_tx:get{id}==nil',tx)==[True]
            assert call(0,'tx_vote_status',tx,[0,1],row[10])[0]==['OPEN','']
            prepare(tx,tokens,row,0)
            wait_rows(c,table,[(0,'after'),(1,'after')])
        elif case in ('orphan','no'):
            wait_rows(c,table,[(0,'before'),(1,'before')])
        elif case in ('partial','partial_remote'):
            wait_rows(c,table,[(0,'after'),(1,'after')])
        elif case=='local':
            wait_rows(c,table,[(0,'after'),(1,'before')])
            assert call(0,'tx_open_local',tx)[0][1]=='COMMIT'
        if case in ('remote','orphan','partial','partial_remote','wait','no'):
            deadline=time.monotonic()+10
            while True:
                record=call(0,'tx_status',tx)[0]
                if record[5]: break
                assert time.monotonic()<deadline,record
                time.sleep(.1)
            expected='COMMIT' if case in ('partial','partial_remote','wait') else 'ABORT'
            assert record[1]==expected and record[4],record
            # Losing an unprepared image never overwrites a durable vote.
            assert call(1,'tx_vote_status',tx,[0,1],row[10])[0][0]==('NO' if case in ('remote','no') else 'YES')
            if expected=='COMMIT':
                c.execute(f"UPDATE {table} SET value='later' WHERE id=0")
                call(0,'tx_finish_vote',tx,'COMMIT',[0,1],record[6],record[12])
                wait_rows(c,table,[(0,'later'),(1,'after')])
        if case=='local':
            c.execute(f"UPDATE {table} SET value='later' WHERE id=0")
            call(0,'tx_open_local',tx)
            wait_rows(c,table,[(0,'later'),(1,'before')])
    print(f'{case}: failover/wait recovery preserved the outcome, fenced lost state and rejected stale replay',flush=True)

if __name__=='__main__': main(sys.argv[1],sys.argv[2])
