"""Native contract checks for incarnation-bound, non-WAL OPEN state."""
from contextlib import closing
import uuid
import psycopg
from replication import connect,leader,native
from transaction_protocol import call
from vote_protocol import rejects

def operator(shard,source,*args):
    with closing(native(leader(shard)['node'],'operator')) as c:
        return c.eval(source,*args).data

def epoch(shard):
    with closing(native(leader(shard)['node'])) as c:
        return c.call('cairn.ping').data[7]

def identity(home=0,incarnation=None):
    raw=bytearray(uuid.uuid4().bytes)
    raw[:2]=home.to_bytes(2,'big')
    raw[6]=(raw[6]&15)|128
    return str(uuid.UUID(bytes=bytes(raw)))+'~'+format(epoch(home) if incarnation is None else incarnation,'016x')

def lsn(shard):
    return operator(shard,'return box.info.lsn')[0]

def start(connection,home=0,read=False):
    table='volatile_'+uuid.uuid4().hex[:9]
    connection.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY,value TEXT NOT NULL)')
    for key in (0,1): connection.execute(f"INSERT INTO {table} VALUES (%s,'before')",(key,))
    tx=identity(home)
    before=lsn(home)
    op='cairn.tx_read' if read else 'cairn.tx_mutate'
    args=[table,home] if read else [table,home,'update',[['=',2,'after']]]
    token,result=call(home,'tx_open_start',tx,op,args)
    assert lsn(home)==before,'OPEN first operation wrote WAL'
    assert result==([[home,'before']] if read else [True]),result
    return tx,table,{home:token}

def join(tx,table,tokens,shard,read=False):
    home=int(tx[:4],16)
    before=[lsn(p) for p in (0,1)]
    tokens[shard]=call(home,'tx_open_enlist',tx,shard,format(epoch(shard),'016x'))[0]
    if read: assert call(shard,'tx_read',tx,table,shard,tokens[shard])==[[shard,'before']]
    else: assert call(shard,'tx_mutate',tx,table,shard,'update',[['=',2,'after']],tokens[shard])==[True]
    assert [lsn(p) for p in (0,1)]==before,'OPEN enlist/mutation wrote WAL'

def freeze(tx):
    home=int(tx[:4],16)
    before=lsn(home)
    row=call(home,'tx_open_freeze',tx)[0]
    assert row[13] and row[9] and row[14] and row[15],row
    assert lsn(home)==before,'membership freeze wrote WAL'
    assert call(home,'tx_open_freeze',tx)[0][14:]==row[14:]
    return row

def prepare(tx,tokens,row,shard):
    return call(shard,'tx_open_prepare',tx,tokens[shard],row[14],row[15])[0]

def finish(tx,row,decision='COMMIT',proofs=()):
    home=int(tx[:4],16)
    row=call(home,'tx_decide',tx,decision,list(proofs))[0]
    assert row[1]==decision,row
    released=[call(p,'tx_finish_vote',tx,decision,row[2],row[6],row[12])[0] for p in row[2]]
    call(home,'tx_clean',tx,released)
    return row

def values(c,table):
    return c.execute(f'SELECT * FROM {table} ORDER BY id').fetchall()

def main():
    with connect() as c:
        for home in (0,1):
            tx,table,tokens=start(c,home)
            join(tx,table,tokens,1-home)
            rejects(lambda:call(home,'tx_open_enlist',tx,1-home,format(epoch(1-home)+1,'016x')))
            for shard in (0,1):
                with closing(native(leader(shard)['node'])) as direct:
                    assert direct.select(table,[shard]).data==[[shard,'before']]
                assert operator(shard,'local id=...; return box.space._cairn_parts:get{id}==nil,box.space._cairn_open_parts:get{id}~=nil',tx)==[True,True]
            row=freeze(tx)
            rejects(lambda:call(home,'tx_open_enlist',tx,1-home,format(epoch(1-home),'016x')))
            rejects(lambda:call(home,'tx_open_prepare',tx,tokens[home],row[14]+' ',row[15]))
            # The remote durable YES is discoverable without a coordinator.
            remote=1-home
            receipt=prepare(tx,tokens,row,remote)
            assert operator(home,'local id=...; return box.space._cairn_tx:get{id}==nil',tx)==[True]
            assert any(item[0]==tx for item in call(remote,'tx_open_pending')[0])
            waiting=call(home,'tx_open_recover',tx,row[14],row[15],False)[0]
            assert waiting[1]=='OPEN'
            home_receipt=prepare(tx,tokens,row,home)
            proofs=[home_receipt,receipt] if home==0 else [receipt,home_receipt]
            for shard in (0,1):
                assert operator(shard,"local id=...; return box.space._cairn_parts:get{id}[3],box.space._cairn_open_parts:get{id}==nil,box.space._cairn_locks.index.by_tx:count{id},box.space._cairn_intents.index.primary:count{id}",tx)==['YES',True,1,1]
            record=finish(tx,row,proofs=proofs)
            assert values(c,table)==[(0,'after'),(1,'after')]
            c.execute(f"UPDATE {table} SET value='later' WHERE id=0")
            call(0,'tx_finish_vote',tx,'COMMIT',record[2],record[6],record[12])
            assert values(c,table)==[(0,'later'),(1,'after')]

        # Unanimous proofs authorize remote finish directly. The home combines
        # its installation and global cleanup only after every remote finish.
        for home in (0,1):
            tx,table,tokens=start(c,home)
            remote=1-home
            join(tx,table,tokens,remote)
            row=freeze(tx)
            proofs=[prepare(tx,tokens,row,p) for p in (0,1)]
            args=[tx,row[14],row[15],proofs]
            rejects(lambda:call(remote,'tx_open_finish',tx,row[14],row[15],proofs[:1]))
            rejects(lambda:call(home,'tx_open_finish_last',*args,[]))
            released=call(remote,'tx_open_finish',*args)[0]
            rejects(lambda:call(home,'get',table,home))
            rejects(lambda:call(home,'tx_open_finish_last',*args,['forged']))
            assert call(home,'tx_status',tx)[0][1]=='OPEN'
            result=call(home,'tx_open_finish_last',*args,[released])[0]
            assert result[1]=='COMMIT' and result[4] and result[5],result
            assert values(c,table)==[(0,'after'),(1,'after')]
            c.execute(f"UPDATE {table} SET value='later' WHERE id=%s",(home,))
            assert call(home,'tx_open_finish_last',*args,[released])[0]==result
            call(remote,'tx_open_finish',*args)
            assert values(c,table)==[(0,'later' if home==0 else 'after'),(1,'later' if home==1 else 'after')]

        # A lost participant cannot re-create an empty transaction and vote YES.
        tx,table,tokens=start(c)
        join(tx,table,tokens,1)
        row=freeze(tx)
        operator(1,'''
local id=...
for _,r in ipairs(box.space._cairn_open_locks.index.by_tx:select{id}) do box.space._cairn_open_locks:delete{r[1],r[2],id} end
for _,r in ipairs(box.space._cairn_open_table_locks.index.by_tx:select{id}) do box.space._cairn_open_table_locks:delete{r[1],id} end
for _,r in ipairs(box.space._cairn_open_intents:select{id}) do box.space._cairn_open_intents:delete{id,r[2],r[3]} end
box.space._cairn_open_parts:delete{id}; return box.space._cairn_open_used:get{id}~=nil
''',tx)
        rejects(lambda:call(1,'tx_read',tx,table,1,tokens[1]))
        rejects(lambda:prepare(tx,tokens,row,1))
        assert call(1,'tx_vote_status',tx,row[2],row[10])[0][0]=='NO'
        call(0,'tx_open_recover',tx,row[14],row[15],True)
        finish(tx,row,'ABORT')
        assert values(c,table)==[(0,'before'),(1,'before')]

        # Unfrozen rollback needs no WAL but permanently closes this identity.
        tx,table,tokens=start(c)
        join(tx,table,tokens,1)
        before=[lsn(p) for p in (0,1)]
        for shard in (0,1): call(shard,'tx_open_abort',tx,tokens[shard],False)
        assert [lsn(p) for p in (0,1)]==before,'unprepared abort wrote WAL'
        rejects(lambda:call(0,'tx_open_start',tx,'cairn.tx_read',[table,0]))
        rejects(lambda:call(1,'tx_read',tx,table,1,tokens[1]))
        rejects(lambda:call(0,'tx_open_freeze',tx))
        assert values(c,table)==[(0,'before'),(1,'before')]

        # One participant installs user tuples and the durable outcome together.
        tx,table,tokens=start(c)
        record=call(0,'tx_open_local',tx)[0]
        assert record[1]=='COMMIT' and record[4] and record[5]
        assert call(0,'tx_open_local',tx)[0]==record
        rejects(lambda:call(0,'tx_mutate',tx,table,0,'delete',None,tokens[0]))
        assert values(c,table)==[(0,'after'),(1,'before')]
        assert operator(0,'local id=...; return box.space._cairn_open_parts:get{id}==nil,box.space._cairn_open_used:get{id}==nil',tx)==[True,True]

        # Retain table scan and row read claims when copying into durable state.
        tx,table,tokens=start(c,read=True)
        assert call(0,'tx_scan',tx,table,None,1024,tokens[0])[0]==[[0,'before']]
        join(tx,table,tokens,1,read=True)
        row=freeze(tx)
        receipts=[prepare(tx,tokens,row,p) for p in (0,1)]
        assert operator(0,'local id,name=...; return box.space._cairn_table_locks:get{name,id}[3],box.space._cairn_locks:get{name,0,id}[4]',tx,table)==['R','R']
        try: c.execute(f"UPDATE {table} SET value='conflict' WHERE id=0")
        except psycopg.errors.SerializationFailure: pass
        else: raise AssertionError('prepared scan/read claim was dropped')
        finish(tx,row,proofs=receipts)
        assert values(c,table)==[(0,'before'),(1,'before')]

        stale=identity(0,epoch(0)-1)
        rejects(lambda:call(0,'tx_open_start',stale,'cairn.tx_read',[table,0]))
    print('volatile OPEN: no-WAL staging/rollback/freeze, immutable epochs, authenticated scope, orphan YES discovery, lost-state NO, complete claim copying, local durable commit and replay fencing passed',flush=True)

if __name__=='__main__': main()
