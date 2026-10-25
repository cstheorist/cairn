-- Optional volatile OPEN state. No client commit is acknowledged until the
-- complete read/write claims are either durably prepared or installed locally.
-- Incarnation-bound identities and retained used markers fence missing state.
return function(h)
    local ctx, api, ffi, json = h.ctx, h.api, require('ffi'), require('json')
    local out = {}
    out.functions = {'tx_open_start', 'tx_open_enlist', 'tx_open_freeze',
        'tx_open_prepare', 'tx_open_recover', 'tx_open_pending', 'tx_open_abort', 'tx_open_local', 'tx_open_status',
        'tx_open_finish', 'tx_open_finish_last'}
    local names = {'_cairn_open_tx', '_cairn_open_parts', '_cairn_open_intents',
        '_cairn_open_locks', '_cairn_open_table_locks', '_cairn_open_used'}

    local function hex(value)
        value = ffi.cast('uint64_t', value)
        return string.format('%08x%08x', tonumber(value / 4294967296ULL), tonumber(value % 4294967296ULL))
    end
    local function unsigned(value)
        assert(type(value) == 'string' and #value == 16 and value:match('^[0-9a-f]+$'), 'invalid incarnation')
        return ffi.new('uint64_t', tonumber(value:sub(1, 8), 16)) * 4294967296ULL
             + ffi.new('uint64_t', tonumber(value:sub(9), 16))
    end
    local function open_id(id)
        local home = h.home(id)
        assert(#id == 53 and id:sub(15, 15) == '8', 'incarnation-scoped UUIDv8 required')
        return home
    end
    local function same_incarnation(present, stored, requested)
        return h.verified.cairn_may_use_open_state(present and 1 or 0,
            ctx.incarnation(), stored, requested) == 1
    end
    local function require_home(id, term)
        ctx.fence(term)
        assert(open_id(id) == ctx.id, 'wrong transaction coordinator')
    end
    local function token(id, epoch, deadline)
        local stage = 'OPEN_ENLIST_' .. epoch .. '_' .. tostring(deadline)
        return epoch .. ':' .. tostring(deadline) .. ':' .. h.certificate(id, stage, {ctx.id})
    end
    local function decode_token(id, member, value)
        assert(type(value) == 'string', 'invalid open enlistment')
        local epoch, raw_deadline, mac = value:match('^([0-9a-f]+):(%d+):(.+)$')
        local incarnation = unsigned(epoch)
        local deadline = tonumber(raw_deadline)
        assert(deadline and deadline % 1 == 0 and deadline > 0, 'invalid open deadline')
        h.validate(id, 'OPEN_ENLIST_' .. epoch .. '_' .. raw_deadline, {member}, mac)
        return incarnation, deadline, epoch
    end
    local function mark_used(id)
        if not box.space._cairn_open_used:get{id} then
            box.space._cairn_open_used:insert{id}
        end
    end
    function out.capacity(id)
        -- This admission check must precede box.atomic. Counting the whole
        -- mutable space inside a write transaction creates MVCC dependencies
        -- on unrelated identities, including transactions being rolled back.
        -- Already admitted identities remain usable at the capacity limit.
        assert(not box.is_in_txn(), 'identity admission must run before the transaction')
        if not box.space._cairn_open_used:get{id} and box.space._cairn_open_used:len() >= 1000000 then
            h.fail('LIMIT', 'open transaction identity capacity exhausted')
        end
    end
    local function clear_claims(id)
        local s = h.spaces(id)
        for _, lock in ipairs(s.locks.index.by_tx:select{id}) do s.locks:delete{lock[1], lock[2], id} end
        for _, lock in ipairs(s.tables.index.by_tx:select{id}) do s.tables:delete{lock[1], id} end
        for _, intent in ipairs(s.intents.index.primary:select{id}) do s.intents:delete{id, intent[2], intent[3]} end
        s.parts:delete{id}
        box.space._cairn_open_tx:delete{id}
    end
    function out.clear(id)
        clear_claims(id)
        -- Durable participant/coordinator tombstones now fence replay.
        box.space._cairn_open_used:delete{id}
    end
    function out.initialize()
        local definitions = {
            {'_cairn_open_tx', {{1,'string'}}}, {'_cairn_open_parts', {{1,'string'}}},
            {'_cairn_open_used', {{1,'string'}}},
            {'_cairn_open_intents', {{1,'string'},{2,'string'},{3,'integer'}}},
            {'_cairn_open_locks', {{1,'string'},{2,'integer'},{3,'string'}}},
            {'_cairn_open_table_locks', {{1,'string'},{2,'string'}}},
        }
        for _, definition in ipairs(definitions) do
            local s = box.schema.space.create(definition[1], {temporary = true, if_not_exists = true})
            assert(s.temporary, 'open transaction space must be temporary')
            s:create_index('primary', {parts = definition[2], if_not_exists = true})
        end
        box.space._cairn_open_locks:create_index('by_tx', {parts={{3,'string'},{1,'string'},{2,'integer'}},if_not_exists=true})
        box.space._cairn_open_locks:create_index('by_mode', {parts={{1,'string'},{2,'integer'},{4,'string'},{3,'string'}},if_not_exists=true})
        box.space._cairn_open_table_locks:create_index('by_tx', {parts={{2,'string'},{1,'string'}},if_not_exists=true})
        box.space._cairn_open_table_locks:create_index('by_mode', {parts={{1,'string'},{3,'string'},{2,'string'}},if_not_exists=true})
        box.space._cairn_open_parts:create_index('deadline', {parts={{4,'unsigned'}},unique=false,if_not_exists=true})
        box.space._cairn_parts:create_index('phase', {parts={{2,'string'}},unique=false,if_not_exists=true})
    end
    function out.reset()
        -- Temporary DML is local and has no WAL. TRUNCATE is schema DDL and
        -- would replicate: never use it to clear this incarnation's RAM.
        local widths = {1,1,3,3,2,1}
        for index, name in ipairs(names) do
            local space = box.space[name]
            while true do
                local rows = space:select({}, {limit=1024})
                if #rows == 0 then break end
                box.atomic(function()
                    for _, row in ipairs(rows) do
                        local key = {}
                        for field=1,widths[index] do table.insert(key,row[field]) end
                        space:delete(key)
                    end
                end)
            end
        end
    end
    function out.maintain()
        local expired = {}
        for _, part in box.space._cairn_open_parts.index.deadline:pairs(os.time(), {iterator='LE'}) do
            table.insert(expired, part[1])
            if #expired >= 128 then break end
        end
        for _, id in ipairs(expired) do
            box.atomic(function()
                local part = box.space._cairn_open_parts:get{id}
                if part and part[4] <= os.time() then clear_claims(id) end
            end)
        end
    end
    function out.decorate(result, row)
        if row[12] == true then
            result[14], result[15], result[16] = true, row[11], row[13]
        end
    end
    function out.start(id, deadline)
        assert(open_id(id) == ctx.id, 'first participant must host its coordinator')
        local requested = unsigned(id:sub(38))
        if not same_incarnation(true, requested, requested) then h.fail('ABORTED', 'home incarnation changed') end
        assert(not box.space._cairn_tx:get{id} and not box.space._cairn_parts:get{id}
            and not box.space._cairn_open_tx:get{id} and not box.space._cairn_open_used:get{id}, 'transaction identity was already used')
        box.space._cairn_open_tx:insert{id,'OPEN',{ctx.id},deadline,false,false,'VOTES',false,true,
            {{ctx.id,hex(ctx.incarnation())}},'',true,''}
    end
    function out.active(id, enlistment)
        open_id(id)
        local requested, deadline = decode_token(id, ctx.id, enlistment)
        if box.space._cairn_parts:get{id} then h.fail('ABORTED', 'participant already voted or completed') end
        local s = h.spaces(id)
        local part = s.parts:get{id}
        if not part then
            if box.space._cairn_open_used:get{id} then h.fail('ABORTED', 'open participant state was closed or lost') end
            if not same_incarnation(true, requested, requested) or deadline <= os.time() then
                h.fail('ABORTED', 'open participant incarnation changed or expired')
            end
            mark_used(id)
            part = s.parts:insert{id,'OPEN',requested,deadline}
        end
        if not same_incarnation(true, part[3], requested) or part[4] ~= deadline or deadline <= os.time() then
            h.fail('ABORTED', 'open participant state no longer matches its enlistment')
        end
        local root = box.space._cairn_open_tx:get{id}
        if root and root[8] then h.fail('ABORTED', 'transaction membership is frozen') end
    end
    function api.tx_open_start(id, operation, arguments, term)
        require_home(id, term)
        assert(operation == 'cairn.tx_read' or operation == 'cairn.tx_mutate' or operation == 'cairn.tx_scan', 'invalid first operation')
        assert(type(arguments) == 'table', 'invalid operation arguments')
        local deadline = os.time() + h.timeout
        local enlistment = token(id, hex(ctx.incarnation()), deadline)
        local parameters = {id}
        for _, value in ipairs(arguments) do table.insert(parameters, value) end
        table.insert(parameters, enlistment); table.insert(parameters, term)
        table.insert(parameters, {'VOLATILE', deadline})
        return enlistment, {api[operation:match('^cairn%.(.+)$')](unpack(parameters))}
    end
    function api.tx_open_enlist(id, member, epoch, term)
        require_home(id, term)
        unsigned(epoch)
        assert(type(member) == 'number' and member % 1 == 0 and member >= 0 and member < ctx.count, 'invalid participant')
        return box.atomic(function()
            local root = box.space._cairn_open_tx:get{id}
            if not root or root[8] or root[4] <= os.time() then h.fail('ABORTED', 'open coordinator unavailable, frozen or expired') end
            local members, bindings = root[3], root[10]
            local found = false
            for _, binding in ipairs(bindings) do
                if binding[1] == member then
                    if binding[2] ~= epoch then h.fail('ABORTED', 'participant incarnation cannot be rebound') end
                    found = true
                end
            end
            if not found then
                table.insert(members, member); table.sort(members)
                table.insert(bindings, {member,epoch})
                table.sort(bindings, function(a,b) return a[1] < b[1] end)
                box.space._cairn_open_tx:update({id}, {{'=',3,members},{'=',10,bindings}})
            end
            return epoch .. ':' .. tostring(root[4]) .. ':' ..
                h.certificate(id, 'OPEN_ENLIST_' .. epoch .. '_' .. tostring(root[4]), {member})
        end)
    end
    local function scope(id, blob, mac)
        assert(type(blob) == 'string' and #blob <= 131072, 'invalid frozen scope')
        local decoded = json.decode(blob)
        assert(type(decoded) == 'table' and #decoded == 4 and decoded[1] == id, 'scope identity mismatch')
        open_id(id)
        local deadline, members, bindings = decoded[2], decoded[3], decoded[4]
        h.validate(id, 'OPEN_SCOPE_' .. blob, members, mac)
        assert(type(deadline) == 'number' and deadline > 0 and deadline % 1 == 0, 'invalid scope deadline')
        assert(type(bindings) == 'table' and #bindings == #members and #members > 0, 'missing incarnation bindings')
        local own
        for index, member in ipairs(members) do
            local binding = bindings[index]
            assert(type(binding) == 'table' and #binding == 2 and binding[1] == member, 'invalid incarnation binding')
            unsigned(binding[2])
            if member == ctx.id then own = binding[2] end
            if member == h.home(id) then assert(binding[2] == id:sub(38), 'home binding differs from identity') end
        end
        assert(own, 'scope excludes this participant')
        return {id,'OPEN',members,deadline,false,false,'VOTES',true,true,bindings,blob,true,mac}, own
    end
    function api.tx_open_freeze(id, term)
        require_home(id, term)
        return box.atomic(function()
            local root = box.space._cairn_open_tx:get{id}
            if not root or root[4] <= os.time() then h.fail('ABORTED', 'open coordinator was lost or expired') end
            local requested = unsigned(id:sub(38))
            if not same_incarnation(true, requested, requested) then h.fail('ABORTED', 'home incarnation changed') end
            if not root[8] then
                local blob = json.encode({id,root[4],root[3],root[10]})
                local mac = h.certificate(id, 'OPEN_SCOPE_' .. blob, root[3])
                root = box.space._cairn_open_tx:update({id}, {{'=',8,true},{'=',11,blob},{'=',13,mac}})
            end
            return h.representation(root)
        end)
    end
    function api.tx_open_status(id, term)
        require_home(id, term)
        return ctx.read(term, function()
            local root = box.space._cairn_tx:get{id} or box.space._cairn_open_tx:get{id}
            return root and h.representation(root) or box.NULL
        end)
    end
    local function install_root(row)
        local existing = box.space._cairn_tx:get{row[1]}
        if existing then
            assert(existing[11] == row[11] and existing[13] == row[13], 'conflicting frozen scope')
            return existing
        end
        return box.space._cairn_tx:insert(row)
    end
    function api.tx_open_prepare(id, enlistment, blob, mac, term)
        ctx.fence(term)
        local root, epoch = scope(id, blob, mac)
        local requested, deadline, token_epoch = decode_token(id, ctx.id, enlistment)
        assert(token_epoch == epoch and deadline == root[4], 'prepare enlistment differs from frozen scope')
        local yes = box.atomic(function()
            local prior = box.space._cairn_parts:get{id}
            if prior then return prior[3] == 'YES' end
            local s, current = h.spaces(id), box.space._cairn_open_parts:get{id}
            local valid = same_incarnation(current ~= nil, current and current[3] or 0ULL, requested)
                and current[4] == deadline and deadline > os.time()
            if h.home(id) == ctx.id then
                local local_root = box.space._cairn_open_tx:get{id}
                valid = valid and local_root and local_root[8] and local_root[11] == blob and local_root[13] == mac
                install_root(root)
            end
            if not valid then
                box.space._cairn_parts:insert{id,'VOTED_ABORT','NO',blob,mac}
                if h.home(id) == ctx.id then box.space._cairn_tx:update({id}, {{'=',2,'ABORT'}}) end
                clear_claims(id)
                return false
            end
            -- Copy every acknowledged write AND every row/table read claim in
            -- the same WAL transaction as YES. Atomic removal leaves no gap.
            for _, intent in ipairs(s.intents.index.primary:select{id}) do box.space._cairn_intents:insert(intent:totable()) end
            for _, lock in ipairs(s.locks.index.by_tx:select{id}) do box.space._cairn_locks:insert(lock:totable()) end
            for _, lock in ipairs(s.tables.index.by_tx:select{id}) do box.space._cairn_table_locks:insert(lock:totable()) end
            box.space._cairn_parts:insert{id,'PREPARED','YES',blob,mac}
            clear_claims(id)
            return true
        end)
        if not yes then h.fail('ABORTED', 'participant durably voted NO because open state was lost, closed or expired') end
        return h.certificate(id, 'PREPARED', {ctx.id})
    end
    function api.tx_open_finish(id, blob, mac, prepared, term)
        ctx.fence(term)
        local root = scope(id, blob, mac)
        h.verify_receipts(root, prepared, 'PREPARED')
        -- Unanimous immutable YES receipts already establish COMMIT. Verify
        -- them here instead of requiring another certificate RPC to the home.
        box.atomic(function() h.finish(id, 'COMMIT') end)
        return h.certificate(id, 'RELEASED_COMMIT', {ctx.id})
    end
    function api.tx_open_finish_last(id, blob, mac, prepared, released, term)
        require_home(id, term)
        local root = scope(id, blob, mac)
        h.verify_receipts(root, prepared, 'PREPARED')
        assert(type(released) == 'table' and #released == #root[3] - 1, 'missing remote finish receipts')
        local index = 1
        for _, member in ipairs(root[3]) do
            if member ~= ctx.id then
                h.validate(id, 'RELEASED_COMMIT', {member}, released[index])
                index = index + 1
            end
        end
        local result = box.atomic(function()
            local durable = box.space._cairn_tx:get{id}
            assert(durable and durable[11] == blob and durable[13] == mac, 'home scope is not durably registered')
            assert(durable[2] ~= 'ABORT', 'conflicting durable decision')
            if durable[5] and durable[6] then
                assert(durable[2] == 'COMMIT', 'invalid completed outcome')
                return durable
            end
            -- Every remote application is durable. Install the home rows,
            -- release its locks and materialize global completion together.
            h.finish(id, 'COMMIT')
            return box.space._cairn_tx:update({id}, {{'=',2,'COMMIT'},{'=',5,true},{'=',6,true}})
        end)
        return h.representation(result)
    end
    function api.tx_open_recover(id, blob, mac, force, term)
        require_home(id, term)
        local root = scope(id, blob, mac)
        return box.atomic(function()
            local durable = box.space._cairn_tx:get{id}
            if durable then return h.representation(install_root(root)) end
            local current = box.space._cairn_open_tx:get{id}
            local epoch = unsigned(id:sub(38))
            local alive = current and current[8] and current[11] == blob and current[13] == mac
                and current[4] > os.time() and same_incarnation(true, epoch, epoch)
            if alive and not force then return h.representation(current) end
            assert(not box.space._cairn_parts:get{id} or box.space._cairn_parts:get{id}[3] ~= 'YES', 'home YES is missing its coordinator')
            root[2] = 'ABORT'
            install_root(root)
            box.space._cairn_parts:replace{id,'VOTED_ABORT','NO',blob,mac}
            clear_claims(id)
            return h.representation(box.space._cairn_tx:get{id})
        end)
    end
    function out.materialize_abort(id)
        -- Recovery may learn a remote NO while the home still has only its
        -- frozen temporary root. Materialize the scope and home NO atomically
        -- before the ordinary decision API attempts to sign the abort.
        local root = box.space._cairn_open_tx:get{id}
        assert(root and root[8], 'volatile transaction requires authenticated scope recovery')
        local prior = box.space._cairn_parts:get{id}
        assert(not prior or prior[3] ~= 'YES', 'home YES is missing its coordinator')
        local row = root:totable()
        row[2] = 'ABORT'
        local result = box.space._cairn_tx:insert(row)
        box.space._cairn_parts:replace{id,'VOTED_ABORT','NO',row[11],row[13]}
        clear_claims(id)
        return result
    end
    function api.tx_open_pending(term)
        ctx.fence(term)
        return ctx.read(term, function()
            local result = {}
            for _, phase in ipairs({'PREPARED','VOTED_ABORT'}) do
                for _, part in box.space._cairn_parts.index.phase:pairs(phase, {iterator='EQ'}) do
                    if #part[1] == 53 and part[4] then
                        table.insert(result, {part[1],part[4],part[5]})
                        if #result >= 128 then return result end
                    end
                end
            end
            return result
        end)
    end
    function api.tx_open_abort(id, enlistment, persist, term)
        ctx.fence(term)
        local home = open_id(id)
        out.capacity(id)
        if home ~= ctx.id then decode_token(id, ctx.id, enlistment) end
        return box.atomic(function()
            local durable = box.space._cairn_parts:get{id}
            if durable then
                if durable[3] == 'YES' then h.fail('ABORTED', 'voted transaction requires scope recovery') end
                return true
            end
            local root = box.space._cairn_open_tx:get{id}
            if root and root[8] then h.fail('ABORTED', 'frozen transaction requires scope recovery') end
            if home == ctx.id and persist then
                -- Compete on the same durable key as an uncertain local commit.
                -- A successful abort must fence its delayed WAL transaction.
                local row = root and root:totable() or
                    {id,'ABORT',{ctx.id},os.time(),true,true,'VOTES',false,true,{},'',true,''}
                row[2],row[5],row[6] = 'ABORT',true,true
                box.space._cairn_tx:insert(row)
                box.space._cairn_parts:replace{id,'DONE_ABORT','NO'}
                out.clear(id)
                return true
            end
            mark_used(id)
            clear_claims(id)
            return true
        end)
    end
    function api.tx_open_local(id, term)
        require_home(id, term)
        local row = box.atomic(function()
            local durable = box.space._cairn_tx:get{id}
            if durable then return durable end
            local root = box.space._cairn_open_tx:get{id}
            local part = box.space._cairn_open_parts:get{id}
            local epoch = unsigned(id:sub(38))
            if not root or not part or not same_incarnation(true, part[3], epoch) or root[4] <= os.time() then
                local aborted = {id,'ABORT',{ctx.id},os.time(),true,true,'VOTES',false,true,{},'',true,''}
                box.space._cairn_tx:insert(aborted)
                box.space._cairn_parts:replace{id,'DONE_ABORT','NO'}
                out.clear(id)
                return box.space._cairn_tx:get{id}
            end
            assert(#root[3] == 1 and root[3][1] == ctx.id and not root[8], 'local commit requires exactly one unfrozen participant')
            for _, intent in ipairs(box.space._cairn_open_intents.index.primary:select{id}) do
                if intent[4] then box.space[intent[2]]:delete{intent[3]}
                else box.space[intent[2]]:replace(intent[5]) end
            end
            local result = root:totable()
            result[2],result[5],result[6] = 'COMMIT',true,true
            box.space._cairn_tx:insert(result)
            box.space._cairn_parts:replace{id,'DONE_COMMIT','YES'}
            out.clear(id)
            return box.space._cairn_tx:get{id}
        end)
        return h.representation(row)
    end
    return out
end
