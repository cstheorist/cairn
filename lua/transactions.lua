-- Recoverable strict two-phase locking and two-phase commit over synchronous
-- Tarantool spaces. User tuples change only after a durable COMMIT decision.
-- Decision, prepare, apply and release receipts are authenticated so the gateway
-- cannot manufacture progress after an ambiguous RPC result.
return function(ctx)
    local api = ctx.api
    local crypto, digest = require('crypto'), require('digest')
    local ffi = require('ffi')
    ffi.cdef[[
        typedef struct { int32_t valid; int64_t value; } cairn_checked_integer;
        typedef struct { int32_t valid; uint64_t value; } cairn_checked_unsigned;
        uint32_t cairn_verified_abi_version(void);
        cairn_checked_integer cairn_checked_add_i64(int64_t a, int64_t b);
        cairn_checked_unsigned cairn_next_incarnation(uint64_t current);
        int32_t cairn_may_use_open_state(int32_t present, uint64_t current, uint64_t stored, uint64_t requested);
    ]]
    local verified = ffi.load('/usr/local/lib/libcairn_verified_native.so')
    assert(verified.cairn_verified_abi_version() == 2, 'unsupported verified storage ABI')
    local secret = os.getenv('CAIRN_TX_SECRET') or 'development-cairn-transaction-key'
    assert(#secret >= 16, 'CAIRN_TX_SECRET must have at least 16 bytes')
    local timeout = tonumber(os.getenv('CAIRN_TX_TIMEOUT') or '30')
    assert(timeout >= 1 and timeout <= 3600)
    local exports = {}
    local volatile
    function exports.next_incarnation(current)
        local checked = verified.cairn_next_incarnation(current)
        assert(checked.valid == 1, 'storage incarnation exhausted')
        return checked.value
    end
    local functions = {'tx_begin', 'tx_enlist', 'tx_status', 'tx_decide', 'tx_complete',
        'tx_clean', 'tx_pending', 'tx_read', 'tx_scan', 'tx_mutate', 'tx_prepare', 'tx_apply', 'tx_release', 'tx_commit_local', 'tx_start', 'tx_start_voted', 'tx_vote_status', 'tx_vote_abort', 'tx_start_finished', 'tx_finish_vote'}

    local function fail(code, message)
        -- Preserve the SQLSTATE marker through nested Lua calls. String errors
        -- acquire a caller source location when box.atomic rethrows them.
        box.error({code = box.error.PROC_LUA, reason = '[CAIRN:' .. code .. '] ' .. message})
    end
    local function transaction_id(id)
        assert(type(id) == 'string' and (#id == 36 or #id == 53), 'invalid transaction id')
        assert(id:sub(1, 36):match('^[0-9a-f%-]+$'), 'invalid transaction UUID')
        if #id == 53 then assert(id:sub(37, 37) == '~' and id:sub(38):match('^[0-9a-f]+$'), 'invalid incarnation suffix') end
        assert(id:sub(9, 9) == '-' and id:sub(14, 14) == '-' and id:sub(19, 19) == '-' and id:sub(24, 24) == '-', 'invalid UUID layout')
        assert(id:sub(15, 15) == '4' or id:sub(15, 15) == '8', 'unsupported transaction ID version')
    end
    local function home(id)
        transaction_id(id)
        return id:sub(15, 15) == '8' and tonumber(id:sub(1, 4), 16) or 0
    end
    local function certificate(id, stage, members)
        local message = id .. '|' .. stage .. '|' .. table.concat(members, ',')
        return digest.base64_encode(crypto.hmac.sha256(secret, message))
    end
    local function validate_certificate(id, stage, members, token)
        transaction_id(id)
        assert(type(members) == 'table' and #members <= ctx.count, 'invalid participants')
        for index, member in ipairs(members) do
            assert(type(member) == 'number' and member >= 0 and member < ctx.count and member % 1 == 0, 'invalid participant')
            assert(index == 1 or members[index - 1] < member, 'participants must be sorted and unique')
        end
        assert(type(token) == 'string' and certificate(id, stage, members) == token, 'invalid transaction certificate')
    end
    local function contains(members, member)
        for _, candidate in ipairs(members) do if candidate == member then return true end end
        return false
    end
    local function participant_certificate(id, stage, members, token)
        validate_certificate(id, stage, members, token)
        assert(contains(members, ctx.id), 'certificate does not include this shard')
    end
    local function coordinator(term, id)
        ctx.fence(term)
        if id then assert(home(id) == ctx.id, 'wrong transaction coordinator') end
    end
    local function record(id)
        transaction_id(id)
        assert(home(id) == ctx.id, 'wrong transaction coordinator')
        local row = box.space._cairn_tx:get{id}
        assert(row, 'unknown transaction')
        return row
    end
    local function representation(row, kind, complete)
        local result = row:totable()
        result[2] = kind or row[2]
        result[5] = complete or row[5]
        result[7] = result[2] == 'OPEN' and '' or certificate(row[1], result[2], row[3])
        result[8] = result[5] and certificate(row[1], 'COMPLETE_' .. result[2], row[3]) or ''
        result[9] = row[7] == 'VOTES'
        result[10] = row[7] == 'VOTES' and row[8] == true
        result[11] = result[10] and certificate(row[1], 'VOTE_SCOPE', row[3]) or ''
        result[12] = row[4] <= os.time()
        result[13] = row[9] == true and result[10] and certificate(row[1], 'FINISH_SCOPE', row[3]) or ''
        if volatile then volatile.decorate(result, row) end
        return result
    end
    local function verify_receipts(row, receipts, stage)
        assert(type(receipts) == 'table' and #receipts == #row[3], 'missing participant receipts')
        for index, member in ipairs(row[3]) do
            validate_certificate(row[1], stage, {member}, receipts[index])
        end
    end

    function exports.initialize()
        local definitions = {
            {'_cairn_parts', {{1, 'string'}}},
            {'_cairn_intents', {{1, 'string'}, {2, 'string'}, {3, 'integer'}}},
            {'_cairn_locks', {{1, 'string'}, {2, 'integer'}, {3, 'string'}}},
            {'_cairn_tx', {{1, 'string'}}},
            {'_cairn_table_locks', {{1, 'string'}, {2, 'string'}}},
        }
        for _, definition in ipairs(definitions) do
            local space = box.schema.space.create(definition[1], {if_not_exists = true, is_sync = ctx.replicated})
            space:create_index('primary', {parts = definition[2], if_not_exists = true})
        end
        box.space._cairn_locks:create_index('by_tx', {parts = {{3, 'string'}, {1, 'string'}, {2, 'integer'}}, if_not_exists = true})
        box.space._cairn_table_locks:create_index('by_tx', {parts = {{2, 'string'}, {1, 'string'}}, if_not_exists = true})
        box.space._cairn_table_locks:create_index('by_mode', {parts = {{1, 'string'}, {3, 'string'}, {2, 'string'}}, if_not_exists = true})
        box.space._cairn_locks:create_index('by_mode', {parts = {{1, 'string'}, {2, 'integer'}, {4, 'string'}, {3, 'string'}}, if_not_exists = true})
        box.space._cairn_tx:create_index('pending', {parts = {{2, 'string'}, {6, 'boolean'}}, unique = false, if_not_exists = true})
        volatile.initialize()
    end

    local function spaces(id)
        local prefix = #id == 53 and '_cairn_open_' or '_cairn_'
        return {parts = box.space[prefix .. 'parts'], intents = box.space[prefix .. 'intents'],
            locks = box.space[prefix .. 'locks'], tables = box.space[prefix .. 'table_locks']}
    end

    function exports.check_unlocked(name, key, id, write)
        -- Read only incompatible mode ranges. Reading compatible IX/R entries
        -- would create spurious MVCC dependencies on unrelated transactions.
        for _, prefix in ipairs({'_cairn_', '_cairn_open_'}) do
          for _, mode in ipairs(write and {'R','X'} or {'X'}) do
            for _, lock in box.space[prefix .. 'table_locks'].index.by_mode:pairs({name, mode}, {iterator = 'EQ'}) do
                if lock[2] ~= id then fail('CONFLICT', 'table has a conflicting scan or schema lock') end
            end
        end
          for _, mode in ipairs(write and {'R','W'} or {'W'}) do
            for _, lock in box.space[prefix .. 'locks'].index.by_mode:pairs({name, key, mode}, {iterator = 'EQ'}) do
                if lock[3] ~= id then fail('CONFLICT', 'row belongs to an unresolved transaction; retry the whole transaction') end
            end
          end
        end
    end

    local function active_participant(id, enlistment)
        if #id == 53 then return volatile.active(id, enlistment) end
        validate_certificate(id, 'ENLIST', {ctx.id}, enlistment)
        local part = box.space._cairn_parts:get{id}
        if not part then
            box.space._cairn_parts:insert{id, 'OPEN'}
        elseif part[2] ~= 'OPEN' then
            fail('ABORTED', 'transaction no longer accepts operations')
        end
    end
    local function start_local(id, first)
        if not first then return end
        if type(first) == 'table' and first[1] == 'VOLATILE' then return volatile.start(id, first[2]) end
        assert(#id == 36, 'incarnation-scoped transactions require volatile start')
        assert(home(id) == ctx.id, 'first participant must host its coordinator')
        assert(not box.space._cairn_tx:get{id} and not box.space._cairn_parts:get{id}, 'transaction id already exists')
        if first == 'FINISH' then
            box.space._cairn_tx:insert{id, 'OPEN', {ctx.id}, os.time() + timeout, false, false, 'VOTES', false, true}
        elseif first == 'VOTES' then
            box.space._cairn_tx:insert{id, 'OPEN', {ctx.id}, os.time() + timeout, false, false, 'VOTES', false}
        else
            box.space._cairn_tx:insert{id, 'OPEN', {ctx.id}, os.time() + timeout, false, false}
        end
    end
    local function lock_row(id, name, key, write)
        exports.check_unlocked(name, key, id, write)
        local locks = spaces(id).locks
        local existing = locks:get{name, key, id}
        if not existing or (write and existing[4] ~= 'W') then
            if not existing and locks.index.by_tx:count{id} >= 4096 then fail('LIMIT', 'transaction exceeds 4096 row locks') end
            locks:replace{name, key, id, write and 'W' or 'R'}
        end
    end
    local function lock_table(id, name, scan)
        local tables = spaces(id).tables
        local existing = tables:get{name, id}
        local requested = scan and 'R' or 'IX'
        -- A transaction that both scans and writes must retain both claims.
        -- X is the conservative SIX mode: no other table lock is compatible.
        if existing and (existing[3] == 'X' or existing[3] ~= requested) then requested = 'X' end
        local incompatible = requested == 'X' and {'R','IX','X'} or (requested == 'R' and {'IX','X'} or {'R','X'})
        for _, prefix in ipairs({'_cairn_', '_cairn_open_'}) do
          for _, mode in ipairs(incompatible) do
            for _, lock in box.space[prefix .. 'table_locks'].index.by_mode:pairs({name, mode}, {iterator = 'EQ'}) do
                if lock[2] ~= id then fail('CONFLICT', 'table scan conflicts with a concurrent writer') end
            end
          end
        end
        if not existing or existing[3] ~= requested then
            tables:replace{name, id, requested}
        end
    end
    local function current_row(id, name, key)
        local intent = spaces(id).intents:get{id, name, key}
        if intent then
            if intent[4] then return nil end
            return intent[5]
        end
        local row = box.space[name]:get{key}
        return row and row:totable() or nil
    end

    local function add_integer(a, b, column)
        if a == box.NULL or b == box.NULL then
            if not column.nullable then fail('NOT_NULL', 'NULL violates NOT NULL') end
            return box.NULL
        end
        local function valid(value)
            if type(value) == 'number' then
                return value % 1 == 0 and value >= -9007199254740991 and value <= 9007199254740991
            end
            return ffi.istype('int64_t', value) or
                (ffi.istype('uint64_t', value) and value <= 0x7FFFFFFFFFFFFFFFULL)
        end
        assert(valid(a) and valid(b), 'integer addition requires signed integers')
        a, b = ffi.cast('int64_t', a), ffi.cast('int64_t', b)
        local checked = verified.cairn_checked_add_i64(a, b)
        if checked.valid ~= 1 then
            fail('RANGE', 'BIGINT arithmetic overflow')
        end
        local result = checked.value
        if (column.kind == 'smallint' and (result < -32768LL or result > 32767LL)) or
           (column.kind == 'integer' and (result < -2147483648LL or result > 2147483647LL)) then
            fail('RANGE', column.kind .. ' arithmetic overflow')
        end
        return result
    end

    function api.tx_begin(id, term)
        assert(#id == 36, 'incarnation-scoped transactions require volatile start')
        coordinator(term, id)
        transaction_id(id)
        return box.atomic(function()
            assert(not box.space._cairn_tx:get{id}, 'transaction id already exists')
            return representation(box.space._cairn_tx:insert{id, 'OPEN', {}, os.time() + timeout, false, false})
        end)
    end
    function api.tx_enlist(id, member, term)
        coordinator(term, id)
        return box.atomic(function()
            local row = record(id)
            if row[2] ~= 'OPEN' or row[4] <= os.time() then fail('ABORTED', 'transaction expired or decided') end
            assert(type(member) == 'number' and member >= 0 and member < ctx.count and member % 1 == 0, 'invalid participant')
            if not contains(row[3], member) then
                if row[7] == 'VOTES' and row[8] then fail('ABORTED', 'transaction membership is frozen') end
                local members = row[3]
                table.insert(members, member)
                table.sort(members)
                box.space._cairn_tx:update({id}, {{'=', 3, members}})
            end
            return certificate(id, 'ENLIST', {member})
        end)
    end
    function api.tx_status(id, term)
        coordinator(term, id)
        return ctx.read(term, function() return representation(record(id)) end)
    end
    function api.tx_decide(id, decision, receipts, term)
        coordinator(term, id)
        assert(decision == 'COMMIT' or decision == 'ABORT', 'invalid decision')
        local result, certified_kind = box.atomic(function()
            local row = box.space._cairn_tx:get{id}
            if #id == 53 and not row then
                if decision == 'ABORT' then row = volatile.materialize_abort(id)
                else error('volatile transaction requires scope recovery before decision') end
            end
            if not row and decision == 'ABORT' then
                -- A failed/ambiguous atomic first operation may have created
                -- nothing. Fence a delayed start before acknowledging rollback.
                row = box.space._cairn_tx:insert{id, 'ABORT', {}, os.time() + timeout, false, false}
            end
            assert(row, 'unknown transaction')
            if row[2] ~= 'OPEN' then return row end
            if row[7] == 'VOTES' then
                if decision == 'COMMIT' then
                    assert(row[8] and #row[3] > 0, 'commit requires frozen membership')
                    verify_receipts(row, receipts, 'PREPARED')
                    -- Unanimous durable YES votes are already an immutable
                    -- commit fact. Issuing its certificate needs no new WAL.
                    return row, 'COMMIT'
                end
                local part = box.space._cairn_parts:get{id}
                if not part or part[3] ~= 'YES' then
                    assert(not part or part[2] == 'OPEN' or part[3] == 'NO', 'cannot vote abort')
                    box.space._cairn_parts:replace{id, 'VOTED_ABORT', 'NO'}
                elseif #receipts == 0 then
                    -- The caller must obtain an immutable NO from another
                    -- participant, or discover that every vote is YES.
                    return row
                else
                    assert(#receipts == 2 and contains(row[3], receipts[1]), 'abort requires a participant NO')
                    validate_certificate(id, 'VOTED_ABORT', {receipts[1]}, receipts[2])
                end
                return box.space._cairn_tx:update({id}, {{'=', 2, 'ABORT'}, {'=', 8, true}})
            end
            if decision == 'COMMIT' then
                if row[4] <= os.time() then fail('ABORTED', 'transaction expired before decision') end
                assert(#row[3] > 0, 'empty distributed commit')
                verify_receipts(row, receipts, 'PREPARED')
            end
            return box.space._cairn_tx:update({id}, {{'=', 2, decision}})
        end)
        -- Sign only after a durable decision, or every durable YES, is known.
        return representation(result, certified_kind)
    end
    function api.tx_complete(id, receipts, term)
        coordinator(term, id)
        local result, kind, completed = box.atomic(function()
            local row = record(id)
            if row[7] == 'VOTES' then
                if row[5] then return row end
                local decision = row[2] == 'OPEN' and 'COMMIT' or row[2]
                assert(row[8], 'completion requires frozen membership')
                verify_receipts(row, receipts, 'APPLIED_' .. decision)
                -- Application receipts durably establish global completion;
                -- the coordinator materializes its flags during cleanup.
                return row, decision, true
            end
            assert(row[2] ~= 'OPEN', 'transaction has no decision')
            if row[5] then return row end
            verify_receipts(row, receipts, 'APPLIED_' .. row[2])
            return box.space._cairn_tx:update({id}, {{'=', 5, true}})
        end)
        return representation(result, kind, completed)
    end
    function api.tx_clean(id, receipts, term)
        coordinator(term, id)
        return box.atomic(function()
            local row = record(id)
            if row[7] == 'VOTES' then
                local decision = row[2] == 'OPEN' and 'COMMIT' or row[2]
                verify_receipts(row, receipts, 'RELEASED_' .. decision)
                if row[5] and row[6] then return true end
                assert(row[8], 'cleanup requires frozen membership')
                box.space._cairn_tx:update({id}, {{'=', 2, decision}, {'=', 5, true}, {'=', 6, true}})
                return true
            end
            assert(row[5], 'transaction is not complete')
            verify_receipts(row, receipts, 'RELEASED_' .. row[2])
            box.space._cairn_tx:update({id}, {{'=', 6, true}})
            return true
        end)
    end
    function api.tx_commit_local(id, term)
        coordinator(term, id)
        local result = box.atomic(function()
            local row = record(id)
            assert(#row[3] == 1 and row[3][1] == ctx.id, 'local commit requires exactly this shard')
            if row[2] ~= 'OPEN' then return row end
            if row[4] <= os.time() then fail('ABORTED', 'transaction expired before decision') end
            local part = box.space._cairn_parts:get{id}
            assert(part and part[2] == 'OPEN', 'local participant must be open')
            -- The decision, all row changes, tombstone, and lock cleanup share
            -- one synchronous WAL transaction. There is no remote participant.
            local intents = box.space._cairn_intents.index.primary:select{id}
            for _, intent in ipairs(intents) do
                if intent[4] then box.space[intent[2]]:delete{intent[3]}
                else box.space[intent[2]]:replace(intent[5]) end
                box.space._cairn_intents:delete{id, intent[2], intent[3]}
            end
            local locks = box.space._cairn_locks.index.by_tx:select{id}
            for _, lock in ipairs(locks) do box.space._cairn_locks:delete{lock[1], lock[2], id} end
            local tables = box.space._cairn_table_locks.index.by_tx:select{id}
            for _, lock in ipairs(tables) do box.space._cairn_table_locks:delete{lock[1], id} end
            box.space._cairn_parts:replace{id, 'DONE_COMMIT', 'YES'}
            return box.space._cairn_tx:update({id}, {{'=', 2, 'COMMIT'}, {'=', 5, true}, {'=', 6, true}})
        end)
        return representation(result)
    end
    function api.tx_pending(term)
        coordinator(term)
        return ctx.read(term, function()
            local result = {}
            for _, phase in ipairs({'COMMIT', 'ABORT', 'OPEN'}) do
                for _, row in box.space._cairn_tx.index.pending:pairs({phase, false}, {iterator = 'EQ'}) do
                    if phase ~= 'OPEN' or row[4] <= os.time() or (row[7] == 'VOTES' and row[8]) then
                        table.insert(result, representation(row))
                        if #result >= 128 then return result end
                    end
                end
            end
            return result
        end)
    end

    local function start_operation(id, operation, arguments, term, first)
        coordinator(term, id)
        assert(operation == 'cairn.tx_read' or operation == 'cairn.tx_mutate' or operation == 'cairn.tx_scan', 'invalid first operation')
        assert(type(arguments) == 'table', 'invalid first operation arguments')
        local token = certificate(id, 'ENLIST', {ctx.id})
        local parameters = {id}
        for _, argument in ipairs(arguments) do table.insert(parameters, argument) end
        table.insert(parameters, token); table.insert(parameters, term); table.insert(parameters, first)
        -- The operation inserts the coordinator, enlists this participant and
        -- acquires its first locks/intents inside ONE synchronous transaction.
        -- Return the token only after that operation's commit has returned.
        local result = {api[operation:match('^cairn%.(.+)$')](unpack(parameters))}
        return token, result
    end
    function api.tx_start(id, operation, arguments, term)
        return start_operation(id, operation, arguments, term, true)
    end
    function api.tx_start_voted(id, operation, arguments, term)
        return start_operation(id, operation, arguments, term, 'VOTES')
    end
    function api.tx_start_finished(id, operation, arguments, term)
        return start_operation(id, operation, arguments, term, 'FINISH')
    end
    function api.tx_read(id, name, key, enlistment, term, first)
        ctx.fence(term)
        ctx.schema(name); ctx.owner(key)
        if #id == 53 then volatile.capacity(id) end
        return box.atomic(function()
            start_local(id, first)
            active_participant(id, enlistment)
            lock_row(id, name, key, false)
            return current_row(id, name, key) or {}
        end)
    end
    function api.tx_mutate(id, name, key, operation, value, enlistment, term, first)
        ctx.fence(term)
        local schema = ctx.schema(name)
        ctx.owner(key)
        if #id == 53 then volatile.capacity(id) end
        return box.atomic(function()
            start_local(id, first)
            active_participant(id, enlistment)
            lock_table(id, name, false)
            lock_row(id, name, key, true)
            local current = current_row(id, name, key)
            local updated
            if operation == 'insert' then
                if current then fail('DUPLICATE', 'duplicate primary key') end
                updated = value
            elseif operation == 'delete' then
                updated = nil
            elseif operation == 'update' then
                for _, change in ipairs(value) do
                    assert(change[1] == '=' and change[2] ~= schema.primary_key + 1, 'invalid update')
                end
                if current then updated = box.tuple.new(current):update(value):totable() end
            elseif operation == 'add' then
                assert(type(value) == 'table' and #value == 2, 'invalid increment')
                local field = value[1]
                assert(type(field) == 'number' and field % 1 == 0 and field >= 1 and field <= #schema.columns and field ~= schema.primary_key + 1, 'invalid increment column')
                local column = schema.columns[field]
                assert(column.kind == 'smallint' or column.kind == 'integer' or column.kind == 'bigint', 'integer increment required')
                if current then
                    updated = box.tuple.new(current):totable()
                    updated[field] = add_integer(current[field], value[2], column)
                end
            else error('invalid mutation') end
            if updated then
                assert(#updated == #schema.columns and updated[schema.primary_key + 1] == key, 'invalid tuple/key')
                box.tuple.new(updated, {format = box.tuple.format.new(box.space[name]:format())})
            end
            local intents = spaces(id).intents
            if not intents:get{id, name, key} and intents.index.primary:count{id} >= 4096 then
                fail('LIMIT', 'transaction exceeds 4096 modified rows')
            end
            intents:replace{id, name, key, updated == nil, updated or {}}
            return operation == 'insert' or current ~= nil
        end)
    end
    function api.tx_scan(id, name, after, limit, enlistment, term, first)
        ctx.fence(term)
        local schema = ctx.schema(name)
        assert(type(limit) == 'number' and limit >= 1 and limit <= 1024 and limit % 1 == 0, 'invalid scan page size')
        if after == box.NULL then after = nil end
        if #id == 53 then volatile.capacity(id) end
        return box.atomic(function()
            start_local(id, first)
            active_participant(id, enlistment)
            lock_table(id, name, true)
            local own = spaces(id).intents.index.primary:select{id, name}
            local raw = box.space[name].index.primary:select(after and {after} or {}, {
                iterator = after and 'GT' or 'ALL', limit = limit + #own + 1})
            local ffi = require('ffi')
            local function signed(key) return ffi.cast('int64_t', key) end
            local function key_name(key) return tostring(signed(key)) end
            local merged = {}
            for _, row in ipairs(raw) do merged[key_name(row[schema.primary_key + 1])] = row:totable() end
            for _, intent in ipairs(own) do
                if not after or signed(intent[3]) > signed(after) then
                    if intent[4] then merged[key_name(intent[3])] = nil
                    else merged[key_name(intent[3])] = intent[5] end
                end
            end
            local rows = {}
            for _, row in pairs(merged) do table.insert(rows, row) end
            table.sort(rows, function(a, b) return signed(a[schema.primary_key + 1]) < signed(b[schema.primary_key + 1]) end)
            local more = #rows > limit
            while #rows > limit do table.remove(rows) end
            return rows, more and rows[#rows][schema.primary_key + 1] or box.NULL
        end)
    end
    function api.tx_prepare(id, enlistment, term)
        assert(#id == 36, 'incarnation-scoped transactions require scope-aware prepare')
        ctx.fence(term)
        validate_certificate(id, 'ENLIST', {ctx.id}, enlistment)
        box.atomic(function()
            local part = box.space._cairn_parts:get{id}
            assert(part and (part[2] == 'OPEN' or part[2] == 'PREPARED'), 'participant cannot prepare')
            if home(id) == ctx.id then
                local row = record(id)
                if row[7] == 'VOTES' then
                    if not row[8] then
                        if row[4] <= os.time() then fail('ABORTED', 'transaction expired before voting') end
                        box.space._cairn_tx:update({id}, {{'=', 8, true}})
                    end
                end
            end
            if part[2] == 'OPEN' then box.space._cairn_parts:update({id}, {{'=', 2, 'PREPARED'}, {'=', 3, 'YES'}}) end
        end)
        return certificate(id, 'PREPARED', {ctx.id})
    end
    function api.tx_apply(id, decision, members, token, term)
        ctx.fence(term)
        assert(decision == 'COMMIT' or decision == 'ABORT', 'invalid decision')
        participant_certificate(id, decision, members, token)
        box.atomic(function()
            local part = box.space._cairn_parts:get{id}
            local applied = 'APPLIED_' .. decision
            if part and (part[2] == applied or part[2] == 'DONE_' .. decision) then return end
            if decision == 'COMMIT' then
                assert(part and part[2] == 'PREPARED', 'commit requires durable prepare')
                for _, intent in box.space._cairn_intents.index.primary:pairs({id}, {iterator = 'EQ'}) do
                    if intent[4] then box.space[intent[2]]:delete{intent[3]}
                    else box.space[intent[2]]:replace(intent[5]) end
                end
            else
                assert(not part or part[2] == 'OPEN' or part[2] == 'PREPARED' or part[2] == 'VOTED_ABORT', 'conflicting decision')
            end
            box.space._cairn_parts:replace{id, applied, part and part[3] or box.NULL}
        end)
        return certificate(id, 'APPLIED_' .. decision, {ctx.id})
    end

    local function vote_result(id, part)
        if not part or part[2] == 'OPEN' then return {'OPEN', ''} end
        if part[3] == 'YES' then return {'YES', certificate(id, 'PREPARED', {ctx.id})} end
        if part[3] == nil or part[3] == box.NULL then
            -- An abort may release an unprepared participant without requiring
            -- that participant's own vote. Absence of a vote is not a YES.
            assert(part[2] == 'APPLIED_ABORT' or part[2] == 'DONE_ABORT', 'invalid unrecorded vote')
            return {'OPEN', ''}
        end
        assert(part[3] == 'NO', 'invalid durable participant vote')
        return {'NO', certificate(id, 'VOTED_ABORT', {ctx.id})}
    end
    function api.tx_vote_status(id, members, scope, term)
        ctx.fence(term)
        participant_certificate(id, 'VOTE_SCOPE', members, scope)
        return ctx.read(term, function() return vote_result(id, box.space._cairn_parts:get{id}) end)
    end
    function api.tx_vote_abort(id, members, scope, term)
        ctx.fence(term)
        participant_certificate(id, 'VOTE_SCOPE', members, scope)
        local part = box.atomic(function()
            local current = box.space._cairn_parts:get{id}
            if not current or current[2] == 'OPEN' then
                return box.space._cairn_parts:replace{id, 'VOTED_ABORT', 'NO'}
            end
            return current
        end)
        return vote_result(id, part)
    end
    local function finish_participant(id, decision)
        local part = box.space._cairn_parts:get{id}
        if part and part[2] == 'DONE_' .. decision then return end
        if decision == 'COMMIT' then
            assert(part and (part[2] == 'PREPARED' or part[2] == 'APPLIED_COMMIT'), 'commit requires durable YES')
            if part[2] == 'PREPARED' then
                for _, intent in box.space._cairn_intents.index.primary:pairs({id}, {iterator = 'EQ'}) do
                    if intent[4] then box.space[intent[2]]:delete{intent[3]}
                    else box.space[intent[2]]:replace(intent[5]) end
                end
            end
        else
            assert(not part or part[2] == 'OPEN' or part[2] == 'PREPARED' or part[2] == 'VOTED_ABORT' or part[2] == 'APPLIED_ABORT', 'conflicting decision')
        end
        -- The global decision is irrevocable. Install this shard's complete
        -- image and unlock it in one WAL transaction. Unfinished shards
        -- retain their write locks, so SQL cannot read their old versions.
        local locks = box.space._cairn_locks.index.by_tx:select{id}
        for _, lock in ipairs(locks) do box.space._cairn_locks:delete{lock[1], lock[2], id} end
        local intents = box.space._cairn_intents.index.primary:select{id}
        for _, intent in ipairs(intents) do box.space._cairn_intents:delete{id, intent[2], intent[3]} end
        local tables = box.space._cairn_table_locks.index.by_tx:select{id}
        for _, lock in ipairs(tables) do box.space._cairn_table_locks:delete{lock[1], id} end
        box.space._cairn_parts:replace{id, 'DONE_' .. decision, part and part[3] or box.NULL}
        if #id == 53 then volatile.clear(id) end
    end
    function api.tx_finish_vote(id, decision, members, token, scope, term)
        ctx.fence(term)
        assert(decision == 'COMMIT' or decision == 'ABORT', 'invalid decision')
        participant_certificate(id, 'FINISH_SCOPE', members, scope)
        participant_certificate(id, decision, members, token)
        box.atomic(function() finish_participant(id, decision) end)
        return certificate(id, 'RELEASED_' .. decision, {ctx.id})
    end

    function api.tx_release(id, decision, members, token, term)
        ctx.fence(term)
        assert(decision == 'COMMIT' or decision == 'ABORT', 'invalid decision')
        participant_certificate(id, 'COMPLETE_' .. decision, members, token)
        box.atomic(function()
            local part = box.space._cairn_parts:get{id}
            assert(part and (part[2] == 'APPLIED_' .. decision or part[2] == 'DONE_' .. decision), 'participant is not applied')
            if part[2] == 'DONE_' .. decision then return end
            local locks = box.space._cairn_locks.index.by_tx:select{id}
            for _, lock in ipairs(locks) do box.space._cairn_locks:delete{lock[1], lock[2], id} end
            local intents = box.space._cairn_intents.index.primary:select{id}
            for _, intent in ipairs(intents) do box.space._cairn_intents:delete{id, intent[2], intent[3]} end
            local tables = box.space._cairn_table_locks.index.by_tx:select{id}
            for _, lock in ipairs(tables) do box.space._cairn_table_locks:delete{lock[1], id} end
            box.space._cairn_parts:update({id}, {{'=', 2, 'DONE_' .. decision}})
        end)
        return certificate(id, 'RELEASED_' .. decision, {ctx.id})
    end

    volatile = dofile('/opt/cairn/volatile.lua') {
        ctx = ctx, api = api, verified = verified, fail = fail,
        home = home, certificate = certificate, validate = validate_certificate,
        participant_certificate = participant_certificate, representation = representation,
        spaces = spaces, timeout = timeout, finish = finish_participant, verify_receipts = verify_receipts,
    }
    for _, name in ipairs(volatile.functions) do table.insert(functions, name) end
    exports.reset_open = volatile.reset
    exports.maintain_open = volatile.maintain
    exports.functions = functions
    return exports
end
