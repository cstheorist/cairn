-- Native tuples are the source of truth for SQL and iproto reads.
local ffi = require('ffi')
local json = require('json')
local shard_id = assert(tonumber(os.getenv('CAIRN_SHARD_ID')))
local shard_count = assert(tonumber(os.getenv('CAIRN_SHARD_COUNT')))
assert(shard_id >= 0 and shard_id < shard_count and shard_count <= 1024)
-- The persisted topology fixes this divisor for the lifetime of the process.
local shard_count_u64 = ffi.new('uint64_t', shard_count)
local gateway_password = assert(os.getenv('CAIRN_GATEWAY_PASSWORD'))
local reader_password = assert(os.getenv('CAIRN_READER_PASSWORD'))
local managed = os.getenv('CAIRN_MANAGED_CONFIG') == '1'
local fiber = require('fiber')
local initialized_term = nil
local incarnation = 0ULL
local transactions
local api_names = {'ping', 'table_schema', 'create_table', 'get', 'get_many', 'insert', 'delete', 'update'}

if not managed then box.cfg {
    listen = 3301,
    wal_mode = 'fsync',
    memtx_use_mvcc_engine = true,
    log_level = 4,
} end

local function initialize()
if managed and not box.space._cairn_catalog then
    -- Do not introduce synchronous transactions into an unfinished initial join:
    -- a follower must finish its snapshot before it can acknowledge a quorum WAL.
    local followers = 0
    for id, peer in pairs(box.info.replication) do
        if id ~= box.info.id and peer.downstream and peer.downstream.status == 'follow' then
            followers = followers + 1
        end
    end
    assert(followers >= 1, 'waiting for an initial synchronous follower')
end
if not managed then
box.schema.user.create('gateway', {password = gateway_password, if_not_exists = true})
box.schema.user.create('reader', {password = reader_password, if_not_exists = true})
box.schema.user.passwd('gateway', gateway_password)
box.schema.user.passwd('reader', reader_password)
end

box.schema.space.create('_cairn_topology', {if_not_exists = true, is_sync = managed})
box.space._cairn_topology:create_index('primary', {parts = {{1, 'string'}}, if_not_exists = true})
local previous = box.space._cairn_topology:get{'topology'}
if previous then
    assert(previous[2] == shard_id and previous[3] == shard_count,
           'persisted topology differs; resharding is not implemented')
else
    box.space._cairn_topology:insert{'topology', shard_id, shard_count}
end

box.schema.space.create('_cairn_catalog', {if_not_exists = true, is_sync = managed})
box.space._cairn_catalog:create_index('primary', {parts = {{1, 'string'}}, if_not_exists = true})
transactions.initialize()
for _, name in ipairs(api_names) do
    local qualified = 'cairn.' .. name
    box.schema.func.create(qualified, {setuid = true, if_not_exists = true})
    box.schema.user.grant('gateway', 'execute', 'function', qualified, {if_not_exists = true})
    if name == 'get' or name == 'get_many' or name == 'ping' then
        box.schema.user.grant('reader', 'execute', 'function', qualified, {if_not_exists = true})
    end
end
-- A synchronous barrier also covers preceding asynchronous system-space DDL.
incarnation = box.atomic(function()
    local prior = box.space._cairn_topology:get{'incarnation'}
    local next_value = transactions.next_incarnation(prior and prior[2] or 0ULL)
    box.space._cairn_topology:replace{'incarnation', next_value}
    return next_value
end)
transactions.reset_open()
initialized_term = managed and box.info.election.term or 0
end

cairn = {}
function cairn.ping()
    local term = managed and box.info.election.term or 0
    return shard_id, shard_count, term, not box.info.ro and initialized_term == term,
           managed, managed and box.cfg.replication_synchro_quorum or 1, box.info.uuid, incarnation
end
local schemas = {}

local function fence(expected_term)
    local term = managed and box.info.election.term or 0
    if box.info.ro or initialized_term ~= term or (managed and expected_term ~= term) then
        error('[CAIRN:FENCED] request rejected before execution: leadership epoch changed', 0)
    end
end

local function read_confirmed(expected_term, read)
    fence(expected_term)
    if not managed then return box.atomic({txn_isolation = 'read-confirmed'}, read) end
    box.begin{txn_isolation = 'linearizable', timeout = 2}
    local ok, result = pcall(read)
    if not ok then box.rollback(); error(result, 0) end
    box.commit()
    return result
end

local function name_ok(name)
    assert(type(name) == 'string' and #name <= 63 and name:match('^[a-z][a-z0-9_]*$'), 'invalid table name')
    assert(not name:match('^_') and not name:match('^cairn'), 'reserved table name')
end

local function get_schema(name)
    name_ok(name)
    if schemas[name] then return schemas[name] end
    local row = box.space._cairn_catalog:get{name}
    assert(row, 'table does not exist')
    schemas[name] = json.decode(row[2])
    return schemas[name]
end

local function check_owner(key)
    local normalized = ffi.cast('uint64_t', ffi.cast('int64_t', key)) + 0x8000000000000000ULL
    local owner = tonumber(normalized % shard_count_u64)
    assert(owner == shard_id, 'wrong shard for primary key')
end

function cairn.table_schema(name, expected_term)
    return read_confirmed(expected_term, function()
        name_ok(name)
        local row = box.space._cairn_catalog:get{name}
        if row then return row[2] end
    end)
end

function cairn.create_table(encoded, if_not_exists, expected_term)
    fence(expected_term)
    local schema = json.decode(encoded)
    name_ok(schema.name)
    assert(#schema.columns > 0 and #schema.columns <= 128, 'invalid column count')
    assert(schema.primary_key >= 0 and schema.primary_key < #schema.columns, 'invalid primary key')
    local existing = box.space._cairn_catalog:get{schema.name}
    if existing then
        assert(if_not_exists, 'table already exists; retry partial DDL with IF NOT EXISTS')
        assert(existing[2] == encoded, 'schema mismatch; IF NOT EXISTS requires an identical schema')
        box.schema.user.grant('reader', 'read', 'space', schema.name, {if_not_exists = true})
        return true
    end
    assert(not box.space[schema.name], 'space exists outside the cairn catalog')
    local format = {}
    local names = {}
    for i, column in ipairs(schema.columns) do
        assert(type(column.name) == 'string' and column.name:match('^[a-z][a-z0-9_]*$'), 'invalid column')
        assert(not names[column.name], 'duplicate column')
        names[column.name] = true
        local types = {bigint = 'integer', integer = 'integer', smallint = 'integer', text = 'string', boolean = 'boolean'}
        assert(types[column.kind], 'invalid type')
        format[i] = {name = column.name, type = types[column.kind], is_nullable = column.nullable}
    end
    local pk = schema.primary_key + 1
    assert(({bigint = true, integer = true, smallint = true})[schema.columns[pk].kind] and not schema.columns[pk].nullable, 'integer PRIMARY KEY required')
    -- DDL/catalog publish is atomic on one shard; it is explicitly not cluster-atomic.
    box.atomic(function()
        local space = box.schema.space.create(schema.name, {format = format, field_count = #format, is_sync = managed})
        space:create_index('primary', {type = 'TREE', parts = {{pk, 'integer'}}})
        box.space._cairn_catalog:insert{schema.name, encoded}
    end)
    box.schema.user.grant('reader', 'read', 'space', schema.name, {if_not_exists = true})
    schemas[schema.name] = schema
    return true
end

function cairn.get(name, key, expected_term)
    return read_confirmed(expected_term, function()
        get_schema(name)
        check_owner(key)
        transactions.check_committed_point(name, key)
        local row = box.space[name]:get{key}
        if row then return row:totable() end
        return {}
    end)
end

function cairn.get_many(requests, expected_term)
    assert(type(requests) == 'table' and #requests > 0 and #requests <= 64, 'expected 1..64 point reads')
    return read_confirmed(expected_term, function()
        local rows, bytes = {}, 0
        for _, request in ipairs(requests) do
            assert(type(request) == 'table' and #request == 2, 'invalid point read')
            local name, key = request[1], request[2]
            get_schema(name); check_owner(key)
            transactions.check_committed_point(name, key)
            local row = box.space[name]:get{key}
            bytes = bytes + (row and row:bsize() + #row * 8 or 1) + #name + 32
            if bytes > 8 * 1024 * 1024 - 4096 then error('[CAIRN:LIMIT] read batch exceeds response budget', 0) end
            table.insert(rows, {name, key, row and row:totable() or {}})
        end
        return rows
    end)
end

function cairn.insert(name, row, expected_term)
    fence(expected_term)
    local schema = get_schema(name)
    check_owner(row[schema.primary_key + 1])
    return box.atomic(function()
        transactions.check_unlocked(name, row[schema.primary_key + 1], nil, true)
        return box.space[name]:insert(row):totable()
    end)
end

function cairn.delete(name, key, expected_term)
    fence(expected_term)
    get_schema(name)
    check_owner(key)
    return box.atomic(function()
        transactions.check_unlocked(name, key, nil, true)
        return box.space[name]:delete{key} ~= nil
    end)
end

function cairn.update(name, key, operations, expected_term)
    fence(expected_term)
    local schema = get_schema(name)
    check_owner(key)
    for _, operation in ipairs(operations) do
        assert(operation[1] == '=' and operation[2] ~= schema.primary_key + 1,
               'only assignments to non-key columns are supported')
    end
    return box.atomic(function()
        transactions.check_unlocked(name, key, nil, true)
        return box.space[name]:update({key}, operations) ~= nil
    end)
end

transactions = dofile('/opt/cairn/transactions.lua') {
    api = cairn, id = shard_id, count = shard_count, replicated = managed,
    fence = fence, read = read_confirmed, schema = get_schema, owner = check_owner,
    incarnation = function() return incarnation end,
}
for _, name in ipairs(transactions.functions) do table.insert(api_names, name) end

if managed then
    fiber.create(function()
        local next_maintenance = 0
        while true do
            if not box.info.ro and initialized_term ~= box.info.election.term then
                local ok, err = pcall(initialize)
                if not ok then require('log').warn('cairn initialization: %s', tostring(err)) end
            end
            if not box.info.ro and initialized_term == box.info.election.term and fiber.time() >= next_maintenance then
                local ok, err = pcall(transactions.maintain_open)
                if not ok then require('log').warn('cairn open cleanup: %s', tostring(err)) end
                next_maintenance = fiber.time() + 1
            end
            fiber.sleep(0.1)
        end
    end)
else
    initialize()
    fiber.create(function()
        while true do
            fiber.sleep(1)
            local ok, err = pcall(transactions.maintain_open)
            if not ok then require('log').warn('cairn open cleanup: %s', tostring(err)) end
        end
    end)
end
-- The reader user has only per-table read grants; neither user can EVAL or write spaces directly.
