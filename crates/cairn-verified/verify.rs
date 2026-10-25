use vstd::prelude::*;

verus! {

// Each block marked runtime is extracted verbatim by scripts/extract_core.py.
// Contracts are outside the markers; executable bodies are shared with Cargo.

pub fn normalize_key(key: i64) -> (result: u64)
    ensures result as int == key as int + 9223372036854775808int,
// runtime-begin normalize_key
{
    if key < 0 {
        (key - (-9223372036854775808i64)) as u64
    } else {
        key as u64 + 0x8000000000000000u64
    }
}
// runtime-end

pub fn shard_for_key(key: i64, count: u64) -> (result: u64)
    requires count > 0,
    ensures result < count,
            result as int == (key as int + 9223372036854775808int) % (count as int),
// runtime-begin shard_for_key
{
    normalize_key(key) % count
}
// runtime-end

pub fn valid_frame(length: u32, maximum: u32) -> (result: bool)
    ensures result == (length >= 4 && length <= maximum),
// runtime-begin valid_frame
{
    length >= 4 && length <= maximum
}
// runtime-end

pub fn payload_length(length: u32, _maximum: u32) -> (result: u32)
    requires length >= 4, length <= _maximum,
    ensures result == length - 4, result <= _maximum,
// runtime-begin payload_length
{
    length - 4
}
// runtime-end

pub fn accepts_topology(actual_id: u64, actual_count: u64, expected_id: u64, expected_count: u64) -> (result: bool)
    ensures result == (actual_id == expected_id && actual_count == expected_count && expected_count > 0),
// runtime-begin accepts_topology
{
    actual_id == expected_id && actual_count == expected_count && expected_count > 0
}
// runtime-end

pub fn accepts_leader(writable: bool, term: u64, minimum_term: u64) -> (result: bool)
    ensures result == (writable && term >= minimum_term),
            result ==> term >= minimum_term,
// runtime-begin accepts_leader
{
    writable && term >= minimum_term
}
// runtime-end

pub fn may_decide_commit(prepared: u64, participants: u64, aborted: bool) -> (result: bool)
    ensures result == (!aborted && participants > 0 && prepared == participants),
            result ==> prepared > 0 && prepared == participants && !aborted,
// runtime-begin may_decide_commit
{
    !aborted && participants > 0 && prepared == participants
}
// runtime-end

pub fn transaction_home(prefix: u16, scoped: bool) -> (result: u64)
    ensures scoped ==> result == prefix as u64,
            !scoped ==> result == 0,
// runtime-begin transaction_home
{
    if scoped {
        prefix as u64
    } else {
        0
    }
}
// runtime-end

pub fn checked_add_i64(a: i64, b: i64) -> (result: Option<i64>)
    ensures match result {
        Some(value) => (value as int) == (a as int) + (b as int),
        None => (a as int) + (b as int) < -9223372036854775808int
             || (a as int) + (b as int) > 9223372036854775807int,
    },
// runtime-begin checked_add_i64
{
    if (b > 0 && a > 9223372036854775807i64 - b) || (b < 0 && a < -9223372036854775808i64 - b) {
        None
    } else {
        Some(a + b)
    }
}
// runtime-end

pub fn checked_sub_i64(a: i64, b: i64) -> (result: Option<i64>)
    ensures match result {
        Some(value) => (value as int) == (a as int) - (b as int),
        None => (a as int) - (b as int) < -9223372036854775808int
             || (a as int) - (b as int) > 9223372036854775807int,
    },
// runtime-begin checked_sub_i64
{
    if (b > 0 && a < -9223372036854775808i64 + b) || (b < 0 && a > 9223372036854775807i64 + b) {
        None
    } else {
        Some(a - b)
    }
}
// runtime-end

pub fn may_commit_locally(participants: u64, coordinator: u64, participant: u64) -> (result: bool)
    ensures result == (participants == 1 && coordinator == participant),
            result ==> participants > 0 && coordinator == participant,
// runtime-begin may_commit_locally
{
    participants == 1 && coordinator == participant
}
// runtime-end

pub fn valid_read_batch(requests: u64, responses: u64, maximum: u64) -> (result: bool)
    ensures result == (requests > 0 && requests <= maximum && requests == responses),
            result ==> responses > 0 && responses <= maximum,
// runtime-begin valid_read_batch
{
    requests > 0 && requests <= maximum && requests == responses
}
// runtime-end

pub fn next_incarnation(current: u64) -> (result: Option<u64>)
    ensures match result {
        Some(value) => value as int == current as int + 1,
        None => current == 18446744073709551615u64,
    },
// runtime-begin next_incarnation
{
    if current == u64::MAX {
        None
    } else {
        Some(current + 1)
    }
}
// runtime-end

pub fn may_use_open_state(present: bool, current: u64, stored: u64, requested: u64) -> (result: bool)
    ensures result == (present && current == stored && stored == requested),
            result ==> present && current == requested,
// runtime-begin may_use_open_state
{
    present && current == stored && stored == requested
}
// runtime-end

}
