//! C ABI around the same extracted Verus bodies linked into the gateway.
#![no_std]
#[allow(dead_code)]
#[path = "../src/lib.rs"]
mod verified;

#[cfg(target_vendor = "apple")]
#[link(name = "System")]
extern "C" {}

#[repr(C)]
pub struct CheckedInteger {
    pub valid: i32,
    pub value: i64,
}

#[no_mangle]
pub extern "C" fn cairn_verified_abi_version() -> u32 {
    2
}

#[repr(C)]
pub struct CheckedUnsigned {
    pub valid: i32,
    pub value: u64,
}

#[no_mangle]
pub extern "C" fn cairn_next_incarnation(current: u64) -> CheckedUnsigned {
    match verified::next_incarnation(current) {
        Some(value) => CheckedUnsigned { valid: 1, value },
        None => CheckedUnsigned { valid: 0, value: 0 },
    }
}

#[no_mangle]
pub extern "C" fn cairn_may_use_open_state(present: i32, current: u64, stored: u64, requested: u64) -> i32 {
    i32::from(verified::may_use_open_state(present == 1, current, stored, requested))
}

#[no_mangle]
pub extern "C" fn cairn_checked_add_i64(a: i64, b: i64) -> CheckedInteger {
    match verified::checked_add_i64(a, b) {
        Some(value) => CheckedInteger { valid: 1, value },
        None => CheckedInteger { valid: 0, value: 0 },
    }
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    loop {
        core::hint::spin_loop();
    }
}
