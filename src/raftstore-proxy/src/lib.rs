#![feature(drain_filter)]

use crate::proxy::proxy_version_info;
use std::os::raw::{c_char, c_int};
use tikv_util::*;

pub mod engine_store_ffi;
mod lock_cf_reader;
mod proxy;
mod run;
mod rawserver;

fn log_proxy_info() {
    info!("Welcome To RaftStore Proxy");
    for line in proxy_version_info().lines() {
        info!("{}", line);
    }
}

pub fn print_proxy_version() {
    println!("{}", proxy_version_info());
}

/// # Safety
/// Print version infomatin to std output.
#[no_mangle]
pub unsafe extern "C" fn print_raftstore_proxy_version() {
    print_proxy_version();
}

/// # Safety
/// Please make sure such function will be run in an independent thread. Usage about interfaces can be found in `struct EngineStoreServerHelper`.
#[no_mangle]
pub unsafe extern "C" fn run_raftstore_proxy_ffi(
    argc: c_int,
    argv: *const *const c_char,
    helper: *const u8,
) {
    log_proxy_info();
    proxy::run_proxy(argc, argv, helper);
}
