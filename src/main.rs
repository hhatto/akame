#[macro_use]
extern crate lazy_static;

use chrono::{DateTime, NaiveDateTime, Utc};
use redis::FromRedisValue;
use redis::InfoDict;
use std::collections::HashMap;
use std::time::Duration;

lazy_static! {
    static ref IGNORE_COMMANDS: Vec<&'static str> = vec!["SLOWLOG", "INFO"];
}

#[derive(Default, Debug)]
struct RedisVersion {
    major: usize,
    minor: usize,
    patch: usize,
}

fn get_version(conn: &redis::Connection) -> Option<RedisVersion> {
    let info: InfoDict = redis::cmd("INFO")
        .arg("server")
        .query(conn)
        .expect("fail info command");
    let version_str = info.get("redis_version").unwrap_or_else(|| "".to_string());
    if version_str.is_empty() {
        None
    } else {
        let v: Vec<&str> = version_str.split('.').collect();
        let version = RedisVersion {
            major: v[0].parse::<usize>().expect("invalid major version"),
            minor: v[1].parse::<usize>().expect("invalid minor version"),
            patch: v[2].parse::<usize>().expect("invalid patch version"),
        };
        Some(version)
    }
}

#[derive(Default, Debug)]
struct RedisSlowlog {
    id: u64,
    timestamp: u64,
    exec_time: Duration,
    cmd: Vec<String>,
    address: String,     // support by Redis 4.0 or greater
    client_name: String, // support by Redis 4.0 or greater
}

fn get_slowlogs(conn: &redis::Connection, num: usize, version: usize) -> Vec<RedisSlowlog> {
    let mut slowlogs: Vec<RedisSlowlog> = vec![];
    let raw_slowlogs: Vec<redis::Value> = redis::cmd("SLOWLOG")
        .arg("GET")
        .arg(format!("{}", num))
        .query(conn)
        .expect("fail slowlog command");
    for raw_slowlog in raw_slowlogs.iter() {
        let slowlog = if version >= 4 {
            let s: (u64, u64, u64, Vec<String>, String, String) =
                FromRedisValue::from_redis_value(raw_slowlog).unwrap();
            RedisSlowlog {
                id: s.0,
                timestamp: s.1,
                exec_time: Duration::from_micros(s.2),
                cmd: s.3,
                address: s.4,
                client_name: s.5,
            }
        } else {
            let s: (u64, u64, u64, Vec<String>) =
                FromRedisValue::from_redis_value(raw_slowlog).unwrap();
            RedisSlowlog {
                id: s.0,
                timestamp: s.1,
                exec_time: Duration::from_micros(s.2),
                cmd: s.3,
                ..RedisSlowlog::default()
            }
        };
        if IGNORE_COMMANDS.contains(&slowlog.cmd[0].to_uppercase().as_str()) {
            continue;
        }
        slowlogs.push(slowlog);
    }
    slowlogs
}

fn main() {
    let client = redis::Client::open("redis://127.0.0.1").expect("fail connect redis");
    let conn = client
        .get_connection()
        .expect("fail to get redis connection");
    let redis_version = get_version(&conn);
    match redis_version {
        Some(ref v) => println!("redis version: {}.{}.{}", v.major, v.minor, v.patch),
        None => println!("redis version: unknown"),
    }

    let redis_version_major = match redis_version {
        Some(v) => v.major,
        None => 0,
    };

    let mut all_slowlogs: HashMap<u64, RedisSlowlog> = HashMap::new();
    loop {
        let slowlogs = get_slowlogs(&conn, 100, redis_version_major);
        for slowlog in slowlogs {
            if !all_slowlogs.contains_key(&slowlog.id) {
                let ndt = NaiveDateTime::from_timestamp_opt(slowlog.timestamp as i64, 0);
                if ndt.is_none() {
                    continue;
                }
                let dt = DateTime::<Utc>::from_utc(ndt.unwrap(), Utc);
                println!(
                    "[{:?}] id={}, time={:.1}[ms], cmd='{:?}', address={}, name={}",
                    dt,
                    slowlog.id,
                    slowlog.exec_time.subsec_nanos() as f64 * 1e-6,
                    slowlog.cmd,
                    slowlog.address,
                    slowlog.client_name
                );
                all_slowlogs.insert(slowlog.id, slowlog);
            }
        }
        std::thread::sleep(Duration::from_millis(5000));
    }
}
