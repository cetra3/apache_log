extern crate futures;
extern crate futures_cpupool;

extern crate r2d2;
extern crate r2d2_postgres;
extern crate postgres;

extern crate chrono;

extern crate clap;

extern crate num_cpus;

extern crate pom;


use pom::DataInput;
use pom::parser::*;




mod builder;

mod iter;

use futures::Stream;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};
use r2d2::Pool;
use postgres::types::ToSql;

use chrono::NaiveDateTime;


use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use std::str::FromStr;

use clap::{Arg, App};


struct ApacheLog {
    ip_address: String,
    identd: String,
    username: String,
    time: NaiveDateTime,
    request: String,
    status_code: i64,
    size: Option<i64>,
    referrer: String,
    user_agent: String
}



fn main() {

    let matches = App::new("Apache Logs")
            .version("0.2.0")
            .author("Cetra Free")
            .about("Parses Apache logs, putting them in a database table")
            .arg(Arg::with_name("db_conn")
                    .help("Url for postgres connection")
                    .short("c")
                    .takes_value(true)
                    .default_value("postgres://logs:logs@127.0.0.1"))
            .arg(Arg::with_name("filename")
                    .help("Filename of the Access log")
                    .short("f")
                    .takes_value(true)
                    .default_value("access_log"))
            .arg(Arg::with_name("mode")
                    .help("Mode: either (p)arallel for multi-threaded or (s)erial")
                    .short("m")
                    .takes_value(true)
                    .default_value("p"))
            .get_matches();

    let db_conn = matches.value_of("db_conn").unwrap();
    let file_name = String::from(matches.value_of("filename").unwrap());

    let manager = PostgresConnectionManager::new(db_conn, TlsMode::None).unwrap();

    let mode = matches.value_of("mode").unwrap();

    let config = r2d2::Config::default();
    let pgpool = r2d2::Pool::new(config, manager).unwrap();

    builder::build(&pgpool);

    let file = File::open(&file_name).unwrap();
    let reader = BufReader::new(file);

    match mode {
        "s" => {
            println!("Processing '{}' in serial", file_name);

            for line in reader.lines() {
                let pgpool = pgpool.clone();
                producer(pgpool, &line.unwrap());
            }

        },
        _ => {
            println!("Processing '{}' in parallel", file_name);

            let cpu_count = num_cpus::get();

            // generate a thread pool
            let pool = futures_cpupool::Builder::new()
                .name_prefix("pool-")
                .pool_size(cpu_count)
                .create();

            let stream = iter::iter(reader.lines());

            // process input
            let stream = stream.map(|line| {

                let pgpool = pgpool.clone();

                pool.spawn_fn(move || {

                    producer(pgpool, &line);

                    Ok(())
                })
            })
            .buffer_unordered(cpu_count * 4);

            let number = stream.wait().count();
            println!("Number of entries:{}", number);
        }
    }

}


fn producer(pool: Pool<PostgresConnectionManager>, line: &str) {

    let parser = ipaddr() + untilspace() + untilspace() + space() * betweenbrackets() + space() * betweenquotes() + untilspace() + untilspace() + space() * betweenquotes() + space() * betweenquotes();
    let mut input = DataInput::new(line.as_ref());

    let output = parser.parse(&mut input);

    if let Ok(((((((((ip_address, identd), username), time), request), status_code), raw_size), referrer), user_agent)) = output {

        let size = match i64::from_str(&raw_size) {
            Ok(parse_size) => Some(parse_size),
            _ => None
        };

        let log = ApacheLog {
            ip_address: ip_address,
            identd: identd,
            username: username,
            time: NaiveDateTime::parse_from_str(&time, "%d/%b/%Y:%H:%M:%S %z").unwrap(),
            request: request,
            status_code: i64::from_str(&status_code).unwrap(),
            size: size,
            referrer: referrer,
            user_agent: user_agent,
        };

        let mut params: Vec<&ToSql> = Vec::new();
        let mut columns: Vec<&str> = Vec::new();

        columns.push("ip_address");
        params.push(&log.ip_address);

        columns.push("identd");
        params.push(&log.identd);

        columns.push("username");
        params.push(&log.username);

        columns.push("time");
        params.push(&log.time);

        columns.push("request");
        params.push(&log.request);

        columns.push("status_code");
        params.push(&log.status_code);

        if let Some(ref parsed_size) = log.size {
            columns.push("size");
            params.push(parsed_size);
        }

        columns.push("referrer");
        params.push(&log.referrer);

        columns.push("user_agent");
        params.push(&log.user_agent);

        let mut query = String::new();
        query.push_str("INSERT INTO logs(");

        query.push_str(&columns.join(", "));

        query.push_str(") values (");
        query.push_str(&(1..params.len() + 1).map(|num| format!("${}", num)).collect::<Vec<String>>().join(","));
        query.push_str(")");

        if let Ok(conn) = pool.get() {
            conn.execute(&query, &params).unwrap();
        }
    }
}


fn ipaddr<'a>() -> Parser<'a, u8, String> {
    one_of(b"1234567890.").repeat(0..).collect().convert(String::from_utf8)
}

fn space<'a>() -> Parser<'a, u8, ()> {
    one_of(b" \t\r\n").repeat(0..).discard()
}

fn untilspace<'a>() -> Parser<'a, u8, String> {

    let value = space() * none_of(b" ").repeat(0..);

    value.convert(String::from_utf8)
}

fn betweenbrackets<'a>() -> Parser<'a, u8, String> {

    let value = sym(b'[') * none_of(b"]").repeat(0..) - sym(b']').discard();

    value.convert(String::from_utf8)
}


fn betweenquotes<'a>() -> Parser<'a, u8, String> {
    let value = sym(b'"') * none_of(b"\"").repeat(0..) - sym(b'"').discard();

    value.convert(String::from_utf8)
}

