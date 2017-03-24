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


static ZERO: i64 = 0;


mod builder;

mod iter;

use futures::{Future, Stream};
use futures::stream;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};
use r2d2::Pool;
use postgres::types::ToSql;

use chrono::NaiveDateTime;


use std::io::{Error, ErrorKind, BufReader};
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
                let log = producer(&line.unwrap()).unwrap();

                let logs = vec!(log);

                submitter(pgpool, logs).unwrap();

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

                pool.spawn_fn(move || {

                    producer(&line)
                })

            })
            .buffer_unordered(cpu_count * 100)
            .chunks(10000)
            .map(|chunk| {

                let pgpool = pgpool.clone();


                pool.spawn_fn(move || {
                    submitter(pgpool, chunk)
                })

            }).collect();

            /*
            We have to break up the stream here because of
                https://github.com/rust-lang/rust/issues/40003
            */

            let parse_stream = stream.wait().unwrap();

            let submission = stream::futures_unordered(parse_stream);

            let number = submission.wait().count();

            println!("Number of batches:{}", number);
        }
    }

}

fn submitter(pool: Pool<PostgresConnectionManager>, logs: Vec<ApacheLog>) -> Result<(), Error> {


    let mut columns: Vec<&str> = Vec::new();

    columns.push("ip_address");
    columns.push("identd");
    columns.push("username");
    columns.push("time");
    columns.push("request");
    columns.push("status_code");
    columns.push("size");
    columns.push("referrer");
    columns.push("user_agent");

    let mut query = String::new();
    query.push_str("INSERT INTO logs(");

    query.push_str(&columns.join(", "));

    query.push_str(") values (");
    query.push_str(&(1..columns.len() + 1).map(|num| format!("${}", num)).collect::<Vec<String>>().join(","));
    query.push_str(")");

    if let Ok(conn) = pool.get() {
        let trans = conn.transaction().unwrap();

        let stmt = trans.prepare(&query).unwrap();

        for log in logs {
            let mut params: Vec<&ToSql> = Vec::new();

            params.push(&log.ip_address);
            params.push(&log.identd);
            params.push(&log.username);
            params.push(&log.time);
            params.push(&log.request);
            params.push(&log.status_code);

            if let Some(ref parsed_size) = log.size {
                params.push(parsed_size);
            } else {
                params.push(&ZERO);
            }


            params.push(&log.referrer);


            params.push(&log.user_agent);


            stmt.execute(&params).unwrap();

        }

        trans.commit().unwrap();

        ()

    }
    Err(Error::from(ErrorKind::InvalidData))

}

fn producer(line: &str) -> Result<ApacheLog, Error> {

    let parser = ipaddr() + untilspace() + untilspace() + space() * betweenbrackets() + space() * betweenquotes() + untilspace() + untilspace() + space() * betweenquotes() + space() * betweenquotes();
    let mut input = DataInput::new(line.as_ref());

    let output = parser.parse(&mut input);

    if let Ok(((((((((ip_address, identd), username), time), request), status_code), raw_size), referrer), user_agent)) = output {

        let size = match i64::from_str(&raw_size) {
            Ok(parse_size) => Some(parse_size),
            _ => None
        };

        return Ok(ApacheLog {
            ip_address: ip_address,
            identd: identd,
            username: username,
            time: NaiveDateTime::parse_from_str(&time, "%d/%b/%Y:%H:%M:%S %z").unwrap(),
            request: request,
            status_code: i64::from_str(&status_code).unwrap(),
            size: size,
            referrer: referrer,
            user_agent: user_agent,
        });
   }

    Err(Error::from(ErrorKind::InvalidData))
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

