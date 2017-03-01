extern crate futures;
extern crate futures_cpupool;
extern crate regex;

extern crate r2d2;
extern crate r2d2_postgres;
extern crate postgres;

extern crate chrono;

extern crate clap;

mod builder;


use futures::{Future, Sink, Stream};
use regex::Regex;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};
use r2d2::Pool;
use postgres::types::ToSql;

use chrono::NaiveDateTime;


use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use std::str::FromStr;

use clap::{Arg, App};


fn main() {

    let matches = App::new("Apache Logs")
            .version("0.1.0")
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
            .get_matches();

    let db_conn = matches.value_of("db_conn").unwrap();
    let file_name = String::from(matches.value_of("filename").unwrap());

    let manager = PostgresConnectionManager::new(db_conn, TlsMode::None).unwrap();

    let config = r2d2::Config::default();
    let pgpool = r2d2::Pool::new(config, manager).unwrap();


    builder::build(&pgpool);

    // generate a thread pool
    let pool = futures_cpupool::Builder::new()
        .name_prefix("pool-")
        .create();

    // produce input
    let (tx, rx) = futures::sync::mpsc::channel(64);
    let handle = std::thread::Builder::new()
        .name("source".to_string())
        .spawn(move || {
            consumer(tx, file_name);
        })
        .unwrap();


    let re = Regex::new("^([0-9.]+)\\s([\\w-]+)\\s([\\w-]+)\\s\\[([^\\]]+)\\]\\s\"([^\"]+)\"\\s(\\d+)\\s([\\d-]+)\\s\"([^\"]+)\"\\s\"([^\"]+)\"").unwrap();

    // process input
    let rx = rx.map(|line| {

        let re = re.clone();
        let pgpool = pgpool.clone();

        pool.spawn_fn(move || {

            producer(pgpool, re, &line);

            Ok(())
        })
    })
    .buffered(1024);

    let number = rx.wait().count();

    handle.join().unwrap();

    println!("Number of entries:{}", number);
}

fn consumer(mut tx: futures::sync::mpsc::Sender<String>, file_name: String)  {

    let f = File::open(&file_name).unwrap();
    let f = BufReader::new(f);

    for line in f.lines() {
        tx = tx.send(line.unwrap()).wait().unwrap();
    }

}

fn producer(pool: Pool<PostgresConnectionManager>, re: Regex, line: &str) {


    let caps = re.captures(&line).expect("Could not parse line");

    let ip_address = String::from(caps.get(1).unwrap().as_str());
    let identd = String::from(caps.get(2).unwrap().as_str());
    let username = String::from(caps.get(3).unwrap().as_str());
    let time = String::from(caps.get(4).unwrap().as_str());
    let request = String::from(caps.get(5).unwrap().as_str());
    let status_code = i64::from_str(caps.get(6).unwrap().as_str()).unwrap();
    let size = i64::from_str(caps.get(7).unwrap().as_str());
    let referrer = String::from(caps.get(8).unwrap().as_str());
    let user_agent = String::from(caps.get(9).unwrap().as_str());

    let datetime = NaiveDateTime::parse_from_str(&time, "%d/%b/%Y:%H:%M:%S %z").unwrap();

    let mut params: Vec<&ToSql> = Vec::new();
    let mut columns: Vec<&str> = Vec::new();

    columns.push("ip_address");
    params.push(&ip_address);

    columns.push("identd");
    params.push(&identd);

    columns.push("username");
    params.push(&username);

    columns.push("time");
    params.push(&datetime);

    columns.push("request");
    params.push(&request);

    columns.push("status_code");
    params.push(&status_code);

    if let Ok(ref parsed_size) = size {
        columns.push("size");
        params.push(parsed_size);
    }

    columns.push("referrer");
    params.push(&referrer);

    columns.push("user_agent");
    params.push(&user_agent);

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



