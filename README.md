# Apache Log Parser

The project provides an Apache log parser using Rust futures to store logs within a Postgres database.

It expects that the logs in a file are in the [Combined Log Format](http://httpd.apache.org/docs/current/logs.html#combined)

## Usage


After running `cargo install` to create a binary called `apache_log`, usage is from the command line:

```
USAGE:
    apache_log [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c <db_conn>         Url for postgres connection [default: postgres://logs:logs@127.0.0.1]
    -f <filename>        Filename of the Access log [default: access_log]
    -m <mode>            Mode: either (p)arallel for multi-threaded or (s)erial [default: p]
```

You will need a postgres database and user.  The table is created automatically by `builder.rs` but requires that the URL for the db connection is in working order.  The default is localhost with a user `logs`, password `logs` and db `logs`.


The filename defaults to `access_log` in the current directory but can be overridden by the `-f` option.

If you need to ingest more files than one, you can use the following command to run this once per file:

```
find . -name "access_log*" -exec apache_log -f {} \;
```

## Todo

* Provide a configurable log file format which allows this to be used with other log formats.

## Benchmarks

Running the code in parallel is the fastest. on a Macbook Pro it roughly can do around 6000 lines per second

Running in serial:

```
cargo run --release -- -m s
   Compiling apache_log v0.2.0 (file:///Users/cetra/Desktop/apache_log)
    Finished release [optimized] target(s) in 6.71 secs
     Running `target/release/apache_log -m s`
Processing 'access_log' in serial
Number of lines: 692434, number of batches: 70, total time: 48s, lines per second: 14425
```

~14k per second

Running in parallel:

```
cargo run --release
    Finished release [optimized] target(s) in 0.0 secs
     Running `target/release/apache_log`
Processing 'access_log' in parallel
Number of lines: 692434, number of batches: 70, total time: 18s, lines per second: 38468
```

~38k per second
