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
```

You will need a postgres database and user.  The table is created automatically by `builder.rs` but requires that the URL for the db connection is in working order.  The default is localhost with a user `logs`, password `logs` and db `logs`.


The filename defaults to `access_log` in the current directory but can be overridden by the `-f` option.

If you need to ingest more files than one, you can use the following command to run this once per file:

```
find . -name "access_log*" -exec apache_log -f {} \;
```

## Todo

* Make the parsing of the log file more efficient. At the moment release on a Macbook Pro, it can ingest about 5000 records per second.  Maybe move away from regex?
* Provide a configurable log file format which allows this to be used with other log formats.

## Benchmarks

```
10:38:23-cetra@Cetras-MBP:~/Desktop/apache_log$ time cargo run --release
    Finished release [optimized] target(s) in 0.0 secs
     Running `target/release/apache_log`
Number of entries:285866

real    0m49.615s
user    3m14.670s
sys     0m22.918s
```

`285866` / `49.615` = `5761.68` logs/s
