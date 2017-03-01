use std::collections::HashSet;

use r2d2_postgres::PostgresConnectionManager;
use r2d2::Pool;

pub type PostgresPool = Pool<PostgresConnectionManager>;

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Column {
    name: String,
    data_type: DataType
}

#[derive(Debug)]
pub struct Table {
    name: String,
    columns: HashSet<Column>,
}

#[derive(Hash, Eq, PartialEq, Debug)]
#[allow(dead_code)]
pub enum DataType {
    ID,
    DATE,
    LONG,
    SMALL,
    UUID,
    DOUBLE,
    URL,
    BOOLEAN,
    TEXT,
    STRING
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Column {
        Column {
            name: String::from(name),
            data_type: data_type
        }
    }
}

impl Table {
    pub fn new(name: &str, columns: Vec<Column>) -> Table {

        let mut column_set: HashSet<Column> = HashSet::new();

        for column in columns.into_iter() {
            column_set.insert(column);
        }

        Table {
            name: String::from(name),
            columns: column_set
        }
    }
}

impl DataType {
    pub fn to_sql_type(&self) -> &str {
        match *self {
            DataType::ID => "bigserial primary key",
            DataType::DATE => "timestamp",
            DataType::LONG => "bigint",
            DataType::SMALL => "smallint",
            DataType::UUID => "uuid",
            DataType::DOUBLE => "double precision",
            DataType::URL => "varchar(2083)",
            DataType::BOOLEAN => "boolean",
            DataType::TEXT => "text",
            DataType::STRING => "varchar(255)"
        }
    }
}

pub fn build(pool: &PostgresPool) {

    let tables = vec![
        Table::new("logs",
                   vec![
                       Column::new("id", DataType::ID),
                       Column::new("ip_address", DataType::STRING),
                       Column::new("identd", DataType::STRING),
                       Column::new("username", DataType::STRING),
                       Column::new("time", DataType::DATE),
                       Column::new("request", DataType::TEXT),
                       Column::new("status_code", DataType::LONG),
                       Column::new("size", DataType::LONG),
                       Column::new("referrer", DataType::URL),
                       Column::new("user_agent", DataType::TEXT)
                   ]),
    ];

    let existing_tables = get_tables(pool);

    let conn = pool.get().unwrap();

    for table in tables.iter() {

        if !existing_tables.contains(&table.name) {
            let build_query = create_table_build_query(&table);
            println!("Creating table {} with query: `{}`", table.name, build_query);
            conn.query(&build_query, &[]).unwrap();
        }

        let existing_columns = get_columns(pool, &table.name);

        for column in table.columns.iter() {
            if !existing_columns.contains(&column.name) {
                let column_build_query = create_column_build_query(&column, &table.name);
                println!("Altering table {} adding {:?} with query: `{}`", table.name, column, column_build_query);
                conn.query(&column_build_query, &[]).unwrap();
            }
        }

    }

}

pub fn create_column_build_query(column: &Column, table: &str) -> String {
    let mut query = String::new();

    query.push_str("ALTER TABLE ");
    query.push_str(table);
    query.push_str(" ADD COLUMN ");
    query.push_str(&column.name);
    query.push_str(" ");
    query.push_str(&column.data_type.to_sql_type());

    query
}

pub fn create_table_build_query(table: &Table) -> String {

    let column_str: Vec<String> = table.columns.iter()
        .map(|column| format!("{} {}", column.name, column.data_type.to_sql_type()))
        .collect();

    let mut query = String::new();

    query.push_str("CREATE TABLE ");
    query.push_str(&table.name);
    query.push_str("( ");
    query.push_str(&column_str.join(", "));
    query.push_str(" )");
    query
}

pub fn get_tables(pool: &PostgresPool) -> HashSet<String> {

    let mut tables = HashSet::new();
    let conn = pool.get().unwrap();

    let query = conn.query("select table_name from information_schema.tables where table_schema = 'public'", &[]);

    for row in &query.unwrap() {
        tables.insert(row.get(0));
    }

    tables
}

pub fn get_columns(pool: &PostgresPool, table: &str) -> HashSet<String> {

    let mut columns = HashSet::new();
    let conn = pool.get().unwrap();

    let query = conn.query("select column_name from information_schema.columns where table_name = $1", &[&table]);

    for row in &query.unwrap() {
        columns.insert(row.get(0));
    }

    columns
}
