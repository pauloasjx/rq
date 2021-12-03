use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{self};
use std::io::prelude::*;
use std::iter::Iterator;

struct RQTable {
    file_path: String,
    table_name: String,
}

impl RQTable {
    fn new(file_path: String) -> Self {
        let table_name = format!("{:x}", md5::compute(&file_path));
        Self {
            file_path,
            table_name,
        }
    }

    fn create_table(&self, conn: &rusqlite::Connection, header: &str) {
        let mut create_query = String::new();
        create_query.push_str(&format!(
            "CREATE TABLE {} (id INTEGER PRIMARY KEY",
            self.table_name
        ));
        for header_col in header.split(',') {
            create_query.push_str(&format!(", {} TEXT NOT NULL", header_col));
        }
        create_query.push(')');
        conn.execute(&create_query, []).unwrap();
    }

    fn build_table(&self, conn: &rusqlite::Connection) {
        let file = std::fs::File::open(self.file_path.to_string()).unwrap();
        let reader = std::io::BufReader::new(file);

        let mut lines_iter = reader.lines();
        let header = lines_iter.next().unwrap().unwrap();

        self.create_table(&conn, &header);

        for line in lines_iter {
            match line {
                Ok(value) => {
                    if value != "".to_string() {
                        self.insert_row(&conn, &header, value);
                    }
                }
                _ => {}
            }
        }
    }

    fn insert_row(&self, conn: &rusqlite::Connection, header: &str, row: String) {
        let mut insert_query = String::new();
        insert_query.push_str(&format!("INSERT INTO {} (", self.table_name));
        let header_split: Vec<&str> = header.split(',').collect();
        for (header_pos, header_col) in header_split.iter().enumerate() {
            insert_query.push_str(header_col);
            if header_pos < header_split.len() - 1 {
                insert_query.push_str(",");
            }
        }
        insert_query.push_str(") VALUES (");
        let row_split: Vec<&str> = row.split(',').collect();
        for (row_pos, row_col) in row_split.iter().enumerate() {
            insert_query.push_str(row_col);
            if row_pos < row_split.len() - 1 {
                insert_query.push_str(",");
            }
        }
        insert_query.push_str(");");
        conn.execute(&insert_query, []).unwrap();
        ()
    }
}

struct RQDatabase {
    conn: rusqlite::Connection,
    tables: Vec<RQTable>,
}

impl RQDatabase {
    fn from_query(conn: rusqlite::Connection, query: &str) -> Self {
        let db_files = Self::find_tables(query);
        let tables = db_files
            .iter()
            .map(|t| RQTable::new(t.to_string()))
            .collect::<Vec<RQTable>>();
        tables.iter().for_each(|t| t.build_table(&conn));

        RQDatabase { conn, tables }
    }

    fn find_tables(query: &str) -> Vec<&str> {
        let (_, tables) =
            query
                .split_whitespace()
                .fold(("", Vec::new()), |(last, mut result), current| {
                    match last {
                        "from" => {
                            result.push(current);
                        }
                        _ => {}
                    }
                    (current, result)
                });

        tables
    }

    fn run_query(&self, query: &str) {
        let mut result_query = query.to_string();
        for table in self.tables.iter() {
            result_query = result_query.replace(&table.file_path, &table.table_name);
        }

        let mut stmt = self.conn.prepare(&result_query).unwrap();
        for col in stmt.column_names() {
            print!("{}\t", col);
        }
        print!("\n");

        let ncols = stmt.column_count();
        stmt.query([])
            .unwrap()
            .for_each(|row| {
                (0..ncols).for_each(|ncol| {
                    let cell = row.get::<_, rusqlite::types::Value>(ncol).unwrap();
                    match cell {
                        rusqlite::types::Value::Text(value) => {
                            print!("\"{}\"\t", value);
                        }
                        rusqlite::types::Value::Integer(value) => {
                            print!("{}\t", value);
                        }
                        otherwise => {
                            print!("{:?}\t", otherwise);
                        }
                    }
                });
                print!("\n")
            })
            .unwrap();
    }
}

fn usage() {
    print!("Usage ./rq <query>\n");
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        return;
    }

    let query = &args[1];
    let conn = rusqlite::Connection::open_in_memory().unwrap();

    let database = RQDatabase::from_query(conn, query);
    database.run_query(&query);
}
