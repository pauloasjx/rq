use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{self};
use std::fmt;
use std::io::prelude::*;
use std::iter::Iterator;

#[derive(Debug)]
pub enum RQType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

impl fmt::Display for RQType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                RQType::Null | RQType::Text => "TEXT",
                RQType::Integer => "INTEGER",
                RQType::Real => "REAL",
                RQType::Blob => "BLOB",
            }
        )
    }
}

#[derive(Debug)]
pub struct RQColumn {
    column_name: String,
    column_type: RQType,
}

impl RQColumn {
    pub fn new(column_name: String, column_type: RQType) -> Self {
        RQColumn {
            column_name,
            column_type,
        }
    }
}

#[derive(Debug)]
pub struct RQTable {
    file_path: String,
    table_name: String,
    table_columns: Vec<RQColumn>,
}

impl RQTable {
    pub fn new(file_path: String) -> Self {
        let table_name = format!("t_{:x}", md5::compute(&file_path));
        Self {
            file_path,
            table_name,
            table_columns: vec![],
        }
    }

    fn check_header(&self, header_cols: &[&str]) -> bool {
        header_cols
            .iter()
            .any(|h| h.trim().chars().any(|c| c == ' ' || c == '\n'))
    }

    fn infer_columns(&mut self) {
        let file = std::fs::File::open(self.file_path.to_string()).unwrap();
        let reader = std::io::BufReader::new(file);

        let mut lines_iter = reader.lines();
        let header: Vec<RQColumn> = lines_iter
            .next()
            .unwrap()
            .unwrap()
            .split(',')
            .map(|c| RQColumn {
                column_name: String::from(c),
                column_type: RQType::Null,
            })
            .collect();

        let result = lines_iter.fold(header, |last, current| {
            let cc: Vec<RQColumn> = current
                .unwrap()
                .trim()
                .split(',')
                .map(|s: &str| s.trim_matches(|c: char| c == '"' || c == '\'').trim())
                .zip(last.iter())
                .map(|(c, l)| {
                    let cn = if c == "Null" {
                        RQType::Null
                    } else if c.parse::<i64>().is_ok() {
                        RQType::Integer
                    } else if c.parse::<f64>().is_ok() {
                        RQType::Real
                    } else {
                        RQType::Text
                    };

                    let column_type = match &l.column_type {
                        RQType::Text => RQType::Text,
                        RQType::Integer => match cn {
                            RQType::Text => RQType::Text,
                            RQType::Real => RQType::Real,
                            _ => RQType::Integer,
                        },
                        RQType::Real => match cn {
                            RQType::Text => RQType::Text,
                            _ => RQType::Real,
                        },
                        RQType::Null => cn,
                        RQType::Blob => RQType::Blob,
                    };

                    RQColumn {
                        column_name: String::from(&l.column_name),
                        column_type,
                    }
                })
                .collect();

            if cc.len() < last.len() {
                last
            } else {
                cc
            }
        });

        self.table_columns = result;
    }

    fn create_table(&self, conn: &rusqlite::Connection) {
        let mut create_query = String::new();
        create_query.push_str(&format!(
            "CREATE TABLE {} (id INTEGER PRIMARY KEY",
            self.table_name
        ));

        // let header_cols = header.split(',').collect();
        // if self.check_header(&header_cols) {
        // panic!("Invalid header");
        // }

        // println!("{:#?}", self.table_columns);

        for table_column in &self.table_columns {
            create_query.push_str(&format!(
                ", \"{}\" {} NOT NULL",
                table_column.column_name, table_column.column_type
            ));
        }
        create_query.push(')');
        conn.execute(&create_query, []).unwrap();
    }

    fn build_table(&mut self, conn: &rusqlite::Connection) {
        let file = std::fs::File::open(self.file_path.to_string()).unwrap();
        let reader = std::io::BufReader::new(file);

        self.infer_columns();
        self.create_table(conn);

        let mut lines_iter = reader.lines();
        let header = lines_iter.next().unwrap().unwrap();

        for line in lines_iter.flatten() {
            if line != *"" {
                self.insert_row(conn, &header, line);
            }
        }
    }

    fn insert_row(&self, conn: &rusqlite::Connection, header: &str, row: String) {
        let mut insert_query = String::new();
        insert_query.push_str(&format!("INSERT INTO {} (", self.table_name));
        let header_split: Vec<&str> = header.split(',').collect();
        for (header_pos, header_col) in header_split.iter().enumerate() {
            insert_query.push_str(&format!("\"{}\"", header_col));
            if header_pos < header_split.len() - 1 {
                insert_query.push(',');
            }
        }
        insert_query.push_str(") VALUES (");
        let row_split: Vec<&str> = row.split(',').collect();
        for (row_pos, row_col) in row_split.iter().enumerate() {
            insert_query.push_str(&format!("\"{}\"", row_col.replace("\"", "'")));
            if row_pos < row_split.len() - 1 {
                insert_query.push(',');
            }
        }
        insert_query.push_str(");");
        println!("{}", insert_query);
        conn.execute(&insert_query, []).unwrap();
    }
}

struct RQDatabase {
    conn: rusqlite::Connection,
    tables: Vec<RQTable>,
}

impl RQDatabase {
    fn from_query(conn: rusqlite::Connection, query: &str) -> Self {
        let db_files = Self::find_tables(query);
        let mut tables = db_files
            .iter()
            .map(|t| RQTable::new(t.to_string()))
            .collect::<Vec<RQTable>>();

        tables.iter_mut().for_each(|t| t.build_table(&conn));

        RQDatabase { conn, tables }
    }

    fn find_tables(query: &str) -> Vec<&str> {
        let (_, tables) =
            query
                .split_whitespace()
                .fold(("", Vec::new()), |(last, mut result), current| {
                    match last {
                        "from" | "join" => {
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
        println!();

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
                        rusqlite::types::Value::Real(value) => {
                            print!("{}\t", value);
                        }
                        otherwise => {
                            print!("{:?}\t", otherwise);
                        }
                    }
                });
                println!()
            })
            .unwrap();
    }
}

fn usage() {
    println!("Usage ./rq <query>");
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
    database.run_query(query);
}
