use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{self};
use std::io::prelude::*;
use std::iter::Iterator;

fn usage() {
    print!("Usage ./rq <query>\n");
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

fn create_table(conn: &rusqlite::Connection, header: &str, table_name: &str) {
    let mut create_query = String::new();
    create_query.push_str(&format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY",
        table_name
    ));
    for header_col in header.split(',') {
        create_query.push_str(&format!(", {} TEXT NOT NULL", header_col));
    }
    create_query.push(')');
    conn.execute(&create_query, []).unwrap();
}

fn insert_row(conn: &rusqlite::Connection, header: &str, row: String, table_name: &str) {
    let mut insert_query = String::new();
    insert_query.push_str(&format!("INSERT INTO {} (", table_name));
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

fn build_table(conn: &rusqlite::Connection, file_path: &str) {
    let file = std::fs::File::open(file_path).unwrap();
    let reader = std::io::BufReader::new(file);

    let mut lines_iter = reader.lines();
    let header = lines_iter.next().unwrap().unwrap();
    let table_name = format!("{:x}", md5::compute(file_path));

    create_table(&conn, &header, &table_name);

    for line in lines_iter {
        match line {
            Ok(value) => {
                if value != "".to_string() {
                    insert_row(&conn, &header, value, &table_name);
                }
            }
            _ => {}
        }
    }
}
fn run_query(conn: &rusqlite::Connection, query: &str) {
    let mut stmt = conn.prepare(query).unwrap();
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

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        return;
    }

    let query = &args[1];
    let conn = rusqlite::Connection::open_in_memory().unwrap();

    let tables = find_tables(query);
    let sample_table = tables.first().unwrap();
    let table_name = format!("{:x}", md5::compute(sample_table));
    build_table(&conn, sample_table);
    run_query(&conn, &query.replace(sample_table, &table_name));
}
