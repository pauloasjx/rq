use rusqlite;
// use std::fs;
use std::io::{self, prelude::*, BufReader};

#[derive(Debug)]
struct Test {
    id: i32,
    description: String,
}

// enum SqliteTypes {
// Text(String),
// Int(i32),
// Float(f32),
// }
//
// struct SqliteWrapper {
// table_name: String,
// conn: rusqlite::Connection,
// }
//
// impl SqliteWrapper {
// fn new(conn: rusqlite::Connection, table_name: String) -> Self {
// Self { conn, table_name }
// }
//
// fn gen_table(&self) {}
//
// fn gen_populate(&self) {}
// }

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

fn build_table(file_path: &str) {
    let file = std::fs::File::open(file_path).unwrap();
    let reader = std::io::BufReader::new(file);

    reader.lines().

    for line in reader.lines() {
        println!("{}", line.unwrap());
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        return;
    }

    let query = &args[1];

    println!("{}", query);
    let tables = find_tables(query);
    let sample_table = tables.first().unwrap();
    println!("{}", sample_table);
    build_table(sample_table);
    // let data = fs::read_to_string(query).expect("Unable to read file");
    // println!("{}", data);
}

fn _main() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();

    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, description TEXT NOT NULL)",
        [],
    )
    .unwrap();

    for n in 1..101 {
        let test = Test {
            id: 0,
            description: format!("test {}", n).to_string(),
        };
        conn.execute(
            "INSERT INTO test (description) VALUES (?1)",
            rusqlite::params![test.description],
        )
        .unwrap();
    }

    let mut stmt = conn.prepare("SELECT * FROM test;").unwrap();
    let test_iter = stmt
        .query_map([], |row| {
            Ok(Test {
                id: row.get(0).unwrap(),
                description: row.get(1).unwrap(),
            })
        })
        .unwrap();

    for test in test_iter {
        println!("{:?}", test.unwrap());
    }
}
