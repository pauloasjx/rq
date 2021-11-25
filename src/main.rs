use rusqlite;

#[derive(Debug)]
struct Test {
    id: i32,
    description: String,
}

fn main() {
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
