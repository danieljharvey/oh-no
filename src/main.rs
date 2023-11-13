use rocksdb::{Options, DB};
use serde_json::Value;

fn main() {}

struct Select {
    table: String,
}

struct Insert {
    table: String,
    key: i32,
    value: Value,
}

fn select(db: &DB, select: Select) -> Vec<(usize, Value)> {
    let prefix = format!("{}_", select.table);
    println!("Selecting {}", prefix);
    let iter = db.prefix_iterator(prefix);
    let mut results = vec![];
    for (index, item) in iter.enumerate() {
        let (_key, value) = item.unwrap();
        let val_string = std::str::from_utf8(&value).unwrap();
        let json_value = serde_json::from_str(val_string).unwrap();
        results.push((index + 1, json_value));
    }
    results
}

fn insert(db: &DB, insert: Insert) {
    let key = format!("{}_{}", insert.table, insert.key);
    db.put(key, serde_json::to_string(&insert.value).unwrap());
}

#[test]
fn test_get_users() {
    let path = format!("./test_storage{}", rand::random::<i32>());
    {
        let db = DB::open_default(path.clone()).unwrap();

        insert(
            &db,
            Insert {
                table: "user".to_string(),
                key: 1,
                value: serde_json::from_str("{\"name\":\"Egg\"}").unwrap(),
            },
        );
        insert(
            &db,
            Insert {
                table: "user".to_string(),
                key: 2,
                value: serde_json::from_str("{\"name\":\"Horse\"}").unwrap(),
            },
        );
        insert(
            &db,
            Insert {
                table: "user".to_string(),
                key: 3,
                value: serde_json::from_str("{\"name\":\"Log\"}").unwrap(),
            },
        );

        let expected = vec![
            (1, serde_json::from_str("{\"name\":\"Egg\"}").unwrap()),
            (2, serde_json::from_str("{\"name\":\"Horse\"}").unwrap()),
            (3, serde_json::from_str("{\"name\":\"Log\"}").unwrap()),
        ];

        assert_eq!(
            select(
                &db,
                Select {
                    table: "user".to_string()
                }
            ),
            expected
        );
    }
    let _ = DB::destroy(&Options::default(), path);
}
