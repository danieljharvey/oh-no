use rocksdb::DB;

fn main() {
    // NB: db is automatically closed at end of lifetime
    let path = "./db_storage";
    {
        let db = DB::open_default(path).unwrap();
        insert(&db,Insert { table: "loser".to_string(), key: 1, value: "Egg".to_string() });
        insert(&db,Insert { table: "loser".to_string(), key: 2, value: "Horse".to_string() });
        insert(&db,Insert { table: "loser".to_string(), key: 3, value: "Log".to_string() });

        insert(&db,Insert { table: "user".to_string(), key: 1, value: "Egg".to_string() });
        insert(&db,Insert { table: "user".to_string(), key: 2, value: "Horse".to_string() });
        insert(&db,Insert { table: "user".to_string(), key: 3, value: "Log".to_string() });

        insert(&db,Insert { table: "boozer".to_string(), key: 1, value: "Egg".to_string() });
        insert(&db,Insert { table: "boozer".to_string(), key: 2, value: "Horse".to_string() });
        insert(&db,Insert { table: "boozer".to_string(), key: 3, value: "Log".to_string() });

        select(&db,Select { table: "user".to_string() });

        select(&db,Select { table: "ccc".to_string()});
    }
    //    let _ = DB::destroy(&Options::default(), path);
}

struct Select {
    table: String
}

struct Insert {
    table: String
    , key: i32,
     value: String
}

fn select(db: &DB, select:Select) {
    let prefix = format!("{}_",select.table);
    println!("Selecting {}",prefix);
    let iter = db.prefix_iterator(prefix);
    for item in iter {
        let (key, value) = item.unwrap();
        println!("Saw {:?} {:?}", std::str::from_utf8(&key), std::str::from_utf8(&value));
    }
}

fn insert(db: &DB, insert: Insert) {
    let key = format!("{}_{}",insert.table,insert.key);
    db.put(key,insert.value);
}
