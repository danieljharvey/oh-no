use rocksdb::{Options,DB};

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

fn select(db: &DB, select:Select)-> Vec<(usize,String)> {
    let prefix = format!("{}_",select.table);
    println!("Selecting {}",prefix);
    let iter = db.prefix_iterator(prefix);
    let mut results = vec![];
    for (index,item) in iter.enumerate() {
        let (_key, value) = item.unwrap();
        let val_string: String = std::str::from_utf8(&value).unwrap().to_string();
        results.push((index + 1,val_string));
    }
    results
}

fn insert(db: &DB, insert: Insert) {
    let key = format!("{}_{}",insert.table,insert.key);
    db.put(key,insert.value);
}

#[test]
fn test_get_users() {
   let path = format!("./test_storage{}",rand::random::<i32>());
    {
   let db = DB::open_default(path.clone()).unwrap();

         insert(&db,Insert { table: "user".to_string(), key: 1, value: "Egg".to_string() });
        insert(&db,Insert { table: "user".to_string(), key: 2, value: "Horse".to_string() });
        insert(&db,Insert { table: "user".to_string(), key: 3, value: "Log".to_string() });

    let expected = vec![(1,"Egg".to_string()),
        (2,"Horse".to_string()),(3, "Log".to_string())];

    assert_eq!(select(&db, Select { table: "user".to_string() }), expected);
    }
   let _ = DB::destroy(&Options::default(), path);

}
