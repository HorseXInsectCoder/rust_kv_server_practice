pub mod memory;

use crate::errors::KvError;
use crate::{KvPair, Value};

/// 对存储的抽象，我们不关心数据存在哪儿，但需要定义外界如何和存储打交道
pub trait Storage {
    /// 从一个 HashTable 里获取一个 key 的 value
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

    /*
        set / del 明显是个会导致 self 修改的方法，为什么它的接口依旧使用的是 &self 呢？
        对于 Storage trait，最简单的实现是 in-memory 的 HashMap。由于我们支持的是 HSET / HGET 这样的命令，
        它们可以从不同的表中读取数据，所以需要嵌套的 HashMap，类似 HashMap>。
        另外，由于要在多线程 / 异步环境下读取和更新内存中的 HashMap，
        所以我们需要类似 Arc<RwLock<HashMap<String, Arc<RwLock<HashMap<String, Value>>>>>> 的结构。
        这个结构是一个多线程环境下具有内部可变性的数据结构，所以 get / set 的接口是 &self 就足够了。
    */
    /// 从一个 HashTable 里设置一个 key 的 value，返回旧的 value
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;

    /// 查看 HashTable 中是否有 key
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;

    /// 从 HashTable 中删除一个 key
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

    /// 遍历 HashTable，返回所有 kv pair（这个接口不好）
    fn get_all(&self, table: &str) -> Result<Vec<KvPair>, KvError>;

    /*
        这里我们想返回一个 iterator，调用者不关心它具体是什么类型，只要可以不停地调用 next() 方法取到下一个值就可以了。
        不同的实现，可能返回不同的 iterator，如果要用同一个接口承载，我们需要使用 trait object。在使用 trait object 时，
        因为 Iterator 是个带有关联类型的 trait，所以这里需要指明关联类型 Item 是什么类型，这样调用者才好拿到这个类型进行处理。
    */
    /// 遍历 HashTable，返回 kv pair 的 Iterator
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = KvPair>>, KvError>;

    // ----------------------

    // 实现HMGET、HMSET、HDEL、HMDEL、HEXIST、HMEXIST
    // 从 table 中获取一组 key，返回它们的 value
    fn m_get(&self, table: &str, keys: Vec<String>) -> Result<Option<Vec<Value>>, KvError>;
}

#[cfg(test)]
mod tests {
    use crate::storage::memory::MemTable;
    use super::*;

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basic_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn memtable_mget_should_work() {
        let store = MemTable::new();
        test_mget(store);
    }


    // #[test]
    // fn memtable_iter_should_work() {
    //     let store = MemTable::new();
    //     test_get_iter(store);
    // }

    fn test_basic_interface(store: impl Storage) {
        // 第一次 set 会创建 table，插入 key 并返回 None（之前没值）
        let v = store.set("table1", "hello".into(), "world".into());
        assert!(v.unwrap().is_none());
        // 再次 set 同样的 key 会更新，并返回之前的值
        let v1 = store.set("table1", "hello".into(), "world1".into());
        assert_eq!(v1, Ok(Some("world".into())));

        // get 存在的 key 会得到最新的值
        let v = store.get("table1", "hello");       // 此时key "hello"的值是 "world1"
        assert_eq!(v, Ok(Some("world1".into())));

        // get 不存在的 key 或者 table 会得到 None
        assert_eq!(Ok(None), store.get("table1", "hello1"));            // key "hello1"不存在
        assert!(store.get("table2", "hello1").unwrap().is_none());      // table "table2"不存在

        // contains 纯在的 key 返回 true，否则 false
        assert_eq!(store.contains("table1", "hello"), Ok(true));
        assert_eq!(store.contains("table1", "hello1"), Ok(false));
        assert_eq!(store.contains("table2", "hello"), Ok(false));

        // del 存在的 key 返回之前的值
        let v = store.del("table1", "hello");
        assert_eq!(v, Ok(Some("world1".into())));

        // del 不存在的 key 或 table 返回 None
        assert_eq!(Ok(None), store.del("table1", "hello1"));
        assert_eq!(Ok(None), store.del("table2", "hello"));
    }

    fn test_get_all(store: impl Storage) {
        store.set("table2", "k1".into(), "v1".into()).unwrap();
        store.set("table2", "k2".into(), "v2".into()).unwrap();
        let mut data = store.get_all("table2").unwrap();
        // 这里的比较是必须的。因为storage有可能用hashmap，而hashmap是无序的。会导致测试失败，所以必须先排好序
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(data, vec![
            KvPair::new("k1", "v1".into()),
            KvPair::new("k2", "v2".into())
        ]);
    }

    fn test_get_iter(storage: impl Storage) {
        storage.set("table2", "k1".into(), "v1".into()).unwrap();
        storage.set("table2", "k2".into(), "v2".into()).unwrap();
        let mut data: Vec<_> = storage.get_iter("table2").unwrap().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(data, vec![
            KvPair::new("k1", "v1".into()),
            KvPair::new("k2", "v2".into())
        ])
    }

    fn test_mget(store: impl Storage) {
        store.set("table2", "k1".into(), "v1".into()).unwrap();
        store.set("table2", "k2".into(), "v2".into()).unwrap();
        let k_vec = vec!["k1".to_string(), "k2".to_string()];
        let mut data = store.m_get("table2", k_vec).unwrap().unwrap();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        // let v1 = <&str as Into<Value>>::into("v1");
        // let v2 = <&str as Into<Value>>::into("v2");
        assert_eq!(data, vec![
            "v1".into(),
            "v2".into()
        ]);
        println!("{:?}", data);
    }
}