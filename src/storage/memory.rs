
use dashmap::{DashMap, mapref::one::Ref};
use crate::{KvPair, Storage, Value};
use crate::errors::KvError;

#[derive(Clone, Default, Debug)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    /// 创建一个缺省的 MemTable
    pub fn new() -> Self {
        Self::default()
    }

    /// 如果名为 name 的 hash table 不存在，则创建，否则返回
    // Ref<String, DashMap<String, Value>>，具体是干什么的，要靠猜啊，官方文档也没有详细说明
    fn get_or_create_table(&self, name: &str) -> Ref<String, DashMap<String, Value>> {
        match self.tables.get(name) {
            Some(table) => table,
            None => {
                let entry = self.tables.entry(name.into()).or_default();
                entry.downgrade()   // 上面返回的是RefMut<String, DashMap<String, Value>>，但我们定义的返回值是Ref<>，所以要downgrade
            }
        }
    }
}

impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.get(key).map(|v| v.value().clone()))       // Value没有实现`Copy` trait，只能用clone()
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.insert(key, value))
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        // 调用remove后，得到Option<(K, V)>，这里注意Option里面是一个元组的(K, V)，然后只返回value，所以是map(|(_k, v)| v)
        Ok(table.remove(key).map(|(_k, v)| v))
    }

    fn get_all(&self, table: &str) -> Result<Vec<KvPair>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table
            .iter()
            .map(|pair| KvPair::new(pair.key(), pair.value().clone()))
            .collect()
        )
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item=KvPair>>, KvError> {
        todo!()
    }
}