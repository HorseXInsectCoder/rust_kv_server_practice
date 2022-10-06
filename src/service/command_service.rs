use crate::*;
use crate::errors::KvError;


impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) =>  v.into(),
            Err(e) => e.into(),
        }
    }
}

/*  这些测试的作用就是验证产品需求，比如：HSET 成功返回上一次的值（这和 Redis 略有不同，Redis 返回表示多少 key 受影响的一个整数）
    HGET 返回 Value
    HGETALL 返回一组无序的 Kvpair
 */
#[cfg(test)]
mod tests {
    use crate::command_request::RequestData;
    use super::*;
    use crate::storage::memory::MemTable;       // 要先使memory可见，pub mod memory;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
        // let res = dispatch(cmd.clone(), &store);
        let res = dispatch(cmd.clone(), &store);

        // 看源码应该是生成的Value(在::prost::Message这个宏)已经实现了Default
        // 第一次set进去，返回一个默认值（因为设计是set进去就返回上一个对应key的value值）
        // pairs在这个测试用不上，填入空切片即可
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        // println!("{:?}", res);
        // CommandResponse { status: 200, message: "", values: [Value { value: Some(Integer(10)) }], pairs: [] }
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        println!("{:?}", res);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into())
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hgetall("score");
        let res = dispatch(cmd, &store);
        let pairs = &[
            KvPair::new("u1", 6.into()),
            KvPair::new("u2", 8.into()),
            KvPair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    // 从 Request 中得到 Response，目前处理 HGET/HGETALL/HSET
    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hget(v) => v.execute(store),
            RequestData::Hgetall(v) => v.execute(store),
            RequestData::Hset(v) => v.execute(store),
            _ => todo!(),
        }
    }

    // 测试成功返回的结果
    fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[KvPair]) {
        res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);    // res.values是Vec<abi::Value>，所以直接用切片更方便
        assert_eq!(res.pairs, pairs);
    }

    // 测试失败返回的结果
    fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
        assert_eq!(res.status, code);
        assert!(res.message.contains(msg));
        assert_eq!(res.values, &[]);
        assert_eq!(res.pairs, &[]);
    }
}