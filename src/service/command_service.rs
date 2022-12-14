use crate::*;
use crate::errors::KvError;
use pb::StringWrapper;


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

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        // match store.m_get(&self.table, self.keys.clone()) {
        //     Ok(Some(v)) => v.into(),
        //     Ok(None) => KvError::NotFound(
        //         self.table, StringWrapper::from(self.keys.clone()).0).into(),
        //     Err(e) => e.into(),
        // }

        self.keys.iter()
            .map(|key| match store.get(&self.table, key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()

    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.pairs.into_iter()
            .map(|pair| {
                let result = store.set(&self.table, pair.key, pair.value.unwrap());
                match result {
                    Ok(Some(v)) => v,
                    _ => Value::default(),
                }
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => Value::default().into(),
            Err(e) => e.into()
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.del(&self.table, &key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(b) => Value::from(b).into(),      // ????????????false????????????????????? true.into()???????????????bool??????Value???????????? into() ?????? CommandResponse
            Err(e) => e.into()
        }
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.contains(&self.table, key) {
                Ok(v) => v.into(),
                _ => Value::default(),
            })
            .collect::<Vec<Value>>()
            .into()
    }

}


/*  ?????????????????????????????????????????????????????????HSET ???????????????????????????????????? Redis ???????????????Redis ?????????????????? key ???????????????????????????
    HGET ?????? Value
    HGETALL ????????????????????? Kvpair
 */
#[cfg(test)]
mod tests {
    use crate::command_request::RequestData;
    use super::*;
    use crate::storage::memory::MemTable;       // ?????????memory?????????pub mod memory;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
        // let res = dispatch(cmd.clone(), &store);
        let res = dispatch(cmd.clone(), &store);

        // ???????????????????????????Value(???::prost::Message?????????)???????????????Default
        // ?????????set????????????????????????????????????????????????set??????????????????????????????key???value??????
        // pairs????????????????????????????????????????????????
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

    #[test]
    fn memtable_mget_should_work() {
        let store = MemTable::new();
        test_mget(store);
    }


    fn test_mget(store: impl Storage) {
        // store.set("table2", "k1".into(), "v1".into()).unwrap();
        // store.set("table2", "k2".into(), "v2".into()).unwrap();
        // let k_vec = vec!["k1".to_string(), "k2".to_string()];
        // let mut data = store.m_get("table2", k_vec).unwrap().unwrap();
        // data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        // // let v1 = <&str as Into<Value>>::into("v1");
        // // let v2 = <&str as Into<Value>>::into("v2");
        // assert_eq!(data, vec![
        //     "v1".into(),
        //     "v2".into()
        // ]);
        // println!("{:?}", data);

        let store = MemTable::new();

        set_key_pairs(
            "user",
            vec![("u1", "Tyr"), ("u2", "Lindsey"), ("u3", "Rosie")],
            &store,
        );

        let cmd = CommandRequest::new_hmget("user", vec!["u1".into(), "u4".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        let values = &["Tyr".into(), Value::default(), "Rosie".into()];
        assert_res_ok(res, values, &[]);
    }

    // ??? Request ????????? Response??????????????? HGET/HGETALL/HSET
    // fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    //     match cmd.request_data.unwrap() {
    //         RequestData::Hget(v) => v.execute(store),
    //         RequestData::Hgetall(v) => v.execute(store),
    //         RequestData::Hset(v) => v.execute(store),
    //         RequestData::Hmget(v) => v.execute(store),
    //         _ => todo!(),
    //     }
    // }

    #[test]
    fn mset_should_work() {
        let store = MemTable::new();

        set_key_pairs("t1", vec![("u1", "world")], &store);

        let pairs = vec![
            KvPair::new("u1", 10.1.into()),
            KvPair::new("u2", 8.1.into()),
        ];
        let cmd = CommandRequest::new_hmset("t1", pairs);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into(), Value::default()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "hello")], &store);

        // u2????????????????????????None
        let cmd = CommandRequest::new_hdel("t1", "u2");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hdel("t1", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["hello".into()], &[]);
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "v1"), ("u2", "v2")], &store);

        let cmd = CommandRequest::new_hmdel("t1", vec!["u1".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["v1".into(), Value::default()], &[]);
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "v1"), ("u2", "v2")], &store);

        let cmd = CommandRequest::new_hexist("t1", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into()], &[]);

        let cmd = CommandRequest::new_hexist("t1", "u3");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[false.into()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "v1"), ("u2", "v2")], &store);

        let cmd = CommandRequest::new_hmexist("t1", vec!["u1".into(), "u3".into()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into(), false.into()], &[]);
    }


    // ???????????????????????????
    fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[KvPair]) {
        res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);    // res.values???Vec<abi::Value>?????????????????????????????????
        assert_eq!(res.pairs, pairs);
    }

    // ???????????????????????????
    fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
        assert_eq!(res.status, code);
        assert!(res.message.contains(msg));
        assert_eq!(res.values, &[]);
        assert_eq!(res.pairs, &[]);
    }

    fn set_key_pairs<T: Into<Value>>(table: &str, pairs: Vec<(&str, T)>, store: &impl Storage) {
        pairs.into_iter()
            .map(|(k, v)| CommandRequest::new_hset(table, k, v.into()))
            .for_each(|cmd| {
                dispatch(cmd, store);
            });
    }
}