
#[derive(Debug)]
struct Value(String);

#[derive(Debug)]
struct kvpair {
    k: String,
    v: Value,
}

impl Into<String> for Value {
    fn into(self) -> String {
        self.0
    }
}

impl kvpair {
    fn new(k: String, v: Value) -> Self {
        Self {
            k,
            v
        }
    }
}

#[derive(Debug)]
struct kvString(Vec<kvpair>);

// impl kvString {
//     fn new(pair: Vec<kvpair>) -> String {
//         let mut s = String::new();
//         pair.iter().map(|p| {
//             let r = format!("{}--**{}", p.k, p.v);
//             s.push_str(r.as_str());
//             s.push_str("\n");
//         }).collect::<Vec<_>>();
//         s
//     }
// }

impl ToString for kvString {
    fn to_string(&self) -> String {
        format!("{:?}--", self.0)
    }
}


fn main() {
    let kv_1 = kvpair::new("hello".to_string(), Value("world".to_string()));
    let kv_2 = kvpair::new("abc".to_string(), Value("xyz".to_string()));

    let mut v = Vec::new();
    v.push(kv_1);
    v.push(kv_2);
    let r: _ = kvString(v).to_string();
    println!("{:?}", r);
}