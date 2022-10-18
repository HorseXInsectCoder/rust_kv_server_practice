
#[derive(Debug)]
struct Value(String);

#[derive(Debug)]
struct kvpair {
    k: String,
    v: Value,
}

// impl Into<String> for Value {
//     fn into(self) -> String {
//         self.0
//     }
// }

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

#[derive(Debug)]
struct Person {
    name: String,
    age: i32,
}

#[derive(Debug)]
enum Device {
    X1,
    M1,
    Switch
}

#[derive(Debug)]
struct ComplexStruct {
    person: Person,
    location: String,
    id: i32,
    devices: Vec<Device>
}

impl ToString for ComplexStruct {
    fn to_string(&self) -> String {
        format!("{:?}--{:?}--{:?}--{:?}", self.id, self.person, self.location, self.devices)
    }
}

#[derive(Debug)]
struct MyVec(Vec<String>);

impl MyVec {
    fn new(v: Vec<String>) -> Self {
        Self {
            0: v
        }
    }
}

impl ToString for MyVec {
    fn to_string(&self) -> String {
        format!("{:?}", self.0.join("--"))
    }
}

fn main() {
    let person = Person {
        name: "voyager-1".to_string(),
        age: 18
    };
    let cs = ComplexStruct {
        person,
        location: "swiss".to_string(),
        id: 1,
        devices: vec![Device::M1]
    };
    println!("{:?}", cs);
    println!("{:?}", cs.to_string());

    let v = vec!["hello".into(), "world".into()];
    let myvec = MyVec::new(v);
    println!("{:?}", myvec.to_string());
}