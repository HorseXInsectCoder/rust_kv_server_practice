fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(&["."]);
    config.type_attribute(".", "#[derive(PartialOrd)]");
    config
        .out_dir("src/pb")                              // 输出目录，这个目录要预先存在，否则报错
        .compile_protos(&["abi.proto"], &["."]) // 生成文件的名字
        .unwrap();
}