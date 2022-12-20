use yaml_rust::{Yaml, YamlLoader};

use std::io::Cursor;

pub fn cursor_load(string_contents: String) -> Cursor<String> {
    Cursor::new(string_contents)
}

pub fn yaml_load(yaml: String) -> Yaml{
    let yaml_contents: String = std::fs::read_to_string(yaml).expect("Cant find the file");
    let yaml_parsed: Yaml = (&YamlLoader::load_from_str(&yaml_contents).unwrap()[0]).to_owned();

    /* let mut out_str: String = String::new();
    YamlEmitter::new(&mut out_str).dump(&yaml_parsed).unwrap();
    
    println!("Parsing YAML:\n{}", out_str); */

    yaml_parsed
}