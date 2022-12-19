use yaml_rust::{Yaml, YamlLoader};

use std::fs::File;

pub fn file_load(path: String) -> File{
    match File::open(path){
        Ok(file) => file,
        Err(err) => panic!("An error has occured:\n{}", err)
    }
}

pub fn yaml_load(yaml: String) -> Yaml{
    let yaml_contents: String = std::fs::read_to_string(yaml).expect("Cant find the file");
    let yaml_parsed: Yaml = (&YamlLoader::load_from_str(&yaml_contents).unwrap()[0]).to_owned();

    /* let mut out_str: String = String::new();
    YamlEmitter::new(&mut out_str).dump(&yaml_parsed).unwrap();
    
    println!("Parsing YAML:\n{}", out_str); */

    yaml_parsed
}