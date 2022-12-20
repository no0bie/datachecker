use polars::{prelude::*, export::regex::Regex};

use std::collections::HashMap;

use pyo3::prelude::*;

use walkdir::WalkDir;

mod helper;
mod missing;
mod numeric;
mod parser; use parser::parse_yaml;
mod io; use io::{yaml_load, cursor_load};

#[pyfunction]
fn load_from_csv_string(yaml_path: String, csv_string: String, check_name: String)  -> PyResult<bool>{    
    Ok(parse_yaml(
        &yaml_load(yaml_path),
        CsvReader::new(cursor_load(csv_string)).finish().unwrap(),
        &check_name
    ))
}

#[pyfunction]
fn load_from_csv(yaml_path: String, df_path: String, check_name: String) -> PyResult<bool>{
    Ok(parse_yaml(
        &yaml_load(yaml_path),
        CsvReader::from_path(df_path).unwrap().finish().unwrap(),
        &check_name
    ))
}

#[pyfunction]
fn load_from_directory(yaml_path: String, dir_path: String, check_name: String, regex_file_names: String) -> PyResult<HashMap<String, bool>>{
    
    let yaml = yaml_load(yaml_path);
    let re: Regex = Regex::new(&regex_file_names).unwrap();

    let mut files: HashMap<String, bool> = HashMap::new();

    for file in WalkDir::new(dir_path).into_iter().filter_map(|file| file.ok()) {
        if file.metadata().unwrap().is_file() && re.is_match(&file.file_name().to_str().unwrap().to_string()) {
            let bool = parse_yaml(
                &yaml,
                CsvReader::from_path(file.path()).unwrap().finish().unwrap(),
                &check_name
            );
            
            files.insert(file.file_name().to_str().unwrap().to_string(), bool);
        }
    }
    Ok(files)
}

#[pymodule]
fn datachecker(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(load_from_csv_string, m)?)?;
    m.add_function(wrap_pyfunction!(load_from_directory, m)?)?;
    m.add_function(wrap_pyfunction!(load_from_csv, m)?)?;
    Ok(())
}