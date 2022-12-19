use pyo3::prelude::*;
use polars::prelude::*;

mod helper;
mod missing;
mod numeric;
mod parser; use parser::parse_yaml;
mod io; use io::{yaml_load, file_load};

fn main(yaml_path: String, df: DataFrame){
    parse_yaml(
        yaml_load(yaml_path), 
        df
    )
}

#[pyfunction]
fn load_from_csv(yaml_path: String, df_path: String){
    main(
        yaml_path, 
        CsvReader::new(file_load(df_path)).finish().unwrap()
    )
}

#[pyfunction]
fn load_from_json(yaml_path: String, df_path: String){
    main(
        yaml_path, 
        JsonReader::new(file_load(df_path)).finish().unwrap()
    )
}

#[pymodule]
fn datachecker(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(load_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(load_from_csv, m)?)?;
    Ok(())
}