use core::panic;

use polars::prelude::*;
use yaml_rust::Yaml;


pub fn match_cond(cond_type: &str, check: &str, columns: Vec<Yaml>, df: &DataFrame) -> (u64, String){
    match (cond_type, check) {
        ("forbidden", "present") | ("required", "missing") => get_columns(cond_type, columns, df),
        ("wrong", "") => panic!("Not implemented"),// if check.cmp("type").is_eq(){wrong_check(columns)} else {panic!("You wrote {} but for the statement wrong column you can only use type", check)},
        _ => panic!("The check for schema with parameters {} {} don't exist, check spelling", cond_type, check)
    }

}

fn get_columns(cond_type: &str, columns: Vec<Yaml>, df: &DataFrame) -> (u64, String){
    let col_str= columns.iter().map(|col| col.as_str().expect(&format!("Expected string and got {:?} check indentation", col)).to_string());

    let col_str2 = col_str.clone();
    
    let (existing_columns, missing_columns): (Vec<String>, Vec<String>) = (col_str.filter(|col| df.get_column_names().contains(&col.as_str())).collect::<Vec<String>>(), col_str2.filter(|col| !df.get_column_names().contains(&col.as_str())).collect::<Vec<String>>());

    let parsed_existing: String = existing_columns.iter().map(|col| format!("{} ", col)).collect();
    let parsed_missing: String = missing_columns.iter().map(|col| format!("{} ", col)).collect();

    let msg = format!("      - {}:\n         - missing = [ {}]\n         - found = [ {}]", cond_type, parsed_missing, parsed_existing);

    ((existing_columns.len() != columns.len()) as u64, msg)
}