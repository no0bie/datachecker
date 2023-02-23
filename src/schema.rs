use core::panic;

use itertools::Itertools;

use polars::prelude::*;
use yaml_rust::Yaml;

pub fn like_file(check: &str, df_base: DataFrame, df: &DataFrame) -> (u64, String) {
    match check {
        "columns" => {
            let columns = df.get_column_names().iter().map(|x| Yaml::String(x.to_string())).collect::<Vec<Yaml>>();
            let (msg, existing, missing) = check_columns("columns", columns, &df_base);
            return ((existing >= missing) as u64, msg);
        },
        "types" => check_types(df_base, df), 
        _ => panic!(),
    }
}

pub fn match_cond(cond_type: &str, check: &str, columns: Vec<Yaml>, df: &DataFrame) -> (u64, String){
    match (cond_type, check) {
        ("forbidden", "present") => {let (msg, m, _) = check_columns(cond_type, columns, df); return ((m <= 0) as u64, msg)},
        ("required", "missing") => {let (msg, m, _) = check_columns(cond_type, columns, df); return ((m <= 0) as u64, msg)},
        ("wrong", "") => panic!("Not implemented"),// if check.cmp("type").is_eq(){wrong_check(columns)} else {panic!("You wrote {} but for the statement wrong column you can only use type", check)},
        _ => panic!("The check for schema with parameters {} {} don't exist, check spelling", cond_type, check)
    }

}

fn check_columns(cond_type: &str, columns: Vec<Yaml>, df: &DataFrame) -> (String, usize, usize){
    let col_str= columns.iter().map(|col| col.as_str().expect(&format!("Expected string and got {:?} check indentation", col)).to_string());
    let mut at_least_one_exists = false; 

    // Returns a Vector with missing and existing columns in our dataframe
    // let columns Vec<String> = vec!["existing1 existing2 ... ", "missing1 missing2 ... "]
    let columns_parsed: Vec<String> = col_str.filter_map(|col| {
        if let Ok(_col) = df.column(&col) {
            at_least_one_exists = true;
            return Some((0, format!("{} ", col)))
        }
        return Some((1, format!("{} ", col)))
    })
    .group_by(|x| x.0)
    .into_iter().map(|(_, i)| i.map(|ii| ii.1).collect::<String>()).collect::<Vec<String>>();

    let default: String = String::from("");
    let existing = columns_parsed.get(!at_least_one_exists as usize).unwrap_or(&default); 
    let missing = columns_parsed.get(at_least_one_exists as usize).unwrap_or(&default);

    let msg = format!("      - {}:\n         - missing = [ {}]\n         - found = [ {}]", cond_type, missing, existing);

    (msg, missing.split_whitespace().count(), existing.split_whitespace().count())
}

fn check_types(df_base: DataFrame, df: &DataFrame) -> (u64, String){
    let msg;

    let errs: String = df.get_columns().iter().filter_map(|checking_col|{
        if let Ok(col_df_base) = df_base.column(checking_col.name()) {
            if col_df_base.dtype() == checking_col.dtype(){
                // Ignore them ?
               //  return Some(format!("         - {} OK -> {}\n", checking_col.name(), checking_col.dtype()));
            }
            else{
                return Some(format!("         - {} ERR -> GOT '{}' EXPECTED '{}'\n", checking_col.name(), checking_col.dtype(), col_df_base.dtype()));
            }
        }
        return None;
    }).collect::<String>();

    if errs.is_empty() {
        msg = format!("      - types:\n         - All types match!");
    }
    else {
        msg = format!("      - types:\n{}", errs);
    }


    return ((msg.split("\n").count() - 2 <= 0 )as u64, msg);
}