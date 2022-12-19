use polars::prelude::*;
use crate::helper::{eval, column_exists};

pub fn missing_count(col: &str, cond_op: &str, cond: &str, df: &DataFrame) -> (bool, String){
    column_exists(&col, &df);

    let null_values: f64 = df.column(col).unwrap().null_count() as f64;

    eval(null_values, cond_op, cond)
}

pub fn missing_percent(col: &str, cond_op: &str, cond: &str, df: &DataFrame) -> (bool, String) {
    column_exists(col, df);

    let null_values: f64 = df.column(col).unwrap().null_count() as f64;
    let total_rows: f64 = df.shape().0 as f64;
    
    let percent: f64 = null_values/total_rows*100.0;
    
    let cond: &str = &(cond.replace("%", "")
    .trim()
    .parse::<f64>()
    .expect("Failed to parse percentage"))
    .to_string();

    eval(percent, cond_op, cond)
}