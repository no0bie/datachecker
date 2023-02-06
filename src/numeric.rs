use polars::prelude::*;
use crate::helper::{eval, column_exists};

pub fn avg(col: &str, cond_op: &str, cond: &str, df: &DataFrame) -> (bool, String){
    column_exists(&col, &df);

    let avg_val: f64 = df[col].mean().expect("Expected a number");

    eval(avg_val, cond_op, cond)
}

pub fn max(col: &str, cond_op: &str, cond: &str, df: &DataFrame) -> (bool, String){
    column_exists(&col, &df);

    let max_val: f64 = df[col].max().expect("Expected a number");

    eval(max_val, cond_op, cond)
}

pub fn max_length(col: &str, cond_op: &str, cond: &str, df: &DataFrame) -> (bool, String){
    column_exists(&col, &df);

    let max_val = (df.max().column(col).unwrap().get(0).to_string().len() as f64) - 2.0;

    eval(max_val, cond_op, cond)  
}

pub fn count(col: &str, cond_op: &str, cond: &str, df: &DataFrame) -> (bool, String){
    let count_val: i32 = match col {
        "row" => df.shape().0 as i32,
        "columns" => df.shape().1 as i32,
        _ => panic!("Count can only be used for rows and columns")
    };

    eval(count_val as f64, cond_op, cond)
}