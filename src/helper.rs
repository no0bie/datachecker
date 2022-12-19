use polars::prelude::*;

pub fn string_contruct(string: String, evaluated_str: String , outcome: String) -> String{
    format!("{} ({}) [{}]", string, evaluated_str, outcome)
}

pub fn column_exists(col: &str, df: &DataFrame){
    if !df.get_column_names().contains(&col){
        panic!("Invalid column for DataFrame");
    }
}

pub fn eval(numeric: f64, cond_op: &str, cond: &str) -> (bool, String){

    let result: bool = match cond_op{
        ">" => numeric > cond.parse::<f64>().unwrap(),
        "<" => numeric < cond.parse::<f64>().unwrap(),
        
        "<=" => numeric <= cond.parse::<f64>().unwrap(),
        ">=" => numeric >= cond.parse::<f64>().unwrap(),
        
        "!=" => numeric != cond.parse::<f64>().unwrap(),
        
        "=" => numeric == cond.parse::<f64>().unwrap(),

        "between" => {
            let (upper, lower) = cond.split_once(" and ").expect("Between conditions must be in the format <upper> and <lower>");
            let upper: f64 = upper.parse().unwrap();
            let lower: f64 = lower.parse().unwrap();

            if upper < lower{
                upper < numeric  && numeric < lower
            }
            else{
                lower < numeric  && numeric < upper
            }
        },

        "not between" => {
            let (upper, lower) = cond.split_once(" and ").expect("Between conditions must be in the format <upper> and <lower>");
            let upper: f64 = upper.parse().unwrap();
            let lower: f64 = lower.parse().unwrap();

            if upper < lower{
                upper > numeric  && numeric > lower
            }
            else{
                lower > numeric  && numeric > upper
            }
        }
        
        _ => false,
    };

    (result, format!("{} {} {}", numeric, cond_op, cond))

}