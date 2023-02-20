use polars::prelude::*;

pub fn string_contruct(string: String, evaluated_str: String , outcome: bool) -> String{
    if outcome{
        return format!(" - {} ({}) [PASSED]", string, evaluated_str)
    }
    format!(" - {} ({}) [FAILED]", string, evaluated_str)
    
}

pub fn column_exists(col: &str, df: &DataFrame){
    if !df.get_column_names().contains(&col){
        panic!("Invalid column ({}) for dataFrame, columns in current dataframe: [{}]", col, df.get_columns().iter()
        .map(|df_col| format!("{} ", df_col.name())).collect::<String>());
    }
}

pub fn check_div(n1: u64, n2: u64) -> Option<f64>{
    if n2 == 0{return None}
    return Some(n1 as f64 / n2 as f64)
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

    (result, format!("{} {} {}", numeric, if !result {format!("!{}", cond_op)} else {cond_op.to_string()} , cond))

}