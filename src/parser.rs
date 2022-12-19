use polars::{prelude::*, export::regex::bytes::Regex};

use yaml_rust::Yaml;

use core::panic;

use std::str;

use crate::missing;
use crate::numeric;
use crate::helper::string_contruct;

fn parse_string(string: String, df: &DataFrame) -> (u64, String){

    let mut _outcome: bool = false;
    let mut _evaluated_str: String = String::new();
    
    let malformed: &str = &format!("Malformed check, expected 'check(column_name) operator condition' or 'row/colums_check operator condition' got : {}", string);

    let mut _regex: Regex = Regex::new(r"").unwrap(); 

    if string.starts_with("row_") || string.starts_with("columns_"){
        _regex = Regex::new(r"^(\w*)_(\w*)\s(not\sbetween|between|.*)\s(.*)$").unwrap();
    }
    else{
        _regex = Regex::new(r"^(\w*)\((.*)\)\s(not\sbetween|between|.*)\s(.*)$").unwrap();
    }

    let re_captures = _regex.captures(string.as_bytes()).expect(malformed);
    
    let check  : &str = str::from_utf8(re_captures.get(1).expect(malformed).as_bytes()).expect("Something went wrong");
    let col    : &str = str::from_utf8(re_captures.get(2).expect(malformed).as_bytes()).expect("Something went wrong");
    let cond_op: &str = str::from_utf8(re_captures.get(3).expect(malformed).as_bytes()).expect("Something went wrong");
    let cond   : &str = str::from_utf8(re_captures.get(4).expect(malformed).as_bytes()).expect("Something went wrong");

     // println!("{:?} {:?} {:?} {:?}", check, col, cond_op, cond); // --- PRINT ALL REGEX ---

    (_outcome, _evaluated_str) = match check {
        "row" | "columns" => numeric::count(check, cond_op, cond, df), // Exceptional case where check = row or column
        "max" => numeric::max(col, cond_op, cond, df),
        "max_length" => numeric::max_length(col, cond_op, cond, df),

        "missing_count" => missing::missing_count(col, cond_op, cond, df), 
        "missing_percent" => missing::missing_percent(col, cond_op, cond, df),

        _ => panic!("Not implemented yet")  
    };

    if _outcome {
        return (_outcome as u64, string_contruct(string, _evaluated_str, String::from("PASSED")));
    }
    (_outcome as u64, string_contruct(string, _evaluated_str, String::from("FAILED"), ))

}


fn parse_hash(yaml: Yaml, df: &DataFrame) -> (u64, String){
    let mut  fail_ap: bool = false;
    let mut no_change: bool = true;
    let mut ret: (u64, String) = (0, String::from("NOTHING"));

    for (key, values) in yaml.as_hash().unwrap(){
        let key = key.as_str().expect("Each check has to start with a unindented string");

        for (types, condition) in values.as_hash().expect("YAML is not properly formatted"){

            let (types, condition) = match (types, condition) {
                (Yaml::String(types_parsed), Yaml::String(condition_parsed)) => (types_parsed, condition_parsed),
                _ => panic!("Type and condition must be strings")
            };

            if ["warn", "fail"].contains(&&types.as_str()){
                let (ret_temp, parse_string_temp): (u64, String) = parse_string(format!("{} {}", key.trim(), condition.replace("when", "").trim()), &df);
                let ret_temp: bool = ret_temp == 0;

                ret.0 = ret_temp as u64;

                let parsed_condition: &str = str::from_utf8(Regex::new(r"\s\((.*)\)").unwrap()
                .find(parse_string_temp.as_bytes()).unwrap()
                .as_bytes()).unwrap().trim();

                if ret_temp && no_change{
                    ret.1 = format!("{} {} {} {} [PASSED]", key, types, condition, parsed_condition);
                }
                else if !ret_temp{
                    ret.1 = format!("{} {} {} {} [{}ED]", key, types, condition, parsed_condition, types.to_uppercase());
                    no_change = false;
                    if types.cmp(&"fail".to_string()).is_eq(){
                        fail_ap = true;
                        break;
                    }
                }
                
            }
        }
        if fail_ap{
            break
        }
    }
    ret 
}


pub fn parse_yaml(yaml: Yaml, df: DataFrame) {

    for (key, values) in yaml.as_hash().expect("YAML malformed"){
        let (check, dataset) = key.as_str()
        .expect("Each check has to start with a unindented string")
        .split_once(" ").expect("Titles is");

        if check.cmp(&"check").is_eq() && !dataset.is_empty(){
            let mut check_message: String = format!("Check config for {}:\n", dataset);
            let mut passed: u64 = 0;
            let mut total: u64 = 0;

            for value in values.clone().into_iter(){
                let (int, msg): (u64, String) =  match value {
                    Yaml::String(yaml) => parse_string(yaml, &df),
                    Yaml::Hash(yaml) => parse_hash(Yaml::Hash(yaml), &df),
                    _ => panic!("Seems like your yaml is malformed"),
                };

                total += 1;
                passed += int;

                check_message += &format!(" - TEST {}: {}\n", total, msg);
            }
            check_message += &format!("A total of {} tests were ran: {} failed. {} passsed. {}%", total, total - passed, passed, (passed as f64)/(total as f64)*100.0);
            
            println!("{}", check_message);
        }
        else{
            println!("The format for titles is check <name>");
        }
    }
}