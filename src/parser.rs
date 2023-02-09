use polars::{prelude::*, export::regex::bytes::Regex};

use yaml_rust::Yaml;

use log::{info, warn, error};
use yaml_rust::YamlEmitter;
use yaml_rust::YamlLoader;

use core::panic;

use std::str;

use crate::missing;
use crate::numeric;
use crate::helper::string_contruct;
use crate::schema::match_cond;

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
        
        "avg" => numeric::avg(col, cond_op, cond, df),

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
    let mut ret: (u64, String) = (0, String::from("CHECK INEXISTENT"));

    for (key, values) in yaml.as_hash().unwrap(){
        let key = key.as_str().expect("Each check has to start with a unindented string");

        for (cond_types, condition) in values.as_hash().expect("YAML is not properly formatted"){

            let (cond_type, types) = match (cond_types, condition) {
                (Yaml::String(cond_type_parsed), Yaml::String(_condition_parsed)) => (cond_type_parsed, "simple"),
                (Yaml::String(cond_type_parsed), Yaml::Hash(_condition_parsed)) => (cond_type_parsed, "complex"),
                _ => panic!("Type and condition must be strings")
            };

            if types.cmp("simple").is_eq() {

                let condition = condition.as_str().expect(&format!("Condition malformed: {:?}", condition));

                let (ret_temp, parse_string_temp): (u64, String) = parse_string(format!("{} {}", key.trim(), condition.replace("when", "").trim()), &df);
                ret.0 = ret_temp;

                let ret_temp: bool = ret_temp == 0;

                let parsed_condition: &str = str::from_utf8(Regex::new(r"\s\((.*)\)").unwrap()
                .find(parse_string_temp.as_bytes()).unwrap()
                .as_bytes()).unwrap().trim();

                if ret_temp && no_change{
                    ret.1 = format!("- {} {} {} {} [PASSED]", key, cond_type, condition, parsed_condition);
                }
                else if !ret_temp{
                    ret.1 = format!(" - {} {} {} {} [{}ED]", key, cond_type, condition, parsed_condition, cond_type.to_uppercase());
                    no_change = false;
                    if cond_type.cmp(&"fail".to_string()).is_eq(){
                        fail_ap = true;
                        break;
                    }
                }
                
            }
            else if types.cmp("complex").is_eq() {
                let condition = condition.as_hash().expect(&format!("Condition malformed: {:?}", condition));

                let mut all_parsed: String = String::from("");

                let mut times = 0;
                let mut passed = 0; 

                for (check_str, cols_to_check) in condition {
                    let check_str = check_str.as_str().expect("Malformed YAML, expected string got something else, check indentation");
                    let regex = Regex::new(r"^when\s(.*)\scolumn\s(.*)$").unwrap().captures(check_str.as_bytes()).expect(&format!("Malformed schema check got {}", check_str));
                    
                    let cond_type_col  : &str = str::from_utf8(regex.get(1).expect(&format!("Malformed schema check got {}", check_str)).as_bytes()).expect("Something went wrong");
                    let check          : &str = str::from_utf8(regex.get(2).expect(&format!("Malformed schema check got {}", check_str)).as_bytes()).expect("Something went wrong");

                    let cols_to_check = cols_to_check.as_vec().expect(&format!("Expected array got {:?}, check yaml indentation", cols_to_check)).to_owned();

                    let (ret_temp, parse_string_temp): (u64, String) = match_cond(cond_type_col, check, cols_to_check, df);

                    all_parsed += &&format!("{}\n", parse_string_temp);

                    passed += ret_temp;
                    times += 1;

                }

                // All_parsed has yaml formatting, so if we want to add to yaml we might need to go inside schema.rs

                if times == passed && no_change{
                    ret.1 = format!("- schema [PASSED]:\n{}", all_parsed);
                    no_change = false;
                }

                else if times != passed{
                    ret.1 = format!("- schema [{}ED]:\n{}", cond_type.to_uppercase(), all_parsed);
                    no_change = false;
                    if cond_type.cmp(&"fail".to_string()).is_eq(){
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



pub fn parse_yaml(yaml: &Yaml, df: DataFrame, check_name: &String) -> bool{

    for (key, values) in yaml.as_hash().expect("YAML malformed"){
        let (check, check_name_yaml) = key.as_str()
        .expect("Each check has to start with a unindented string")
        .split_once(" ").expect("Checks should start with check name_of_check");

        match check.cmp(&"check").is_eq(){
            true => (),
            false => {warn!("The format for checks is: \"check name_of_check\", in the .yml is written as \"{} {}\"", check, check_name_yaml); continue;}
        };

        if check_name_yaml.cmp(&check_name).is_ne(){
            continue;
        }

        let mut check_message: String = format!("CHECK CONFIG FOR {}:\n", check_name_yaml.to_uppercase());
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

            check_message += &format!("  - TEST {}:\n    {}\n", total, msg);
        }

        let pass_percent: f64 = (passed as f64)/(total as f64)*100.0;

        check_message += &format!("  - A total of {} tests were ran:\n    - {} failed.\n    - {} passsed.\n    - {}%", total, total - passed, passed, pass_percent);

        let mut out_str: String = String::new();
        YamlEmitter::new(&mut out_str).dump(&YamlLoader::load_from_str(&check_message).unwrap()[0]).unwrap();
        
        return match pass_percent > 50.0 {
            true => {info!("{}", out_str); true},
            false => {error!("{}", out_str); false},
        };
    }

    error!("Check name \"{}\" does not exist in the current yaml, current checks found in yaml: {:?}", check_name, 
    yaml.as_hash().expect("YAML malformed").keys().into_iter().map(|yaml_keys| yaml_keys.to_owned().into_string().unwrap()).collect::<Vec<String>>());

    false
}