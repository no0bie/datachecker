use log::warn;
use polars::{prelude::*, export::regex::bytes::Regex};

use yaml_rust::Yaml;

use log::{info, error};
use yaml_rust::YamlEmitter;
use yaml_rust::YamlLoader;

use core::panic;

use std::str;

use crate::missing;
use crate::numeric;
use crate::helper::string_contruct;
use crate::schema::match_cond;
use crate::helper::check_div;

fn parse_string(string: String, df: &DataFrame) -> (u64, String){
    
    let malformed: &str = &format!("Malformed check, expected \"check_type operator condition\" got: \"{}\"", string);

    let mut _regex: Regex = Regex::new(r"").unwrap(); 

    if string.starts_with("row_") || string.starts_with("columns_"){
        _regex = Regex::new(r"^(\w*)_(\w*)\s(not\sbetween|between|.*)\s(.*)$").unwrap();
    }
    else{
        _regex = Regex::new(r"^(\w*)\((.*)\)\s(not\sbetween|between|.*)\s(.*)$").unwrap();
    }

    let re_captures = _regex.captures(string.as_bytes()).expect(malformed);
    
    let check  : &str = str::from_utf8(re_captures.get(1).expect(&format!("{}, missing check_type", malformed)).as_bytes()).expect("Something went wrong");
    let col    : &str = str::from_utf8(re_captures.get(2).expect(&format!("{}, missing the column that you want to check", malformed)).as_bytes()).expect("Something went wrong");
    let cond_op: &str = str::from_utf8(re_captures.get(3).expect(&format!("{}, missing operator", malformed)).as_bytes()).expect("Something went wrong");
    let cond   : &str = str::from_utf8(re_captures.get(4).expect(&format!("{}, missing condition", malformed)).as_bytes()).expect("Something went wrong");

    let (outcome, evaluated_str) = match check {
        "row" | "columns" => numeric::count(check, cond_op, cond, df), // Exceptional case where check = row or column
        
        "avg" => numeric::avg(col, cond_op, cond, df),

        "max" => numeric::max(col, cond_op, cond, df),
        "max_length" => numeric::max_length(col, cond_op, cond, df),

        "missing_count" => missing::missing_count(col, cond_op, cond, df), 
        "missing_percent" => missing::missing_percent(col, cond_op, cond, df),

        _ => panic!("Not implemented yet")  
    };

    (outcome as u64, string_contruct(string, evaluated_str, outcome))
}

fn parse_hash(yaml: Yaml, df: &DataFrame) -> (u64, String){
    let mut  fail_ap: bool = false;
    let mut no_change: bool = true;
    let mut ret: (u64, String) = (0, String::from("CHECK INEXISTENT"));

    // let key = yaml.as_hash().unwrap().keys().next().unwrap().as_str().expect("Each check has to start with a unindented string");
    // let value = yaml.as_hash().unwrap().keys().next().unwrap();

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

pub fn parse_yaml(yaml: &Yaml, df: DataFrame, check_name: &String) -> Option<bool>{
    let parse: Option<bool> = yaml.as_hash().expect("YAML malformed")
    .iter().filter(|(key, _)| key.as_str().expect("YAML malformed").ends_with(check_name))
    .map(|(_, check_config)| {
        let mut check_message: String = format!("CHECK CONFIG FOR {}:\n", check_name.to_uppercase());
        let mut total: u64 = 0;

        let passed: u64 = check_config.as_vec().unwrap_or(&vec![])
        .iter().filter_map(|check| {
            match check {
                Yaml::String(yaml) => Some(parse_string(yaml.to_owned(), &df)),
                Yaml::Hash(yaml) => Some(parse_hash(Yaml::Hash(yaml.to_owned()), &df)),
                _ => {warn!("Ignored check: {:?}, expected a String() or Hash(), check for yaml formatting", check);return None},
            }
        })
        .map(|(check_bool, check_msg)| {total+=1;check_message+=&format!("  - TEST {}:\n    {}\n", total, check_msg);check_bool})
        .sum::<u64>();

        check_message += &format!("  - A total of {} tests were ran:\n    - {} failed.\n    - {} passsed.\n    - {}%", total, total - passed, passed, check_div(passed, total).unwrap_or(0.0)*100.0);

        let mut out_str: String = String::new();
        YamlEmitter::new(&mut out_str).dump(&YamlLoader::load_from_str(&check_message).unwrap()[0]).unwrap();

        info!("{}", out_str);

        return passed > total.wrapping_div(2)

  }).next();

    match parse {
        None => {
            error!("Check name \"{}\" does not exist in the current yaml, current checks found in yaml: {:?}", check_name, 
                yaml.as_hash().unwrap().keys()
                .filter_map(|k| { 
                    let check = k.as_str().unwrap().split_once(" "); 
                    if check.is_none() {
                        error!("All checks must start with \"check check_name\". You are missing a space in check: \"{}\"", k.as_str().unwrap());
                        return None
                    } 
                    else {
                        return Some(check.unwrap().1)
                    }
                }).collect::<Vec<&str>>());
                return None
            },
        _ => return parse, 
    }
}