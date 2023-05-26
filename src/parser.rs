use log::warn;
use polars::{prelude::*, export::regex::bytes::Regex};

use pyo3::pyclass;
use yaml_rust::Yaml;

use log::{info, error};
use yaml_rust::YamlEmitter;
use yaml_rust::YamlLoader;

use core::panic;

use std::str;

use crate::missing;
use crate::numeric;
use crate::helper::string_contruct;
use crate::schema::like_file;
use crate::schema::match_cond;
use crate::helper::check_div;

/// Beers object:
/// passed (int): Number of successfull or warned tests
/// failed (int): Number of failed tests
/// total (int): Total number of check performed
/// log (str): Contains the log of all the tests, can also be displayed if true is passed to logging in functions
/// result (bool): True if it passed >= 50% of tests, otherwise false 
#[pyclass(name= "Beers")]
pub struct CheckStruct {
    #[pyo3(get)]
    passed: u64,
    #[pyo3(get)]
    failed: u64,
    #[pyo3(get)]
    total: u64,
    #[pyo3(get)]
    log: String,
    #[pyo3(get)]
    result: bool,
}

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
    let mut no_change: bool = true;
    let mut ret: (u64, String) = (0, String::from("ERROR, SOMETHING WENT WRONG, CHECK IGNORED"));

    let key = yaml.as_hash().unwrap().keys().next().unwrap().as_str().expect("Each check has to start with a unindented string");
    let values_raw = yaml.as_hash().unwrap().values().next().unwrap();

    if let Yaml::Hash(values) = values_raw {
        for (cond_types, conditions) in values.iter().filter(|(cond_types, _)| {
            if let Yaml::String(cond_type) = cond_types {
                if ["warn", "fail"].contains(&cond_type.to_lowercase().as_str()){
                    return true
                }
                warn!("The only valid conditions are warn and fail got {}, skipping, ", cond_type);
            }
            else {
                warn!("Expected a String() got {:?}, skipping", cond_types);
            }   
            return false;
            }){

            if let (Yaml::String(cond_type), Yaml::String(condition)) = (cond_types, conditions) {                
                let (ret_temp, parse_string_temp): (u64, String) = parse_string(format!("{} {}", key.trim(), condition.replace("when", "").trim()), &df);
    
                ret.0 = ret_temp;
    
                let parsed_condition: &str = str::from_utf8(Regex::new(r"\s\((.*)\)").unwrap()
                .find(parse_string_temp.as_bytes()).unwrap()
                .as_bytes()).unwrap().trim();
    
                if ret_temp == 0 && no_change{
                    ret.1 = format!("- {} {} {} {} [PASSED]", key, cond_type, condition, parsed_condition);
                }
    
                else if !(ret_temp == 0) {
                    ret.1 = format!(" - {} {} {} {} [{}ED]", key, cond_type, condition, parsed_condition, cond_type.to_uppercase());
                    no_change = false;
                    if cond_type.cmp(&"fail".to_string()).is_eq(){
                        break;
                    }
                }
            }
            else if let (Yaml::String(cond_type), Yaml::Hash(condition)) = (cond_types, conditions) {
    
                let mut all_parsed: String = String::from("");
                let mut times = 0;
    
                let passed: u64 = condition.iter().map(|(check_str, cols_to_check)| {
                    let check_str = check_str.as_str().expect("Malformed YAML, expected string got something else, check indentation");
                    let regex = Regex::new(r"^when\s(.*)\scolumn\s(.*)$").unwrap().captures(check_str.as_bytes()).expect(&format!("Malformed schema check got {}", check_str));
                    
                    let cond_type_col  : &str = str::from_utf8(regex.get(1).expect(&format!("Malformed schema check got {}", check_str)).as_bytes()).expect("Something went wrong");
                    let check          : &str = str::from_utf8(regex.get(2).expect(&format!("Malformed schema check got {}", check_str)).as_bytes()).expect("Something went wrong");
    
                    let cols_to_check = cols_to_check.as_vec().expect(&format!("Expected array got {:?}, check yaml indentation", cols_to_check)).to_owned();
    
                    let (ret_temp, parse_string_temp): (u64, String) = match_cond(cond_type_col, check, cols_to_check, df);
    
                    all_parsed += &format!("{}\n", parse_string_temp);
                    times += 1;
                    ret_temp
                }).sum();
                // All_parsed has yaml formatting, so if we want to add to yaml we might need to go inside schema.rs
    
                if times == passed && no_change{
                    ret.1 = format!("- schema [PASSED]:\n{}", all_parsed);
                    no_change = false;
                }
    
                else if times != passed{
                    ret.1 = format!("- schema [{}ED]:\n{}", cond_type.to_uppercase(), all_parsed);
                    no_change = false;
                    if cond_type.cmp(&"fail".to_string()).is_eq(){
                        break;
                    }
                }
            }
            else {
                return (0 ,format!("Expected String() Hash() or String() String(), got {:?} {:?}", cond_types, conditions));
            }
        }
    }
    else if let Yaml::Array(checks) = values_raw {
        let mut times = 0;
        let mut all_parsed = String::from("");

        let passed: u64 = checks.iter().map(|check| {
            let check: &str = check.as_str().expect(&format!("Expected String() got {:?}", check));
            let (passed, parse_string_temp): (u64, String) = like_file(check, CsvReader::from_path(key.replace("schema like", "").trim()).unwrap().finish().unwrap(), df);
            
            all_parsed += &format!("{}\n", parse_string_temp);
            times += 1;

            passed
        }).sum();

        ret.0 = (times == passed) as u64;
        ret.1 = format!("- {} [{}]:\n{}", key, ["FAILED", "PASSED"][ret.0 as usize], all_parsed);
    }
    else {
        println!("{:?}", values_raw);
    }

    ret
}

pub fn parse_yaml(yaml: &Yaml, df: DataFrame, check_name: &String) -> pyo3::PyResult<CheckStruct> {
    let parse: Option<CheckStruct> = yaml.as_hash().expect("YAML malformed")
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

        let total_passed: f64 = check_div(passed, total).unwrap_or(0.0)*100.0;

        check_message += &format!("  - A total of {} tests were ran:\n    - {} failed.\n    - {} passsed.\n    - {}%", total, total - passed, passed, total_passed);

        let mut out_str: String = String::new();
        YamlEmitter::new(&mut out_str).dump(&YamlLoader::load_from_str(&check_message).unwrap()[0]).unwrap();

        info!("{}", out_str);

        return CheckStruct{
            passed,
            failed: total - passed,
            total,
            log: out_str,
            result: total_passed >= 50.0,
        }

  }).next();

    match parse {
        Some(result) => Ok(result),
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
            return Err(pyo3::exceptions::PyTypeError::new_err(format!("TypeError: Invalid check, {}", check_name)));
        }      
    }
}