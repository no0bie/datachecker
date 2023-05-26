def check_df(df, yaml_path: str, check_name: str, logging: bool = False) -> object:
    """Perform data integrity checks on a on-memory dataframe

    Args:
        df (DataFrame): Pandas dataframe
        yaml_path (str): Path to YAML file that contains your checks
        check_name (str): Name of a defined check inside your yaml
        logging (bool): True to enable logging, disabled by default
        
    Returns:
        object: Object cotaining all information regarding the test
    """
    from .beers import load_from_csv_string
    try:
        import pandas as pd
        if not isinstance(df, pd.DataFrame):
            print("The df passed is not a pandas DataFrame")
            return 
    except ModuleNotFoundError:
        print("Pandas module not installed")
        return

    if logging:
        import logging
        logging.basicConfig(level=logging.DEBUG)

    return load_from_csv_string(yaml_path, df.to_csv(), check_name)

def check_csv_dir(dir_path: str, yaml_path: str, check_name: str, regex_file_names: str, logging: bool = False) -> dict:
    """Perform a directory walk and perform the checks on all csv files that match the file names you passed

    Args:
        dir_path (str): Directory path where your csvs are located
        yaml_path (str): Path to YAML file that contains your checks
        check_name (str): Name of a defined check inside your yaml
        regex_file_names (str): Only checks the csv files that match the defined regex  
        logging (bool): True to enable logging, disabled by default

    Returns:
        dict<str, obj>: Key is the file name including the path and the object cotaining all information regarding the test
    """
    from .beers import load_from_directory

    if logging:
        import logging
        logging.basicConfig(level=logging.DEBUG)

    return load_from_directory(yaml_path, dir_path, check_name, regex_file_names)

def check_csv(csv_path: str, yaml_path: str, check_name: str, logging: bool = False) -> object:
    """Perform data integrity checks on a csv file

    Args:
        csv_path (str): Path to csv
        yaml_path (str): Path to YAML file that contains your checks
        check_name (str): Name of a defined check inside your yaml
        logging (bool): True to enable logging, disabled by default

    Returns:
        object: Object cotaining all information regarding the test
    """
    from .beers import load_from_csv

    if logging:
        import logging
        logging.basicConfig(level=logging.DEBUG)

    return load_from_csv(yaml_path, csv_path, check_name)


__all__ = [
    "check_df",
    "check_csv_dir",
    "check_csv"
]