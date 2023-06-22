use walkdir::WalkDir;

fn main() {
    WalkDir::new("C:\\").into_iter()
    .filter_map(|file| file.ok()).map(|file | {if(file.metadata().unwrap().is_file()){ println!("{file:?}") }}).count();
}