use std::env;
use std::env::Args;

fn handle_get_or_rm(args: &mut Args) {
    match args.next() {
        Some(_) => { // checking Key
            match args.next() {
                None => panic!("unimplemented"),
                Some(_) => panic!("Extra Arguments!")
            }
        },
        None => panic!("Key not set!")
    }
}

fn handle_set(args: &mut Args) {
    match args.next() {
        Some(_) => { // checking Key
            match args.next() { // checking Value
                None => panic!("Value not set!"),
                Some(_) => match args.next() {
                    None => panic!("unimplemented"),
                    Some(_) => panic!("Extra Arguments!")
                }
            }
        },
        None => panic!("Key not set!")
    }
}

fn main() {
    let mut args= env::args();

    if args.len() > 4 {
        panic!("Too many arguments!")
    }

    args.next();

    match args.next() {
        Some(s) => {
            match &s[..] {
                "-V" => println!(env!("CARGO_PKG_VERSION")),
                "get" | "rm" => handle_get_or_rm(&mut args),
                "set" => handle_set(&mut args),
                _ => panic!("Unknown argument!")
            }
        }
        None => panic!("Missing argument!"),
    };
}
