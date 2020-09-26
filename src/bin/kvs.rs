use std::env;
use std::env::Args;
use kvs::{KvStore, Command, Sequencer};

fn get_cmd(args: &mut Args, op: String) -> Command {
    let k = args.next().unwrap();

    let cmd = match args.next() {
        None => {
            if op.eq("get") || op.eq("rm") {
                Command::new(op, k, "".to_owned(), Sequencer::new(0))
            } else {
                panic!("Value not set!");
            }
        }
        Some(v) => {
            if op.eq("get") || op.eq("rm") {
                panic!("Extra Arguments!")
            } else {
                Command::new(op, k, v, Sequencer::new(0))
            }
        }
    };

    match args.next() {
        Some(_) => panic!("Extra Arguments!"),
        None => ()
    }

    cmd
}

fn main() {
    let mut args= env::args();

    if args.len() > 4 {
        panic!("Too many arguments!")
    }

    args.next();

    let mut store = KvStore::open(env::current_dir().unwrap()).unwrap();

    let cmd = match args.next() {
        Some(s) => {
            match &s[..] {
                "-V" => {
                    println!(env!("CARGO_PKG_VERSION"));
                    None
                },
                x if x.eq("get") || x.eq("rm") || x.eq("set") => Some(get_cmd(&mut args, x.to_owned())),
                _ => panic!("Unknown argument!")
            }
        }
        None => panic!("Missing argument!"),
    }.unwrap();

    match &cmd.op[..] {
        "get" => {
            match store.get(cmd.k).unwrap() {
                None => println!("Key not found"),
                Some(v) => println!("{}", v)
            };
        },
        "rm" => store.remove(cmd.k).unwrap(),
        "set" => store.set(cmd.k, cmd.v).unwrap(),
        _ => panic!("Unknown operation!")
    }
}
