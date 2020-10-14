use std::env;
use clap::{App, SubCommand, Arg};
use kvs::{Result, KvError, KvStore};
use std::process::exit;


fn main() -> Result<()> {
    let kvs_app = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
        )
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
                .arg(Arg::with_name("<VALUE>").help("ENTER A VALUE").required(true))
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
        )
        .get_matches();



    match kvs_app.subcommand() {
        ("get", Some(matches)) =>  {
            let k = matches.value_of("<KEY>").expect("<KEY> argument is missing");
            let mut kv = KvStore::open(env::current_dir()?)?;

            if let Some(v) = kv.get(k.to_owned())? {
                println!("{}", v);
            } else {
                println!("Key not found");
            }
        },
        ("rm", Some(matches)) => {
            let k = matches.value_of("<KEY>").expect("<KEY> argument is missing");
            let mut kv = KvStore::open(env::current_dir()?)?;
            match kv.remove(k.to_string()) {
                Ok(()) => (),
                Err(KvError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                },
                Err(e) => return Err(e),
            }
        }
        ("set", Some(matches)) => {
            let k = matches.value_of("<KEY>").expect("<KEY> argument is missing");
            let v = matches.value_of("<VALUE>").expect("<VALUE> argument is missing");

            let mut kv = KvStore::open(env::current_dir()?)?;
            kv.set(k.to_owned(), v.to_owned())?;
        }
        _ => unreachable!()
    }

    Ok(())
}
