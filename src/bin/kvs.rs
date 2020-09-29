use std::env;
use clap::{App, SubCommand, Arg};
use kvs::{Result, KvError};


fn main() -> Result<()> {
    let kvs_app = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").index(1).required(true))
        )
        .get_matches();



    match kvs_app.subcommand() {
        ("get", Some(_)) =>  {
            println!("Key not found");
            Ok(())
        },
        _ => Err(KvError::InvalidArgument)
    }
}
