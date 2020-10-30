use clap::{App, SubCommand, Arg};
use lazy_static::lazy_static;
use std::process::exit;
use regex::Regex;

lazy_static! {
    static ref RE: Regex = Regex::new(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5})").unwrap();
}

fn is_valid_addr(v: String) -> Result<(), String> {
    if RE.is_match(v.as_str()) {
        return Ok(());
    }
    Err(String::from("Invalid Addr"))
}

fn main() {

    let client_app = App::new("kvs-client")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
                .arg(Arg::with_name("addr").long("addr").help("ENTER IP:PORT")
                    .default_value("127.0.0.1:4000").validator(is_valid_addr))
        )
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
                .arg(Arg::with_name("<VALUE>").help("ENTER A VALUE").required(true))
                .arg(Arg::with_name("addr").long("addr").help("ENTER IP:PORT")
                    .default_value("127.0.0.1:4000").validator(is_valid_addr))
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
                .arg(Arg::with_name("addr").long("addr").help("ENTER IP:PORT")
                    .default_value("127.0.0.1:4000").validator(is_valid_addr))
        ).get_matches();

    match client_app.subcommand() {
        ("get", Some(matches)) => {
            // let k = matches.value_of("<KEY>").expect("<KEY> argument is missing");
            // let addr = matches.value_of("addr").unwrap();
        },
        ("rm", Some(_)) => {

        },
        ("set", Some(_)) => {

        },
        _ => unreachable!()
    }
}