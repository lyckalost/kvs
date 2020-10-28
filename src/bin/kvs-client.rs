use clap::{App, SubCommand, Arg};

fn main() {
    let client_app = App::new("kvs-client")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
                .arg(Arg::with_name("--addr").help("ENTER IP:PORT").required(false))
        )
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
                .arg(Arg::with_name("<VALUE>").help("ENTER A VALUE").required(true))
                .arg(Arg::with_name("--addr").help("ENTER IP:PORT").required(false))
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("<KEY>").help("ENTER A KEY").required(true))
                .arg(Arg::with_name("--addr").help("ENTER IP:PORT").required(false))
        ).get_matches();

    match client_app.subcommand() {
        ("get", Some(_)) => {

        },
        ("rm", Some(_)) => {

        },
        ("set", Some(_)) => {

        },
        _ => unreachable!()
    }
}