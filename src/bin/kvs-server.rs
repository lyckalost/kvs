use clap::{App, SubCommand, Arg};
use lazy_static::lazy_static;
use regex::Regex;
use std::env::current_dir;
use kvs::KvError;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::process::exit;
use std::str::FromStr;

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
    let server_app = App::new("kvs-server")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(Arg::with_name("engine").long("engine").help("ENTER ENGINE-NAME, kvs or sled")
            .validator(|v| -> Result<(), String> {
                if v.eq("kvs") || v.eq("sled") {
                    return Ok(());
                }
                Err(String::from("Engine must be kvs or sled"))
            })
            .takes_value(true)
        )
        .arg(Arg::with_name("addr").long("addr").help("ENTER IP:PORT")
            .default_value("127.0.0.1:4000").validator(is_valid_addr)
        ).get_matches();

    eprintln!("Version: {}", env!("CARGO_PKG_VERSION"));
    eprintln!("Addr: {}", server_app.value_of("addr").unwrap());
    eprintln!("Engine: {}", server_app.value_of("engine").unwrap());

    match log_engine_in_config_if_not_set(Engine::from_str(server_app.value_of("engine").unwrap()).unwrap()) {
        Err(e) => {
            exit(1);
        }
        _ => {}
    }
}

fn log_engine_in_config_if_not_set(engine: Engine) -> kvs::Result<()>{
    let mut config_path = current_dir()?.join("config").join("engine");
    fs::create_dir_all(&config_path);

    if config_path.join("kvs").exists() {
        match engine {
            Engine::kvs => Ok(()),
            Engine::sled => Err(kvs::KvError::InvalidArgument),
        }
    } else if config_path.join("sled").exists() {
        match engine {
            Engine::sled => Ok(()),
            Engine::kvs => Err(kvs::KvError::InvalidArgument),
        }
    } else {
        match engine {
            Engine::kvs => File::create(config_path.join("kvs")),
            Engine::sled => File::create(config_path.join("sled")),
        };
        Ok(())
    }

}

#[derive(Debug, PartialEq)]
enum Engine {
    kvs,
    sled,
}

impl FromStr for Engine {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err(())
        }
    }
}