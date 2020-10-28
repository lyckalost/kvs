use assert_cmd::prelude::*;
use kvs::{KvStore, Result, Storage, Index, LogPointer, FileId};
use predicates::ord::eq;
use predicates::str::{contains, is_empty, PredicateStrExt};
use std::process::Command;
use tempfile::TempDir;
use walkdir::{WalkDir};

// `kvs` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin("kvs").unwrap().assert().failure();
}

#[test]
fn storage_set() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(&temp_dir.path().to_path_buf()).unwrap();
    let seq = kvs::Sequencer::new().unwrap();
    let cmd = kvs::Command::Set {key: "key1".to_owned(), value: "value1".to_owned(), sequencer: seq};
    let expected = cmd.clone();

    let lp = storage.mutate(cmd).unwrap();

    assert_eq!(expected, storage.get(&lp).unwrap());
}

#[test]
fn storage_build_index() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(&temp_dir.path().to_path_buf()).unwrap();
    let seq1 = kvs::Sequencer::new().unwrap();
    let cmd1 = kvs::Command::Set {key: "key1".to_owned(), value: "value1".to_owned(), sequencer: seq1};
    let lp1 = storage.mutate(cmd1).unwrap();

    let seq2 = kvs::Sequencer::new().unwrap();
    let cmd2 = kvs::Command::Set {key: "key2".to_owned(), value: "value2".to_owned(), sequencer: seq2};
    let lp2 = storage.mutate(cmd2).unwrap();

    let mut index = Index::new();

    storage.build_index(&mut index).expect("FAIL");

    assert_eq!(lp1, index.get_index(&"key1".to_owned()).unwrap());
    assert_eq!(lp2, index.get_index(&"key2".to_owned()).unwrap());
}

#[test]
fn index_iter() {
    let mut index = Index::new();

    let seq1 = kvs::Sequencer::new().unwrap();
    let cmd1 = kvs::Command::Set {key: "key1".to_owned(), value: "value1".to_owned(), sequencer: seq1};
    let lp1 = LogPointer {start_pos: 0, len: 1, f_id: FileId {id: 0}};
    index.update_index(&cmd1, lp1.clone()).expect("FAIL");

    let seq2 = kvs::Sequencer::new().unwrap();
    let cmd2 = kvs::Command::Set {key: "key2".to_owned(), value: "value2".to_owned(), sequencer: seq2};
    let lp2 = LogPointer {start_pos: 1, len: 2, f_id: FileId {id: 0}};
    index.update_index(&cmd2, lp2.clone()).expect("FAIL");

    let mut found_key1 = false;
    let mut found_key2 = false;
    for (k, v) in (&mut index).into_iter() {
        if "key1".eq(k) {
            found_key1 = true;
            assert_eq!(v.0, lp1);
        }

        if "key2".eq(k) {
            found_key2 = true;
            assert_eq!(v.0, lp2);
        }
    }
    assert!(found_key1 && found_key2);
}

#[test]
fn index_update() {
    let mut index = Index::new();

    let seq1 = kvs::Sequencer::new().unwrap();
    let cmd1 = kvs::Command::Set {key: "key1".to_owned(), value: "value1".to_owned(), sequencer: seq1};
    let lp1 = LogPointer {start_pos: 0, len: 1, f_id: FileId {id: 0}};
    let expected = lp1.clone();
    index.update_index(&cmd1, lp1).expect("FAIL");

    assert_eq!(expected, index.get_index(cmd1.get_key()).unwrap())
}

#[test]
fn index_update_conflict() {
    let mut index = Index::new();

    let seq1 = kvs::Sequencer::new().unwrap();
    let seq2 = kvs::Sequencer::new().unwrap();

    let cmd1 = kvs::Command::Set {key: "key1".to_owned(), value: "value1".to_owned(), sequencer: seq2};
    let cmd2 = kvs::Command::Set {key: "key1".to_owned(), value: "value2".to_owned(), sequencer: seq1};
    let lp1 = LogPointer {start_pos: 0, len: 1, f_id: FileId {id: 0}};
    let lp2 = LogPointer {start_pos: 2, len: 3, f_id: FileId {id: 0}};

    index.update_index(&cmd1, lp1).expect("FAIL");

    // need to find out a way to get rid of this ugly assert
    match index.update_index(&cmd2, lp2) {
        Err(_) => {}
        _ => {assert!(false)}
    }
}

// `kvs -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs get <KEY>` should print "Key not found" for a non-existent key and exit with zero.
#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());
}

// `kvs rm <KEY>` should print "Key not found" for an empty database and exit with non-zero code.
#[test]
fn cli_rm_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stdout(eq("Key not found").trim());
}

// `kvs set <KEY> <VALUE>` should print nothing and exit with zero.
#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value1").trim());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value2").trim());

    Ok(())
}

// `kvs rm <KEY>` should print nothing and exit with zero.
#[test]
fn cli_rm_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());

    Ok(())
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}

// Should get previously stored value.
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

// Should overwrite existent value.
#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
    store.set("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

// Should get `None` when getting a non-existent key.
#[test]
fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    assert!(store.remove("key1".to_owned()).is_err());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert!(store.remove("key1".to_owned()).is_ok());
    assert_eq!(store.get("key1".to_owned())?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    // let p = Path::new("/Users/junwow/myGT/2020/TP201/kvs");
    let mut store = KvStore::open(temp_dir.path())?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut current_size = dir_size();
    for iter in 0..100 {
        for key_id in 0..100 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(key, value)?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered.

        drop(store);
        // reopen and check content.
        let mut store = KvStore::open(temp_dir.path())?;
        for key_id in 0..100 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key)?, Some(format!("{}", iter)));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}
