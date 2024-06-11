use test_log::test;
use value_log::{Config, ValueLog};

#[test]
fn vlog_load_v1() -> value_log::Result<()> {
    let path = std::path::Path::new("test_fixture/v1_vlog");

    let value_log = ValueLog::open(path, Config::default())?;

    assert_eq!(4, value_log.get_reader()?.count());

    Ok(())
}
