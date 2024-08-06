use test_log::test;
use value_log::{Config, ValueLog};

#[test]
fn vlog_load_v1() -> value_log::Result<()> {
    let path = std::path::Path::new("test_fixture/v1_vlog");

    let value_log = ValueLog::open(path, Config::default())?;

    let count = {
        let mut count = 0;

        for kv in value_log.get_reader()? {
            let _ = kv?;
            count += 1;
        }

        count
    };

    assert_eq!(4, count);
    assert_eq!(2, value_log.segment_count());
    assert_eq!(0, value_log.verify()?);

    Ok(())
}

#[test]
fn vlog_load_v1_corrupt() -> value_log::Result<()> {
    let path = std::path::Path::new("test_fixture/v1_vlog_corrupt");

    let value_log = ValueLog::open(path, Config::default())?;

    assert_eq!(2, value_log.verify()?);

    Ok(())
}

/* {
    let mut writer = value_log.get_writer()?;
    writer.write("a", "")?;
    writer.write("b", "")?;
    writer.write("c", "")?;
    writer.write("d", "")?;
    value_log.register_writer(writer)?;
}

{
    let mut writer = value_log.get_writer()?;
    writer.write("a", "We're caught between")?;
    writer.write("b", "This life and dream")?;
    writer.write("c", "But you and me we're bigger")?;
    writer.write("d", "Let's try to figure this out")?;
    value_log.register_writer(writer)?;
} */
