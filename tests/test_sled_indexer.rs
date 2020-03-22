use monolith::indexer::common::Indexer;
use monolith::indexer::SledIndexer;
use monolith::label::{Label, Labels};
use monolith::Result;
use tempfile::TempDir;

//todo: add more test to cover Indexer trait
#[test]
fn test_create_index() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let indexer = SledIndexer::new(temp_dir.path())?;
    let mut labels = Labels::new();
    labels.add(Label::from("test1", "test1value"));
    labels.add(Label::from("test2", "test1value"));
    labels.add(Label::from("test3", "test1value"));
    indexer.create_index(labels.clone(), 1)?;

    let res = indexer.get_series_id_by_labels(labels)?.unwrap();
    assert_eq!(res, 1);

    Ok(())
}
