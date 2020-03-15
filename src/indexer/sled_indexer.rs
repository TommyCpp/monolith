use sled::Db;

use crate::common::label::{Label, Labels};

use crate::common::time_series::TimeSeriesId;
use crate::Result;
use std::ops::Add;
use std::path::Path;

use crate::indexer::common::{intersect_time_series_id_vec, Indexer};

const LABEL_REVERSE_PREFIX: &str = "LR";

///
/// Sled based indexer, use to search timeseries id based on metadata.
pub struct SledIndexer {
    storage: Db,
}

impl SledIndexer {
    fn new(dir: &Path) -> Result<SledIndexer> {
        Ok(SledIndexer {
            storage: Db::start_default(dir)?,
        })
    }

    fn encode_label(label: &Label) -> String {
        format!("{}={}", label.key(), label.value())
    }

    fn encode_labels(labels: &Labels) -> String {
        let mut time_series_meta = labels.clone();
        time_series_meta.sort();
        let mut res = String::new();
        for label in time_series_meta.vec() {
            res = res.add(format!("{}={},", label.key(), label.value()).as_str());
        }
        res.pop(); //remove last ,
        res
    }

    pub fn get(&self, label: &Label) -> Result<Option<Vec<TimeSeriesId>>> {
        let key = SledIndexer::encode_label(label);
        match self.storage.get(&key)? {
            Some(val) => {
                let val_str = String::from_utf8(AsRef::<[u8]>::as_ref(&val).to_vec())?;
                let id_str: Vec<&str> = val_str.split(",").collect();
                let mut res = Vec::new();
                for id in id_str {
                    res.push(id.parse::<u64>()?);
                }
                Ok(Some(res))
            }
            None => Ok(None),
        }
    }
}

impl Indexer for SledIndexer {
    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Vec<TimeSeriesId>> {
        let mut ts_vec = Vec::new();
        for label in labels.vec() {
            if let Some(ts) = self.get(label)? {
                ts_vec.push(ts as Vec<TimeSeriesId>);
            }
        }

        intersect_time_series_id_vec(ts_vec)
    }

    ///
    /// time_series_id must be single increasing.
    /// update_index will not re-sort the time_series_id in values
    fn update_index(&self, labels: Labels, time_series_id: u64) -> Result<()> {
        let tree = &self.storage;
        let keys: Vec<String> = labels.vec().iter().map(SledIndexer::encode_label).collect();
        for key in keys {
            let val = match tree.get(&key)? {
                None => format!("{}", time_series_id),
                Some(val) => {
                    let val_str = String::from_utf8(AsRef::<[u8]>::as_ref(&val).to_vec())?;
                    format!("{},{}", val_str, time_series_id)
                }
            };
            tree.set(&key, val.into_bytes());
            tree.flush();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::label::{Label, Labels};
    use crate::indexer::sled_indexer::SledIndexer;
    use crate::Result;
    use tempfile::TempDir;

    use crate::indexer::common::Indexer;

    #[test]
    fn test_update_index() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let indexer = SledIndexer::new(temp_dir.path())?;
        let mut labels = Labels::new();
        labels.add(Label::from("test1", "test1value"));
        labels.add(Label::from("test2", "test1value"));
        labels.add(Label::from("test3", "test1value"));
        indexer.update_index(labels, 1)?;

        let label1 = indexer.storage.get("test1=test1value")?.unwrap();
        let label2 = indexer.storage.get("test2=test1value")?.unwrap();
        let val_str_1 = String::from_utf8(AsRef::<[u8]>::as_ref(&label1).to_vec())?;
        let val_str_2 = String::from_utf8(AsRef::<[u8]>::as_ref(&label2).to_vec())?;
        assert_eq!(val_str_1, val_str_2);

        let mut another_labels = Labels::new();
        another_labels.add(Label::from("test1", "test1value"));
        indexer.update_index(another_labels, 2);
        let another_label1 = indexer.storage.get("test1=test1value")?.unwrap();
        let another_val_str_1 = String::from_utf8(AsRef::<[u8]>::as_ref(&another_label1).to_vec())?;
        assert_eq!("1,2", another_val_str_1);

        Ok(())
    }
}
