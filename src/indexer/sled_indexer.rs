use sled::Db;

use crate::common::label::{Label, Labels};

use crate::common::time_series::TimeSeriesId;
use crate::{Builder, HasTypeName, MonolithErr, Result};
use std::ops::Add;
use std::path::{Path, PathBuf};

use crate::chunk::ChunkOpts;
use crate::common::option::DbOpts;
use crate::indexer::Indexer;
use crate::utils::intersect_time_series_id_vec;

const LABEL_REVERSE_PREFIX: &str = "LR";
const LABEL_PREFIX: &str = "L";
const ID_PREFIX: &str = "I";

///
/// Sled based indexer, use to search timeseries id based on metadata.
///
/// SledIndexer will establish three kinds of mapping
/// 1. Reverse index mapping, from single label to list of ids, e.g LR<label_key>=<label_value> -> 1,2,3,4,5...
/// 2. index mapping, meta data for a single time series, from id to a list of labels, e.g I1 -> L<label_key>=<label_value>,<label_key>=<label_value>...
/// 3. labels set mapping, similar to second one but in reverse, e.g L<label_key>=<label_value>,<label_key>=<label_value>... -> 1
#[derive(Clone)]
pub struct SledIndexer {
    storage: Db,
}

impl SledIndexer {
    pub fn new(dir: &Path) -> Result<SledIndexer> {
        Ok(SledIndexer {
            storage: Db::start_default(dir)?,
        })
    }

    fn get(&self, key: &String) -> Result<Option<String>> {
        return match self.storage.get(key)? {
            Some(val) => Ok(Some(String::from_utf8(
                AsRef::<[u8]>::as_ref(&val).to_vec(),
            )?)),
            None => Ok(None),
        };
    }

    fn get_id(&self, label: &Label) -> Result<Option<Vec<TimeSeriesId>>> {
        let key = KvIndexerProcessor::encode_label(label);
        let value = self.get(&key)?;
        return if let Some(val_str) = value {
            let id_str: Vec<&str> = val_str.split(",").collect();
            let mut res = Vec::new();
            for id in id_str {
                res.push(id.parse::<u64>()?);
            }
            Ok(Some(res))
        } else {
            Ok(None)
        };
    }
}

impl HasTypeName for SledIndexer {
    fn get_type_name() -> &'static str {
        return "SledIndexer";
    }
}

impl Indexer for SledIndexer {
    fn get_series_metadata_contains_labels(
        &self,
        labels: Labels,
    ) -> Result<Vec<(TimeSeriesId, Labels)>> {
        let ids = self.get_series_id_contains_labels(labels)?;
        let mut res = Vec::new();
        for time_series_id in ids {
            let labels_str =
                self.get(&KvIndexerProcessor::encode_time_series_id(time_series_id))?;
            if labels_str.is_some() {
                let labels = KvIndexerProcessor::decode_labels(labels_str.unwrap(), false)?;
                res.push((time_series_id, labels))
            }
        }
        Ok(res)
    }

    fn get_series_id_contains_labels(&self, labels: Labels) -> Result<Vec<TimeSeriesId>> {
        let mut ts_vec = Vec::new();
        for label in labels.vec() {
            if let Some(ts) = self.get_id(label)? {
                ts_vec.push(ts as Vec<TimeSeriesId>);
            }
        }

        intersect_time_series_id_vec(ts_vec)
    }

    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Option<u64>> {
        if let Some(val) = self
            .storage
            .get(KvIndexerProcessor::encode_labels(&labels, true))?
        {
            let val_str = String::from_utf8(AsRef::<[u8]>::as_ref(&val).to_vec())?;
            return Ok(Some(val_str.parse::<TimeSeriesId>()?));
        }
        Ok(None)
    }

    fn create_index(&self, labels: Labels, time_series_id: u64) -> Result<()> {
        let tree = &self.storage;

        // from label set to time series id
        let label_key = KvIndexerProcessor::encode_labels(&labels, true);
        if tree.contains_key(label_key.clone())? {
            //duplicate label -> id pair
            return Err(MonolithErr::InternalErr(
                "Duplicate label => id pair found in storage".to_string(),
            ));
        }
        tree.set(label_key, format!("{}", time_series_id).into_bytes());

        // from time series to label set
        tree.set(
            KvIndexerProcessor::encode_time_series_id(time_series_id),
            KvIndexerProcessor::encode_labels(&labels, false).into_bytes(),
        );

        // from label to time series ids
        let keys: Vec<String> = labels
            .vec()
            .iter()
            .map(KvIndexerProcessor::encode_label)
            .collect();
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

pub struct SledIndexerBuilder {}

impl Builder<SledIndexer> for SledIndexerBuilder {
    fn build(
        &self,
        path: String,
        _: Option<&ChunkOpts>,
        _: Option<&DbOpts>,
    ) -> Result<SledIndexer> {
        SledIndexer::new(PathBuf::from(path).as_path().join("indexer").as_path())
    }

    fn write_to_chunk(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_from_chunk(&self, dir: &Path, _: Option<&ChunkOpts>) -> Result<Option<SledIndexer>> {
        Ok(Some(SledIndexer {
            storage: sled::Db::start_default(dir)?,
        }))
    }

    fn write_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }
}

impl SledIndexerBuilder {
    pub fn new() -> Self {
        SledIndexerBuilder {}
    }
}

#[cfg(test)]
mod tests {
    use crate::common::label::{Label, Labels};
    use crate::indexer::sled_indexer::{KvIndexerProcessor, SledIndexer};
    use crate::Result;
    use tempfile::TempDir;

    use crate::indexer::common::Indexer;

    #[test]
    fn test_encode_label() -> Result<()> {
        let mut labels = Labels::new();
        labels.add(Label::from_key_value("test1", "test1value"));
        labels.add(Label::from_key_value("test3", "test1value"));
        labels.add(Label::from_key_value("test2", "test1value"));
        let res = KvIndexerProcessor::encode_labels(&labels, true);
        assert_eq!(res, "Ltest1=test1value,test2=test1value,test3=test1value");
        Ok(())
    }

    #[test]
    fn test_create_index() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let indexer = SledIndexer::new(temp_dir.path())?;
        let mut labels = Labels::new();
        labels.add(Label::from_key_value("test1", "test1value"));
        labels.add(Label::from_key_value("test2", "test1value"));
        labels.add(Label::from_key_value("test3", "test1value"));
        indexer.create_index(labels, 1)?;

        let label1 = indexer.storage.get("LRtest1=test1value")?.unwrap();
        let label2 = indexer.storage.get("LRtest2=test1value")?.unwrap();
        let val_str_1 = String::from_utf8(AsRef::<[u8]>::as_ref(&label1).to_vec())?;
        let val_str_2 = String::from_utf8(AsRef::<[u8]>::as_ref(&label2).to_vec())?;
        assert_eq!(val_str_1, val_str_2);

        let mut another_labels = Labels::new();
        another_labels.add(Label::from_key_value("test1", "test1value"));
        indexer.create_index(another_labels, 2);
        let another_label1 = indexer.storage.get("LRtest1=test1value")?.unwrap();
        let another_val_str_1 = String::from_utf8(AsRef::<[u8]>::as_ref(&another_label1).to_vec())?;
        assert_eq!("1,2", another_val_str_1);

        let mut key = "I1".to_string();
        let val = indexer.get(&key)?.unwrap();
        assert_eq!("test1=test1value,test2=test1value,test3=test1value", val);

        key = "Ltest1=test1value,test2=test1value,test3=test1value".to_string();
        let val = indexer.get(&key)?.unwrap();
        assert_eq!("1", val);

        Ok(())
    }

    #[test]
    fn test_decode_labels() -> Result<()> {
        let labels_str = "Lkey1=value1,key2=value2";
        let labels = KvIndexerProcessor::decode_labels(labels_str.to_string(), true)?;
        assert_eq!(2, labels.len());
        assert_eq!("key1", *labels.vec().get(0).unwrap().key());
        assert_eq!("key2", *labels.vec().get(1).unwrap().key());
        assert_eq!("value1", *labels.vec().get(0).unwrap().value());
        assert_eq!("value2", *labels.vec().get(1).unwrap().value());
        Ok(())
    }
}

pub(crate) struct KvIndexerProcessor {}

impl KvIndexerProcessor {
    fn _encode_label(label: &Label) -> String {
        format!("{}={}", label.key(), label.value())
    }

    /// encode single label as key
    pub fn encode_label(label: &Label) -> String {
        format!(
            "{}{}",
            LABEL_REVERSE_PREFIX,
            KvIndexerProcessor::_encode_label(label)
        )
    }

    ///
    /// Encode a set of label into string with format key1=value1,key2=value2...
    /// Used to create a mapping from labels to time series id.
    ///
    /// set __with_prefix__ to true to add prefix on result
    pub fn encode_labels(labels: &Labels, with_prefix: bool) -> String {
        let mut time_series_meta = labels.clone();
        time_series_meta.sort();
        let mut res = String::new();
        for label in time_series_meta.vec() {
            let mut label_str = KvIndexerProcessor::_encode_label(label);
            label_str.push(',');
            res = res.add(label_str.as_str());
        }
        res.pop(); //remove last ,
        if with_prefix {
            res.insert_str(0, LABEL_PREFIX);
        }
        res
    }

    /// Decode a set of label string stored in SledIndexer
    ///
    /// if the string is value, set __with_prefix__ to false
    /// if the string is key, set __with_prefix__ to true
    pub fn decode_labels(labels_str: String, is_key: bool) -> Result<Labels> {
        let mut _labels_str = labels_str.clone();
        if is_key {
            _labels_str.replace_range(..LABEL_PREFIX.len(), "");
        }
        let pairs: Vec<&str> = _labels_str.split(",").collect();
        let mut res = Vec::new();
        for pair in pairs {
            // Per Prometheus doc https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
            // Label name must match regex [a-zA-Z_][a-zA-Z0-9_]*
            // Thus, we can always use the first = to split label name from label value
            let equal_idx = pair.find("=").ok_or(MonolithErr::InternalErr(
                "Cannot parse label data".to_string(),
            ))?;
            let (key, value_with_equal) = pair.split_at(equal_idx);
            let value = {
                let mut s = String::from(value_with_equal);
                s.remove(0);
                s
            };
            let label = Label::from_key_value(key, value.as_str());
            res.push(label)
        }
        Ok(Labels::from_vec(res))
    }

    pub fn encode_time_series_id(id: TimeSeriesId) -> String {
        format!("{}{}", ID_PREFIX, id)
    }

    pub fn decode_time_series_id(id_str: String) -> Result<TimeSeriesId> {
        let mut _id_str = id_str.clone();
        _id_str.replace_range(..ID_PREFIX.len(), "");
        Ok(_id_str.parse::<TimeSeriesId>()?)
    }
}
