use crate::common::time_series::TimeSeriesId;
use crate::Result;
use failure::_core::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Iter;

#[derive(Hash, Clone)]
pub struct Label {
    key: String,
    value: String,
}

///
/// Label will be sort with alphabet order but note that two label will only be equal if and only if
/// they have the same key and same value
impl Label {
    pub fn from(key: &str, value: &str) -> Label {
        Label {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    pub fn new(key: String, value: String) -> Label {
        Label { key, value }
    }

    pub fn key_value(self) -> (String, String) {
        (self.key, self.value)
    }

    pub fn key(&self) -> &String {
        &self.key
    }

    pub fn value(&self) -> &String {
        &self.value
    }
}

impl Ord for Label {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl Eq for Label {}

impl PartialEq for Label {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key) && self.value.eq(&other.value)
    }
}

#[derive(Clone)]
pub struct Labels(Vec<Label>);

impl Hash for Labels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for label in self.0.iter() {
            label.hash(state)
        }
    }
}

impl Labels {
    pub fn new() -> Labels {
        Labels(Vec::new())
    }

    pub fn add(&mut self, label: Label) {
        self.0.push(label)
    }

    pub fn len(&self) -> usize {
        return self.0.len();
    }

    pub fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new(); //todo: find a better hash, sort the labels first
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn vec(&self) -> &Vec<Label> {
        &self.0
    }

    pub fn sort(&mut self) {
        self.0.sort();
    }

    pub fn pop(&mut self) -> Option<Label> {
        self.0.pop()
    }
}

#[cfg(test)]
mod test {
    use crate::common::label::{Label, Labels};

    #[test]
    fn get_hash() {
        let mut labels = Labels::new();
        labels.add(Label::from("test", "test"));
        print!("{}", labels.get_hash())
    }

    #[test]
    fn sort_label() {
        let mut labels = Labels::new();
        labels.add(Label::from("test4", "test"));
        labels.add(Label::from("test1", "test3"));
        labels.add(Label::from("test2", "test4"));

        labels.sort();

        assert_eq!(labels.0.get(0).unwrap().key, "test1")
    }
}
