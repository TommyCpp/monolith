use crate::proto::LabelMatcher;
use failure::_core::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Hash, Clone)]
pub struct Label {
    key: String,
    value: String,
}

///
/// Label will be sort with alphabet order but note that two label will only be equal if and only if
/// they have the same key and same value
impl Label {
    pub fn new(key: String, value: String) -> Label {
        Label { key, value }
    }
    pub fn from_label_matcher(label_matcher: &LabelMatcher) -> Label {
        Label {
            key: label_matcher.name.clone(),
            value: label_matcher.value.clone(),
        }
    }

    pub fn from_key_value(key: &str, value: &str) -> Label {
        Label {
            key: key.to_string(),
            value: value.to_string(),
        }
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

impl From<&Label> for crate::proto::Label {
    fn from(l: &Label) -> Self {
        crate::proto::Label {
            name: l.key.clone(),
            value: l.value.clone(),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

impl From<&crate::proto::Label> for Label {
    fn from(l: &crate::proto::Label) -> Self {
        Label {
            key: l.name.clone(),
            value: l.value.clone(),
        }
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

impl Eq for Labels {}

impl PartialEq for Labels {
    fn eq(&self, other: &Self) -> bool {
        for i in 0..self.len() {
            if !self.0.get(i).unwrap().eq(other.0.get(i).unwrap()) {
                return false;
            }
        }
        true
    }
}

impl Labels {
    pub fn new() -> Labels {
        Labels(Vec::new())
    }

    pub fn from_vec(label_vec: Vec<Label>) -> Labels {
        Labels(label_vec)
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
        labels.add(Label::from_key_value("test", "test"));
        print!("{}", labels.get_hash())
    }

    #[test]
    fn sort_label() {
        let mut labels = Labels::new();
        labels.add(Label::from_key_value("test4", "test"));
        labels.add(Label::from_key_value("test1", "test3"));
        labels.add(Label::from_key_value("test2", "test4"));

        labels.sort();

        assert_eq!(labels.0.get(0).unwrap().key, "test1")
    }
}
