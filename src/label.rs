use std::collections::hash_map::DefaultHasher;
use std::hash::{Hasher, Hash};
use std::path::Iter;

#[derive(Hash)]
pub struct Label {
    key: String,
    value: String,
}

impl Label {
    pub fn from(key: &str, value: &str) -> Label {
        Label { key: key.to_string(), value: value.to_string() }
    }

    pub fn new(key: String, value: String) -> Label {
        Label { key, value }
    }

    pub fn key_value(self) -> (String, String) {
        (self.key, self.value)
    }
}

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

    pub fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new(); //todo: find a better hash, sort the labels first
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn vec(self) -> Vec<Label> {
        self.0
    }
}

#[cfg(test)]
mod test {
    use crate::label::{Labels, Label};
    use crate::time_point::*;

    #[test]
    fn get_hash() {
        let mut labels = Labels::new();
        labels.add(Label::from("test", "test"));
        print!("{}", labels.get_hash())
    }
}