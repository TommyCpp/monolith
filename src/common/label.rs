use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Iter;

#[derive(Hash, Clone)]
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

    pub fn key(&self) -> &String {
        &self.key
    }

    pub fn value(&self) -> &String {
        &self.value
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
}

#[cfg(test)]
mod test {
    use crate::common::label::{Labels, Label};

    #[test]
    fn get_hash() {
        let mut labels = Labels::new();
        labels.add(Label::from("test", "test"));
        print!("{}", labels.get_hash())
    }
}