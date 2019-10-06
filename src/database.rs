use super::common::*;

use std::collections::BTreeMap;

pub struct Database<'d> {
    database: BTreeMap<&'d str, BTreeMap<&'d str, Vec<f64>>>
}

impl<'d> Database<'d> {
    pub fn new() -> Database<'d> {
        Database {
            database: BTreeMap::new()
        }
    }

    pub fn create_series(&mut self, name: &'d str, value: &'d str) -> (){
        if !self.database.contains_key(name) {
            self.database.insert(name, BTreeMap::new());
            ()
        } else {
            self.database.get_mut(name).unwrap().insert(value, Vec::new());
            ()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::database::Database;

    #[test]
    fn test_new_database() {
        Database::new();
    }

    #[test]
    fn test_insert_database() {
        let mut db = Database::new();
        db.create_series("test", "series");
        assert_eq!(db.database.len(), 1)
    }
}
