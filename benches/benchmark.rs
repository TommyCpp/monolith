use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::cmp::min;
use std::ops::Index;
use std::collections::HashSet;
use std::iter::FromIterator;


fn intersect_order_vec(vec1: Vec<u64>, vec2: Vec<u64>) -> Vec<u64> {
    let mut res: Vec<u64> = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < vec1.len() && j < vec2.len() {
        if vec1[i] == vec2[j] {
            res.push(vec1[i]);
            i += 1;
            j += 1;
        } else if vec1.index(i) > vec2.index(j) {
            j += 1;
        } else {
            i += 1;
        }
    }
    res
}

fn intersect_vec(vec1: Vec<u64>, vec2: Vec<u64>) -> Vec<u64> {
    let set1: HashSet<u64> = vec1.into_iter().collect();
    let set2: HashSet<u64> = vec2.into_iter().collect();
    set1.intersection(&set2).cloned().collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("without order", |b| b.iter(|| intersect_vec(vec![1, 2, 3], vec![1, 3, 4, 5, 6])));
    c.bench_function("with order", |b| b.iter(|| intersect_order_vec(vec![1, 2, 3], vec![1, 3, 4, 5, 6])));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
