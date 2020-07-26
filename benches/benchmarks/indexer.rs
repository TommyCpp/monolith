use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use monolith::ops::OrderIntersect;
use monolith::time_series::TimeSeriesId;
use monolith::utils::intersect_time_series_id_vec;
use monolith::Result;

fn non_concurrent_intersect(mut ts: Vec<Vec<TimeSeriesId>>) -> Result<Vec<TimeSeriesId>> {
    while ts.len() > 1 {
        let mut next_vec = Vec::new();
        for i in 0..(ts.len() - 1) {
            let next = ts.get(i).unwrap().order_intersect(ts.get(i + 1).unwrap());
            next_vec.push(next)
        }
        ts = next_vec;
    }
    let res = ts.get(0).unwrap();
    Ok(res.clone())
}

pub fn generate_time_series_id_vec(len: usize) -> Result<Vec<Vec<TimeSeriesId>>> {
    fn setup(len: usize) -> Vec<Vec<TimeSeriesId>> {
        let mut input = Vec::new();
        for i in 0..len {
            let v = (1u64..100000 - i as u64).collect();
            input.push(v);
        }

        input
    }
    Ok(setup(len))
}

fn intersect_criterion_benchmark(c: &mut Criterion) {
    let input = generate_time_series_id_vec(20).unwrap();

    c.bench_function("concurrent one", |b| {
        b.iter(|| intersect_time_series_id_vec(input.clone()))
    });
    c.bench_function("non concurrent one", |b| {
        b.iter(|| non_concurrent_intersect(input.clone()))
    });
}

criterion_group!(intersect, intersect_criterion_benchmark);
