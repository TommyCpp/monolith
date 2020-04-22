use monolith::time_point::TimePoint;
use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

fn vec_u8_serialization(time_point: TimePoint) -> Vec<u8> {
    let timestamp_byte = time_point.timestamp.to_be_bytes();
    let value_byte = time_point.value.to_be_bytes();
    let mut res = Vec::from(&value_byte[..]);
    res.extend_from_slice(&timestamp_byte[..]);
    res
}

fn array_u8_serialization(time_point: TimePoint) -> [u8; 16]{
    let timestamp_byte = time_point.timestamp.to_be_bytes();
    let value_byte = time_point.value.to_be_bytes();
    let mut res = [0x00 as u8; 16];
    res[..8].clone_from_slice(&timestamp_byte);
    res[8..].clone_from_slice(&value_byte);
    res
}

fn string_serialization(time_point: TimePoint) -> String {
    format!("{},{}", time_point.timestamp, time_point.value)
}

fn serialization_criterion_benchmark(c: &mut Criterion){
    let time_point = TimePoint::new(120000, 339978668.77);

    c.bench_function("vec u8 serialization", |b| {
        b.iter(||{
            vec_u8_serialization(time_point.clone());
        })
    });

    c.bench_function("array u8 serialization", |b| {
        b.iter(|| {
            array_u8_serialization(time_point.clone());
        })
    });

    c.bench_function("string serialization", |b| {
        b.iter(|| {
            string_serialization(time_point.clone());
        })
    });
}

criterion_group!(serialization, serialization_criterion_benchmark);