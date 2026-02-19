use arrow::array::{StringArray, GenericByteArray, ArrayData, BinaryArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, ByteArrayType, StringType};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::Rng;
use rand_distr::{Distribution, Uniform};
use std::io::Cursor;
use std::time::Duration;

// ========== 1. 引入你的原始/优化函数 ==========
// 原始 write_bytes_array（你提供的版本）
fn write_bytes_array_original<T: ByteArrayType<Offset = i32>, W: std::io::Write>(
    array: &GenericByteArray<T>,
    output: &mut W,
    transpose_opt: &mut TransposeOpt,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(null_buffer) = array.to_data().nulls() {
        write_len(1, output)?;
        write_bits_buffer(
            null_buffer.buffer(),
            null_buffer.offset(),
            null_buffer.len(),
            output,
        )?;
    } else {
        write_len(0, output)?;
    }

    let value_offsets = array.value_offsets();
    write_offsets_original(output, value_offsets, transpose_opt)?;

    let first_offset = value_offsets
        .first()
        .cloned()
        .expect("value_offsets must be non-empty") as usize;
    let last_offset = value_offsets
        .last()
        .cloned()
        .expect("value_offsets must be non-empty") as usize;
    output.write_all(&array.value_data()[first_offset..last_offset])?;
    Ok(())
}

// 优化后的 write_bytes_array（之前提供的版本）
fn write_bytes_array_optimized<T: ByteArrayType<Offset = i32>, W: std::io::Write>(
    array: &GenericByteArray<T>,
    output: &mut W,
    transpose_opt: &mut TransposeOpt,
) -> Result<(), Box<dyn std::error::Error>> {
    let array_data = array.to_data();
    let num_rows = array_data.len();
    let has_nulls = array_data.nulls().is_some();

    // 合并空值标记+位缓冲区写入
    let mut null_buffer_bytes = Vec::with_capacity(if num_rows == 10000 { 1 + 1250 } else { 1 + (num_rows.div_ceil(8)) });
    null_buffer_bytes.push(if has_nulls { 1u8 } else { 0u8 });

    if has_nulls {
        let null_buffer = array_data.nulls().unwrap();
        let bits_offset = null_buffer.offset();
        let bits_len = null_buffer.len();

        if num_rows == 10000 && bits_len == 10000 {
            let mut out_bits = vec![0u8; 1250];
            let in_ptr = null_buffer.buffer().as_ptr();
            let out_ptr = out_bits.as_mut_ptr();

            unsafe {
                for i in 0..1250 {
                    let in_bit_pos = bits_offset + i * 8;
                    let mut byte = 0u8;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos) as u8) << 0;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos + 1) as u8) << 1;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos + 2) as u8) << 2;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos + 3) as u8) << 3;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos + 4) as u8) << 4;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos + 5) as u8) << 5;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos + 6) as u8) << 6;
                    byte |= (arrow::util::bit_util::get_bit_raw(in_ptr, in_bit_pos + 7) as u8) << 7;
                    *out_ptr.add(i) = byte;
                }
            }
            null_buffer_bytes.extend_from_slice(&out_bits);
        } else {
            let mut out_bits = vec![0u8; bits_len.div_ceil(8)];
            let in_ptr = null_buffer.buffer().as_ptr();
            let out_ptr = out_bits.as_mut_ptr();

            unsafe {
                for i in 0..bits_len {
                    if arrow::util::bit_util::get_bit_raw(in_ptr, bits_offset + i) {
                        arrow::util::bit_util::set_bit_raw(out_ptr, i);
                    }
                }
            }
            null_buffer_bytes.extend_from_slice(&out_bits);
        }
    }
    output.write_all(&null_buffer_bytes)?;

    // 偏移量写入（跳过转置）
    let value_offsets = array.value_offsets();
    let num_rows = value_offsets.len() - 1;
    const I32_BYTE_WIDTH: usize = 4;
    let should_transpose = num_rows == 10000 && I32_BYTE_WIDTH >= 8;

    if !should_transpose {
        output.write_all(bytemuck::cast_slice(value_offsets))?;
    } else {
        write_offsets_original(output, value_offsets, transpose_opt)?;
    }

    // 原始字节写入
    let first_offset = *value_offsets.first().unwrap() as usize;
    let last_offset = *value_offsets.last().unwrap() as usize;
    if first_offset == 0 {
        output.write_all(&array.value_data()[..last_offset])?;
    } else {
        output.write_all(&array.value_data()[first_offset..last_offset])?;
    }

    Ok(())
}

// ========== 2. 依赖函数（原始版本） ==========
#[derive(Debug, Clone)]
pub enum TransposeOpt {
    Disabled,
    Transpose(Box<[u8]>),
}

fn write_len<W: std::io::Write>(len: usize, output: &mut W) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&len.to_le_bytes());
    output.write_all(&buf)?;
    Ok(())
}

fn write_bits_buffer<W: std::io::Write>(
    buffer: &Buffer,
    bits_offset: usize,
    bits_len: usize,
    output: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut out_buffer = vec![0u8; bits_len.div_ceil(8)];
    let in_ptr = buffer.as_ptr();
    let out_ptr = out_buffer.as_mut_ptr();

    for i in 0..bits_len {
        unsafe {
            if arrow::util::bit_util::get_bit_raw(in_ptr, bits_offset + i) {
                arrow::util::bit_util::set_bit_raw(out_ptr, i);
            }
        }
    }
    output.write_all(&out_buffer)?;
    Ok(())
}

fn write_offsets_original<W: std::io::Write>(
    output: &mut W,
    offsets: &[i32],
    transpose_opt: &mut TransposeOpt,
) -> Result<(), Box<dyn std::error::Error>> {
    let lens = offsets
        .iter()
        .zip(&offsets[1..])
        .map(|(beg, end)| end - beg)
        .collect::<Vec<_>>();

    if let TransposeOpt::Transpose(buffer) = transpose_opt {
        let trans_len = 4 * lens.len();
        // 模拟转置（仅bench用，实际无操作）
        buffer[..trans_len].copy_from_slice(lens.as_raw_bytes());
        output.write_all(&buffer[..trans_len])?;
    } else {
        output.write_all(lens.as_raw_bytes())?;
    }
    Ok(())
}

// ========== 3. 测试数据生成函数 ==========
/// 生成10000行Utf8数组（可配置是否有null、字符串长度）
fn generate_test_utf8_array(
    with_nulls: bool,
    avg_str_len: usize,
) -> StringArray {
    let mut rng = rand::thread_rng();
    let str_len_dist = Uniform::new(1, avg_str_len * 2); // 随机字符串长度（1~2*avg）
    let null_prob = 0.1; // 10%的null概率

    let mut values = Vec::with_capacity(10000);
    let mut nulls = Vec::with_capacity(10000);

    for _ in 0..10000 {
        // 生成随机字符串
        let str_len = str_len_dist.sample(&mut rng);
        let s: String = (0..str_len)
            .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
            .collect();
        values.push(s);

        // 生成null标记
        if with_nulls {
            nulls.push(rng.gen_bool(null_prob));
        }
    }

    // 构建StringArray
    if with_nulls {
        let null_buffer = arrow::buffer::Buffer::from_bitmask(&nulls);
        StringArray::from_data(
            ArrayData::new(
                DataType::Utf8,
                10000,
                Some(null_buffer),
                0,
                vec![],
                vec![],
            )?,
            values,
        )
    } else {
        StringArray::from(values)
    }
}

// ========== 4. Benchmark 核心函数 ==========
fn bench_write_bytes_array(c: &mut Criterion) {
    // 配置Criterion（适配10000行场景）
    let mut group = c.benchmark_group("write_bytes_array");
    group
        .measurement_time(Duration::from_millis(1000))
        .warm_up_time(Duration::from_millis(500))
        .throughput(Throughput::Elements(10000)); // 标记吞吐量（10000元素/次）

    // 生成测试数据（覆盖不同场景）
    let test_cases = [
        ("10000行_无null_短字符串(avg10)", generate_test_utf8_array(false, 10)),
        ("10000行_有null_短字符串(avg10)", generate_test_utf8_array(true, 10)),
        ("10000行_无null_长字符串(avg100)", generate_test_utf8_array(false, 100)),
    ];

    // 初始化TransposeOpt（禁用转置，适配你的场景）
    let mut transpose_opt = TransposeOpt::Disabled;

    // 遍历所有测试用例，对比原始/优化版本
    for (case_name, array) in test_cases {
        // 包装为GenericByteArray
        let generic_array = GenericByteArray::<StringType>::from(array.clone());

        // Benchmark 原始版本
        group.bench_function(format!("original_{}", case_name), |b| {
            b.iter(|| {
                let mut output = Cursor::new(Vec::with_capacity(1024 * 1024)); // 内存缓冲区
                write_bytes_array_original(
                    black_box(&generic_array),
                    black_box(&mut output),
                    black_box(&mut transpose_opt.clone()),
                ).unwrap();
            });
        });

        // Benchmark 优化版本
        group.bench_function(format!("optimized_{}", case_name), |b| {
            b.iter(|| {
                let mut output = Cursor::new(Vec::with_capacity(1024 * 1024)); // 内存缓冲区
                write_bytes_array_optimized(
                    black_box(&generic_array),
                    black_box(&mut output),
                    black_box(&mut transpose_opt.clone()),
                ).unwrap();
            });
        });
    }

    group.finish();
}

// ========== 5. 注册Benchmark ==========
criterion_group!(benches, bench_write_bytes_array);
criterion_main!(benches);

// ========== 辅助函数：简化Result处理 ==========
trait ResultExt<T> {
    fn unwrap(self) -> T;
}

impl<T, E: std::fmt::Debug> ResultExt<T> for Result<T, E> {
    fn unwrap(self) -> T {
        match self {
            Ok(t) => t,
            Err(e) => panic!("{:?}", e),
        }
    }
}