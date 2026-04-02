use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    about = "Compute a condensed upper-triangle overlap-coefficient distance matrix from interface_msa_columns_a sets."
)]
struct Args {
    #[arg(long)]
    input_file: PathBuf,

    #[arg(long)]
    output_file: PathBuf,

    #[arg(long)]
    metadata_out: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct InterfacePayload {
    #[serde(default)]
    interface_msa_columns_a: Vec<u32>,
}

#[derive(Debug)]
struct InterfaceEntry {
    partner_domain: String,
    row_key: String,
    columns: Vec<u32>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let entries = load_entries(&args.input_file)?;

    if entries.len() < 2 {
        bail!(
            "need at least two non-empty interface_msa_columns_a entries, found {}",
            entries.len()
        );
    }

    write_condensed_overlap(&entries, &args.output_file)?;

    if let Some(metadata_out) = args.metadata_out.as_deref() {
        write_metadata(&entries, metadata_out)?;
    }

    let pair_count = condensed_size(entries.len());
    eprintln!(
        "wrote {} entries and {} distances to {}",
        entries.len(),
        pair_count,
        args.output_file.display()
    );

    Ok(())
}

fn load_entries(input_file: &Path) -> Result<Vec<InterfaceEntry>> {
    let reader = File::open(input_file)
        .with_context(|| format!("failed to open {}", input_file.display()))?;
    let parsed: BTreeMap<String, BTreeMap<String, InterfacePayload>> =
        serde_json::from_reader(reader)
            .with_context(|| format!("failed to parse {}", input_file.display()))?;

    let mut entries = Vec::new();

    for (partner_domain, rows) in parsed {
        for (row_key, payload) in rows {
            let mut columns = payload.interface_msa_columns_a;
            columns.sort_unstable();
            columns.dedup();
            if columns.is_empty() {
                continue;
            }

            entries.push(InterfaceEntry {
                partner_domain: partner_domain.clone(),
                row_key,
                columns,
            });
        }
    }

    Ok(entries)
}

fn write_condensed_overlap(entries: &[InterfaceEntry], output_file: &Path) -> Result<()> {
    if let Some(parent) = output_file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let file = File::create(output_file)
        .with_context(|| format!("failed to create {}", output_file.display()))?;
    let mut writer = BufWriter::new(file);
    let worker_count = available_worker_count().min(entries.len().saturating_sub(1).max(1));
    let batches = build_row_batches(entries.len(), worker_count);

    if worker_count <= 1 || batches.len() <= 1 {
        write_batches_sequential(entries, &batches, &mut writer, output_file)?;
    } else {
        write_batches_parallel(entries, &batches, worker_count, &mut writer, output_file)?;
    }

    writer
        .flush()
        .with_context(|| format!("failed to flush {}", output_file.display()))?;

    Ok(())
}

fn available_worker_count() -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
}

fn row_output_bytes(entry_count: usize, left_index: usize) -> usize {
    (entry_count - left_index - 1) * std::mem::size_of::<u16>()
}

fn batch_target_bytes(entry_count: usize, worker_count: usize) -> usize {
    let total_bytes = condensed_size(entry_count) * std::mem::size_of::<u16>();
    let target = total_bytes / worker_count.saturating_mul(8).max(1);
    target.clamp(256 * 1024, 4 * 1024 * 1024)
}

fn build_row_batches(entry_count: usize, worker_count: usize) -> Vec<(usize, usize)> {
    let row_count = entry_count.saturating_sub(1);
    if row_count == 0 {
        return Vec::new();
    }

    let target_bytes = batch_target_bytes(entry_count, worker_count);
    let mut batches = Vec::new();
    let mut batch_start = 0usize;

    while batch_start < row_count {
        let mut batch_end = batch_start;
        let mut batch_bytes = 0usize;

        while batch_end < row_count {
            let row_bytes = row_output_bytes(entry_count, batch_end);
            if batch_end > batch_start && batch_bytes + row_bytes > target_bytes {
                break;
            }
            batch_bytes += row_bytes;
            batch_end += 1;
        }

        batches.push((batch_start, batch_end));
        batch_start = batch_end;
    }

    batches
}

fn build_batch_buffer(entries: &[InterfaceEntry], batch_start: usize, batch_end: usize) -> Vec<u8> {
    let entry_count = entries.len();
    let capacity = (batch_start..batch_end)
        .map(|left_index| row_output_bytes(entry_count, left_index))
        .sum();
    let mut buffer = Vec::with_capacity(capacity);

    for left_index in batch_start..batch_end {
        let left = &entries[left_index].columns;
        for right_index in left_index + 1..entry_count {
            let right = &entries[right_index].columns;
            let distance = overlap_distance(left, right);
            let quantized = quantize_unit_interval(distance);
            buffer.extend_from_slice(&quantized.to_le_bytes());
        }
    }

    buffer
}

fn write_batches_sequential<W: Write>(
    entries: &[InterfaceEntry],
    batches: &[(usize, usize)],
    writer: &mut W,
    output_file: &Path,
) -> Result<()> {
    for &(batch_start, batch_end) in batches {
        let buffer = build_batch_buffer(entries, batch_start, batch_end);
        writer.write_all(&buffer).with_context(|| {
            format!(
                "failed to write rows {}..{} to {}",
                batch_start,
                batch_end,
                output_file.display()
            )
        })?;
    }
    Ok(())
}

fn write_batches_parallel<W: Write>(
    entries: &[InterfaceEntry],
    batches: &[(usize, usize)],
    worker_count: usize,
    writer: &mut W,
    output_file: &Path,
) -> Result<()> {
    thread::scope(|scope| -> Result<()> {
        let next_batch = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = mpsc::channel::<(usize, Vec<u8>)>();

        for _ in 0..worker_count {
            let sender = sender.clone();
            let next_batch = Arc::clone(&next_batch);
            scope.spawn(move || loop {
                let batch_index = next_batch.fetch_add(1, Ordering::Relaxed);
                if batch_index >= batches.len() {
                    break;
                }
                let (batch_start, batch_end) = batches[batch_index];
                let buffer = build_batch_buffer(entries, batch_start, batch_end);
                if sender.send((batch_index, buffer)).is_err() {
                    break;
                }
            });
        }
        drop(sender);

        let mut pending_buffers = BTreeMap::new();
        let mut next_batch_to_write = 0usize;

        for (batch_index, buffer) in receiver {
            pending_buffers.insert(batch_index, buffer);
            while let Some(buffer) = pending_buffers.remove(&next_batch_to_write) {
                let (batch_start, batch_end) = batches[next_batch_to_write];
                writer.write_all(&buffer).with_context(|| {
                    format!(
                        "failed to write rows {}..{} to {}",
                        batch_start,
                        batch_end,
                        output_file.display()
                    )
                })?;
                next_batch_to_write += 1;
            }
        }

        if next_batch_to_write != batches.len() {
            bail!(
                "parallel overlap computation ended early: wrote {} of {} batches",
                next_batch_to_write,
                batches.len()
            );
        }

        Ok(())
    })
}

fn write_metadata(entries: &[InterfaceEntry], metadata_out: &Path) -> Result<()> {
    if let Some(parent) = metadata_out.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let file = File::create(metadata_out)
        .with_context(|| format!("failed to create {}", metadata_out.display()))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(b"index\tpartner_domain\trow_key\tcolumn_count\n")?;

    for (index, entry) in entries.iter().enumerate() {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}",
            index,
            entry.partner_domain,
            entry.row_key,
            entry.columns.len()
        )?;
    }

    writer
        .flush()
        .with_context(|| format!("failed to flush {}", metadata_out.display()))?;

    Ok(())
}

fn overlap_distance(left: &[u32], right: &[u32]) -> f64 {
    if left.is_empty() && right.is_empty() {
        return 0.0;
    }
    if left.is_empty() || right.is_empty() {
        return 1.0;
    }

    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut intersection = 0usize;

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                left_index += 1;
            }
            std::cmp::Ordering::Greater => {
                right_index += 1;
            }
            std::cmp::Ordering::Equal => {
                intersection += 1;
                left_index += 1;
                right_index += 1;
            }
        }
    }

    let minimum_size = left.len().min(right.len());
    1.0 - (intersection as f64 / minimum_size as f64)
}

fn quantize_unit_interval(value: f64) -> u16 {
    let clamped = value.clamp(0.0, 1.0);
    (clamped * f64::from(u16::MAX)).round() as u16
}

fn condensed_size(entry_count: usize) -> usize {
    entry_count * (entry_count - 1) / 2
}
