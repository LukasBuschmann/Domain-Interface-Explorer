use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

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

    for left_index in 0..entries.len() - 1 {
        let left = &entries[left_index].columns;
        for right_index in left_index + 1..entries.len() {
            let right = &entries[right_index].columns;
            let distance = overlap_distance(left, right);
            let quantized = quantize_unit_interval(distance);
            writer
                .write_all(&quantized.to_le_bytes())
                .with_context(|| {
                    format!(
                        "failed to write pair ({}, {}) to {}",
                        left_index,
                        right_index,
                        output_file.display()
                    )
                })?;
        }
    }

    writer
        .flush()
        .with_context(|| format!("failed to flush {}", output_file.display()))?;

    Ok(())
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
