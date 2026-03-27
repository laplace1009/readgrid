use std::{
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Serialize, ser::SerializeMap};
use sqlx::{postgres::PgRow, sqlite::SqliteRow};

use crate::db::{
    DataPreview, ExportFormat, ExportRequest,
    query::{raw_pg_row, raw_sqlite_row},
};

pub(crate) const EXPORT_BATCH_SIZE: usize = 250;

pub fn write_preview_csv(preview: &DataPreview, path: &Path) -> Result<usize> {
    let mut writer = create_csv_writer(path)?;
    write_csv_header(&mut writer, &preview.columns, path)?;

    let mut rows_written = 0;
    for row in &preview.rows {
        write_csv_record(
            &mut writer,
            row.cells.iter().map(|cell| cell.display_value.as_str()),
            path,
        )?;
        rows_written += 1;
    }

    flush_csv_writer(&mut writer, path)?;
    Ok(rows_written)
}

pub fn write_preview_json(preview: &DataPreview, path: &Path) -> Result<usize> {
    let mut writer = create_json_writer(path)?;

    for row in &preview.rows {
        writer.write_row(
            &preview.columns,
            &row.cells
                .iter()
                .map(|cell| cell.raw_value.clone())
                .collect::<Vec<_>>(),
        )?;
    }

    writer.finish()
}

pub fn write_preview_export(preview: &DataPreview, request: &ExportRequest) -> Result<usize> {
    match request.format {
        ExportFormat::Csv => write_preview_csv(preview, &request.path),
        ExportFormat::Json => write_preview_json(preview, &request.path),
    }
}

pub(crate) fn create_csv_writer(path: &Path) -> Result<csv::Writer<std::fs::File>> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let file = std::fs::File::create(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    Ok(csv::Writer::from_writer(file))
}

pub(crate) fn create_json_writer(path: &Path) -> Result<JsonArrayWriter<BufWriter<std::fs::File>>> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let file = std::fs::File::create(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    Ok(JsonArrayWriter::new(
        BufWriter::new(file),
        path.to_path_buf(),
    ))
}

fn write_csv_header(
    writer: &mut csv::Writer<std::fs::File>,
    columns: &[String],
    path: &Path,
) -> Result<()> {
    writer
        .write_record(columns.iter())
        .with_context(|| format!("failed to write CSV header to {}", path.display()))?;
    Ok(())
}

fn write_csv_record<'a>(
    writer: &mut csv::Writer<std::fs::File>,
    row: impl IntoIterator<Item = &'a str>,
    path: &Path,
) -> Result<()> {
    writer
        .write_record(row)
        .with_context(|| format!("failed to write CSV row to {}", path.display()))?;
    Ok(())
}

fn flush_csv_writer(writer: &mut csv::Writer<std::fs::File>, path: &Path) -> Result<()> {
    writer
        .flush()
        .with_context(|| format!("failed to flush CSV writer for {}", path.display()))?;
    Ok(())
}

pub(crate) trait ExportRowWriter {
    fn write_row(&mut self, columns: &[String], values: &[Option<String>]) -> Result<()>;
    fn finish(self) -> Result<usize>
    where
        Self: Sized;
}

pub(crate) struct CsvExportWriter {
    writer: csv::Writer<std::fs::File>,
    path: PathBuf,
    rows_written: usize,
}

impl CsvExportWriter {
    pub(crate) fn new(path: &Path, columns: &[String]) -> Result<Self> {
        let mut writer = create_csv_writer(path)?;
        write_csv_header(&mut writer, columns, path)?;
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            rows_written: 0,
        })
    }
}

impl ExportRowWriter for CsvExportWriter {
    fn write_row(&mut self, _columns: &[String], values: &[Option<String>]) -> Result<()> {
        let rendered = values
            .iter()
            .map(|value| value.as_deref().unwrap_or("NULL"))
            .collect::<Vec<_>>();
        write_csv_record(&mut self.writer, rendered, &self.path)?;
        self.rows_written += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        flush_csv_writer(&mut self.writer, &self.path)?;
        Ok(self.rows_written)
    }
}

pub(crate) struct JsonArrayWriter<W: Write> {
    writer: W,
    path: PathBuf,
    rows_written: usize,
}

impl<W: Write> JsonArrayWriter<W> {
    fn new(writer: W, path: PathBuf) -> Self {
        Self {
            writer,
            path,
            rows_written: 0,
        }
    }

    fn write_row(&mut self, columns: &[String], values: &[Option<String>]) -> Result<()> {
        if self.rows_written == 0 {
            self.writer.write_all(b"[").with_context(|| {
                format!("failed to write JSON header to {}", self.path.display())
            })?;
        } else {
            self.writer.write_all(b",").with_context(|| {
                format!("failed to write JSON separator to {}", self.path.display())
            })?;
        }

        serde_json::to_writer(&mut self.writer, &JsonExportRow { columns, values })
            .with_context(|| format!("failed to write JSON row to {}", self.path.display()))?;
        self.rows_written += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        if self.rows_written == 0 {
            self.writer.write_all(b"[]").with_context(|| {
                format!(
                    "failed to write empty JSON array to {}",
                    self.path.display()
                )
            })?;
        } else {
            self.writer.write_all(b"]").with_context(|| {
                format!("failed to finalize JSON output for {}", self.path.display())
            })?;
        }

        self.writer
            .flush()
            .with_context(|| format!("failed to flush JSON writer for {}", self.path.display()))?;
        Ok(self.rows_written)
    }
}

struct JsonExportRow<'a> {
    columns: &'a [String],
    values: &'a [Option<String>],
}

impl Serialize for JsonExportRow<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (column, value) in self.columns.iter().zip(self.values.iter()) {
            map.serialize_entry(column, value)?;
        }
        map.end()
    }
}

impl<W: Write> ExportRowWriter for JsonArrayWriter<W> {
    fn write_row(&mut self, columns: &[String], values: &[Option<String>]) -> Result<()> {
        JsonArrayWriter::write_row(self, columns, values)
    }

    fn finish(self) -> Result<usize> {
        JsonArrayWriter::finish(self)
    }
}

pub(crate) fn write_pg_row(
    writer: &mut impl ExportRowWriter,
    row: &PgRow,
    columns: &[String],
) -> Result<usize> {
    let raw_values = raw_pg_row(row, columns.len());
    writer.write_row(columns, &raw_values)?;
    Ok(1)
}

pub(crate) fn write_sqlite_row(
    writer: &mut impl ExportRowWriter,
    row: &SqliteRow,
    columns: &[String],
) -> Result<usize> {
    let raw_values = raw_sqlite_row(row, columns.len());
    writer.write_row(columns, &raw_values)?;
    Ok(1)
}
