/*
* Copyright 2025 iceberg-compaction
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::StreamExt;
use futures::future::try_join_all;
use iceberg::arrow::{FieldMatchMode, RecordBatchPartitionSplitter};
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, PartitionSpec, Schema};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, TaskWriter};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{CompactionExecutor, RewriteFilesStat};
use crate::CompactionError;
use crate::config::CompactionExecutionConfig;
use crate::error::Result;
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;

#[derive(Debug, Default)]
pub struct DataFusionExecutor {}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        let RewriteFilesRequest {
            file_io,
            schema,
            file_group,
            execution_config,
            partition_spec,
            metrics_recorder,
            location_generator,
            sort_order,
            format_version,
        } = request;
        let mut stats = RewriteFilesStat::default();
        stats.record_input(&file_group);
        let sort_order_id = sort_order.clone().map(|sort_order| sort_order.id as i32);

        // Extract parallelism before file_group is moved
        let executor_parallelism = file_group.executor_parallelism;
        let output_parallelism = file_group.output_parallelism;

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema.clone())
            .with_format_version(format_version)
            .with_input_data_files(file_group)
            .with_sort_order(sort_order.clone())
            .build()?;
        let (batches, input_schema) = DatafusionProcessor::new(
            execution_config.clone(),
            executor_parallelism,
            file_io.clone(),
        )?
        .execute(datafusion_task_ctx, output_parallelism)
        .await?;
        let arc_input_schema = Arc::new(input_schema);
        let mut futures = Vec::with_capacity(executor_parallelism);

        // build iceberg writer for each partition
        for mut batch_stream in batches {
            let location_generator = location_generator.clone();
            let schema = arc_input_schema.clone();
            let execution_config = execution_config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let metrics_recorder = metrics_recorder.clone();

            let future: JoinHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = tokio::spawn(async move {
                let mut data_file_writer = build_iceberg_data_file_writer(
                    execution_config.data_file_prefix.clone(),
                    location_generator,
                    schema,
                    file_io,
                    partition_spec,
                    sort_order_id,
                    execution_config,
                )?;

                // Process each record batch with metrics
                let mut fetch_batch_start = Instant::now();
                while let Some(batch_result) = batch_stream.as_mut().next().await {
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_datafusion_batch_fetch_duration(
                            fetch_batch_start.elapsed().as_millis() as f64,
                        );
                    }

                    let batch = batch_result?;

                    let record_count = batch.num_rows() as u64;
                    let batch_bytes = batch.get_array_memory_size() as u64;

                    // Write the batch
                    let write_start = Instant::now();
                    data_file_writer.write(batch).await?;
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_datafusion_batch_write_duration(
                            write_start.elapsed().as_millis() as f64,
                        );
                    }

                    // Record detailed batch stats
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_batch_stats(record_count, batch_bytes);
                    }

                    fetch_batch_start = Instant::now(); // Reset for next batch
                }

                Ok(data_file_writer.close().await?)
            });
            futures.push(future);
        }

        // collect all data files from all partitions
        let output_data_files: Vec<DataFile> = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .into_iter()
            .map(|res| res.map(|v| v.into_iter()))
            .collect::<Result<Vec<_>>>()
            .map(|iters| iters.into_iter().flatten().collect())?;

        stats.record_output(&output_data_files);

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stats,
        })
    }
}

pub fn build_iceberg_data_file_writer(
    data_file_prefix: String,
    location_generator: DefaultLocationGenerator,
    schema: Arc<Schema>,
    file_io: FileIO,
    partition_spec: Arc<PartitionSpec>,
    sort_order_id: Option<i32>,
    execution_config: Arc<CompactionExecutionConfig>,
) -> Result<Box<dyn IcebergWriter>> {
    let target_file_size =
        usize::try_from(execution_config.target_file_size_bytes).map_err(|_| {
            CompactionError::Config(format!(
                "target_file_size_bytes {} exceeds platform usize",
                execution_config.target_file_size_bytes
            ))
        })?;

    let data_file_builder = {
        // DataFusion rebuilds the projected Arrow schema and does not preserve
        // `PARQUET:field_id` metadata on every field. The projection names are
        // generated from the current Iceberg schema, so name matching is the
        // correct boundary contract for compaction output.
        let parquet_writer_builder = ParquetWriterBuilder::new_with_match_mode(
            execution_config.write_parquet_properties.clone(),
            schema.clone(),
            FieldMatchMode::Name,
        );

        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            data_file_prefix,
            Some(unique_uuid_suffix.to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            target_file_size,
            file_io,
            location_generator,
            file_name_generator,
        )
        .with_max_concurrent_closes(execution_config.max_concurrent_closes);

        DataFileWriterBuilder::new(rolling_writer_builder).sort_order_id(sort_order_id)
    };

    let partition_splitter = if partition_spec.is_unpartitioned() {
        None
    } else {
        Some(RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema.clone(),
            partition_spec.clone(),
        )?)
    };

    let iceberg_task_writer = TaskWriter::new_with_partition_splitter(
        data_file_builder,
        true,
        schema,
        partition_spec,
        partition_splitter,
    );

    Ok(Box::new(iceberg_task_writer))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Float32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type};
    use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
    use tempfile::TempDir;

    use super::build_iceberg_data_file_writer;
    use crate::config::CompactionExecutionConfig;

    #[tokio::test]
    async fn datafusion_writer_resolves_ea_field_4890_without_arrow_id_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_generator = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_owned(),
        );
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        4890,
                        "ea_schema_field",
                        Type::Primitive(PrimitiveType::Float),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(PartitionSpec::builder(schema.clone()).build().unwrap());
        let mut writer = build_iceberg_data_file_writer(
            "ea-field-id-regression".to_owned(),
            location_generator,
            schema,
            file_io,
            partition_spec,
            None,
            Arc::new(CompactionExecutionConfig::default()),
        )
        .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ea_schema_field",
            DataType::Float32,
            true,
        )]));
        assert_eq!(arrow_schema.field(0).metadata(), &HashMap::new());
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(Float32Array::from(vec![
            f32::NAN,
            1.0,
        ])) as ArrayRef])
        .unwrap();

        writer.write(batch).await.unwrap();
        let files = writer.close().await.unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count(), 2);
        assert_eq!(files[0].nan_value_counts(), &HashMap::from([(4890, 1)]));
    }
}
