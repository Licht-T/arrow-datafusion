// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution plan for reading files by S3 Select
use crate::datasource::file_format::file_type::{FileCompressionType, FileType};
use crate::error::{DataFusionError, Result};

use crate::execution::context::TaskContext;
use crate::physical_plan::expressions::PhysicalSortExpr;

use crate::physical_plan::file_format::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::physical_plan::file_format::FileMeta;
use crate::physical_plan::metrics::ExecutionPlanMetricsSet;
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::json::reader::DecoderOptions;
use arrow::{datatypes::SchemaRef, json};

use aws_sdk_s3::model::{
    CompressionType, CsvInput, ExpressionType, FileHeaderInfo, InputSerialization,
    JsonInput, JsonOutput, JsonType, OutputSerialization, ParquetInput, ScanRange,
    SelectObjectContentEventStream,
};
use datafusion_expr::expr::generate_where_condition;
use datafusion_expr::Expr;

use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::ObjectStore;
use parquet::data_type::AsBytes;

use std::any::Any;

use std::cmp::{max, min};

use std::io::{Cursor, Read};

use std::str::FromStr;
use std::sync::Arc;

use url::Url;

use super::FileScanConfig;

const DEFAULT_DELIMITER: u8 = b',';
const DEFAULT_CHUNK_SIZE: usize = 100 * 1024;
const DEFAULT_BUFFER_SIZE: usize = 4;

/// Execution plan for scanning S3Select data source
#[derive(Debug, Clone)]
pub struct S3SelectExec {
    base_config: FileScanConfig,
    predicate: Option<Expr>,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    file_type: FileType,
    file_compression_type: FileCompressionType,
    has_header: bool,
    delimiter: u8,
    chunk_size: usize,
    buffer_size: usize,
}

impl S3SelectExec {
    /// Create a new S3 Select reader execution plan provided base configurations
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Expr>,
        file_type: FileType,
        file_compression_type: FileCompressionType,
    ) -> Self {
        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            predicate,
            projected_schema,
            projected_statistics,
            metrics: ExecutionPlanMetricsSet::new(),
            file_type,
            file_compression_type,
            has_header: true,
            delimiter: DEFAULT_DELIMITER,
            chunk_size: DEFAULT_CHUNK_SIZE,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// has_header setter
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// delimiter setter
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// chunk_size setter
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// buffer_size setter
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }
}

impl ExecutionPlan for S3SelectExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let proj = self.base_config.projected_file_column_names();

        let batch_size = context.session_config().batch_size();
        let file_schema = Arc::clone(&self.base_config.file_schema);
        let object_store_url = self.base_config.object_store_url.as_str();

        let options = DecoderOptions::new().with_batch_size(batch_size);
        let options = if let Some(proj) = proj {
            options.with_projection(proj)
        } else {
            options
        };

        let opener = S3SelectOpener {
            file_schema,
            predicate: self.predicate.to_owned(),
            options,
            file_type: self.file_type.to_owned(),
            file_compression_type: self.file_compression_type.to_owned(),
            has_header: self.has_header.to_owned(),
            delimiter: self.delimiter.to_owned(),
            object_store_url: object_store_url.to_string(),
            chunk_size: self.chunk_size.to_owned(),
            buffer_size: self.buffer_size.to_owned(),
        };

        let stream = FileStream::new(
            &self.base_config,
            partition,
            context,
            opener,
            self.metrics.clone(),
        )?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "S3SelectExec: limit={:?}, files={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

struct S3SelectOpener {
    options: DecoderOptions,
    predicate: Option<Expr>,
    file_schema: SchemaRef,
    file_type: FileType,
    file_compression_type: FileCompressionType,
    has_header: bool,
    delimiter: u8,
    object_store_url: String,
    chunk_size: usize,
    buffer_size: usize,
}

impl S3SelectOpener {
    fn get_s3_bucket_and_key(&self, file_meta: &FileMeta) -> Result<(String, String)> {
        let err_msg = "Unable to parse S3 path";

        let key = file_meta.location().as_ref().to_string();
        let bucket = Url::from_str(self.object_store_url.as_str())
            .map_err(|_| DataFusionError::Execution(err_msg.to_owned()))?
            .host_str()
            .ok_or(DataFusionError::Execution(err_msg.to_owned()))?
            .to_owned();

        Ok((bucket, key))
    }

    fn generate_sql(&self) -> String {
        let cond = generate_where_condition(
            self.predicate.as_ref(),
            self.file_schema.fields.as_ref(),
            self.file_type == FileType::CSV && !self.has_header,
        );
        let fields = match self.has_header {
            false => self
                .file_schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    format!("s.\"_{}\" AS \"{}\"", i + 1, v.name().replace('\"', "\"\""))
                })
                .join(", "),
            true => "*".to_string(),
        };

        format!("SELECT {} FROM s3object s {}", fields, cond)
    }
}

impl FileOpener for S3SelectOpener {
    fn open(
        &self,
        _store: Arc<dyn ObjectStore>,
        file_meta: FileMeta,
    ) -> Result<FileOpenFuture> {
        let file_size = file_meta.object_meta.size;
        let options = self.options.clone();
        let schema = self.file_schema.clone();
        let file_type = self.file_type;
        let file_compression_type = self.file_compression_type;
        let has_header = self.has_header;
        let chunk_size = self.chunk_size;
        let buffer_size = self.buffer_size;
        let delimiter = std::str::from_utf8(self.delimiter.as_bytes())
            .map_err(|_| {
                DataFusionError::Internal("Unable to convert delimiter to str".into())
            })?
            .to_string();

        let err_msg = "Unable to parse S3 path";

        let (bucket, key) = self.get_s3_bucket_and_key(&file_meta)?;
        let sql = self.generate_sql();

        Ok(Box::pin(async move {
            let reader: Box<dyn Read + Send> = Box::new(Cursor::new(Vec::<u8>::new()));

            let mut chunks: Box<dyn Iterator<Item = usize> + Send> =
                Box::new((0..=file_size).step_by(min(file_size, chunk_size)));

            if file_size > chunk_size && file_size % chunk_size != 0 {
                // Add end-of-file bytes.
                chunks = Box::new(chunks.chain(file_size..=file_size));
            }

            let res = futures::stream::iter(chunks.tuple_windows::<(usize, usize)>())
                .map(|(start, end)| {
                    let key = key.clone();
                    let bucket = bucket.clone();
                    let sql = sql.clone();
                    let delimiter = delimiter.clone();

                    tokio::spawn(async move {
                        // FIXME: Due to the bug of AWS SDK, a zero-start range doesn't work.
                        //        When zero-start, we put the dummy value `-1` as range start
                        //        and rewrite a request body in later step.
                        //        Once fixed, remove this logic.
                        //        https://github.com/awslabs/aws-sdk-rust/issues/630
                        let start = match start == 0 {
                            true => -1,
                            false => start as i64,
                        };

                        // Remove overlap
                        let end = match end >= file_size {
                            true => file_size as i64,
                            false => max((end as i64) - 1, 0),
                        };

                        let config = aws_config::from_env().load().await;
                        let client = aws_sdk_s3::Client::new(&config);

                        let mut cursor: Box<dyn Read + Send> =
                            Box::new(Cursor::new(Vec::<u8>::new()));

                        let builder = InputSerialization::builder();
                        let builder = match file_type {
                            FileType::JSON => Ok(builder.json(
                                JsonInput::builder().r#type(JsonType::Lines).build(),
                            )),
                            FileType::CSV => Ok(builder.csv(
                                CsvInput::builder()
                                    .file_header_info(match has_header {
                                        true => FileHeaderInfo::Use,
                                        false => FileHeaderInfo::None,
                                    })
                                    .field_delimiter(delimiter.to_owned())
                                    .build(),
                            )),
                            FileType::PARQUET => {
                                Ok(builder.parquet(ParquetInput::builder().build()))
                            }
                            _ => Err(DataFusionError::NotImplemented(
                                "S3 Select is only available for JSON/CSV/Parquet".into(),
                            )),
                        }?;

                        let request = client
                            .select_object_content()
                            .bucket(bucket.to_owned())
                            .key(key.to_owned())
                            .expression(sql.to_owned())
                            .expression_type(ExpressionType::Sql)
                            .input_serialization({
                                builder
                                    .compression_type(match file_compression_type {
                                        FileCompressionType::UNCOMPRESSED => {
                                            CompressionType::None
                                        }
                                        FileCompressionType::GZIP => {
                                            CompressionType::Gzip
                                        }
                                        FileCompressionType::BZIP2 => {
                                            CompressionType::Bzip2
                                        }
                                    })
                                    .build()
                            })
                            .output_serialization(
                                OutputSerialization::builder()
                                    .json(JsonOutput::builder().build())
                                    .build(),
                            )
                            .scan_range(
                                ScanRange::builder().start(start).end(end).build(),
                            )
                            .customize()
                            .await
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to execute S3 Select: {}",
                                    e
                                ))
                            })?;

                        // FIXME: Due to the bug of AWS SDK, a zero-start range doesn't work.
                        //        When zero-start, we rewrite the dummy value `-1`
                        //        in a request body to zero.
                        //        Once fixed, remove this logic.
                        //        https://github.com/awslabs/aws-sdk-rust/issues/630
                        let request = request.map_request::<DataFusionError>(|x| {
                            Ok(x.map(|y| {
                                y.map(|z| {
                                    let data = z.bytes().unwrap().to_owned();
                                    let body = String::from_utf8(data).unwrap();
                                    body.replace("<Start>-1</Start>", "<Start> 0</Start>")
                                        .into()
                                })
                            }))
                        })?;

                        println!("{:?}", request.request());

                        let mut select_object_content =
                            request.send().await.map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to execute S3 Select: {}",
                                    e
                                ))
                            })?;

                        while let stream = {
                            select_object_content
                                .payload
                                .recv()
                                .await
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "{}: {}",
                                        err_msg, e
                                    ))
                                })?
                                .ok_or(DataFusionError::Execution(format!(
                                    "1: {}",
                                    err_msg
                                )))?
                        } {
                            if let SelectObjectContentEventStream::Records(v) = stream {
                                let records = v
                                    .payload
                                    .ok_or(DataFusionError::Execution(format!(
                                        "2: {}",
                                        err_msg
                                    )))?
                                    .into_inner();
                                cursor = Box::new(cursor.chain(Cursor::new(records)));
                            } else if let SelectObjectContentEventStream::Stats(v) =
                                stream
                            {
                                println!("{:?}", v);
                            } else if let SelectObjectContentEventStream::Cont(v) = stream
                            {
                                println!("{:?}", v);
                            } else if let SelectObjectContentEventStream::Progress(v) =
                                stream
                            {
                                println!("{:?}", v);
                            } else {
                                break;
                            }
                        }
                        Ok::<Box<dyn Read + Send>, DataFusionError>(cursor)
                    })
                })
                .buffer_unordered(buffer_size)
                .map(|x| x?)
                .try_fold(reader, |x, y| async move {
                    let reader: Box<dyn Read + Send> = Box::new(x.chain(y));
                    Ok(reader)
                })
                .await?;

            let reader = json::Reader::new(res, schema.clone(), options);

            Ok(futures::stream::iter(reader).boxed())
        }))
    }
}
