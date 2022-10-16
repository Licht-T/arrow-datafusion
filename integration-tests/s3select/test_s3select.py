# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto3
import io
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from jinja2 import Environment, FileSystemLoader

from util import generate_csv_from_datafusion

# Test queries
root = Path(os.path.dirname(__file__)) / "sqls"
test_files = set(root.glob("*.sql"))

# Test data prep
data_file = tempfile.NamedTemporaryFile(suffix=".csv")

df = pd.read_csv("testing/data/csv/aggregate_test_100.csv")
df["c14"] = pd.to_datetime(df.c9, unit="s")

df = pd.concat([df for _ in range(1000)], ignore_index=True)
df.to_csv(data_file.file, index_label="c0", date_format="%Y-%m-%d")
data_file.flush()

bucket_name = "licht-t-test"
key_name = "datafusion-testdata.csv"

s3_client = boto3.client('s3')
s3_client.upload_file(data_file.name, bucket_name, key_name)

# CREATE TABLE query prep
normal_create_table_file = tempfile.NamedTemporaryFile(suffix=".sql")
s3select_create_table_file = tempfile.NamedTemporaryFile(suffix=".sql")

env = Environment(loader=FileSystemLoader("integration-tests/"))
template = env.get_template("s3select/create_test_table.sql")

rendered = template.render({"file_path": f"s3select://{bucket_name}/{key_name}"})
s3select_create_table_file.write(rendered.encode("utf-8"))
s3select_create_table_file.flush()

rendered = template.render({"file_path": data_file.name})
normal_create_table_file.write(rendered.encode("utf-8"))
normal_create_table_file.flush()


class TestS3Select:
    def test_tests_count(self):
        assert len(test_files) == 6, "tests are missed"

    @pytest.mark.parametrize("fname", test_files, ids=str)
    def test_sql_file(self, fname):
        datafusion_output = pd.read_csv(io.BytesIO(generate_csv_from_datafusion(normal_create_table_file.name, fname)))
        datafusion_s3_select_output = pd.read_csv(io.BytesIO(
            generate_csv_from_datafusion(s3select_create_table_file.name, fname)))
        pd.testing.assert_frame_equal(datafusion_output, datafusion_s3_select_output)
