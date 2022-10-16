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

import subprocess


def generate_csv_from_datafusion(create_table_fname: str, fname: str):
    return subprocess.check_output(
        [
            "./datafusion-cli/target/debug/datafusion-cli",
            "-f",
            create_table_fname,
            "-f",
            fname,
            "--format",
            "csv",
            "-q",
        ],
    )
