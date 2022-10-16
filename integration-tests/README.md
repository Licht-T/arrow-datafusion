<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Integration Tests

There are two parts:
- `psql`
    
    These test run SQL queries against both DataFusion and Postgres and compare the results for parity.
- `s3select`
    
    These test compares the results of the local mode and the S3 Select predicate pushdown mode.
    

## Setup

### `psql`
Set the following environment variables as appropriate for your environment. They are all optional.

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_HOST`
- `POSTGRES_PORT`

If `podman` is available, you can build a PostgreSQL by the following command:

```bash
podman run --name postgres \
-e POSTGRES_HOST_AUTH_METHOD=trust \
-e POSTGRES_INITDB_ARGS="--encoding=UTF-8 --lc-collate=C --lc-ctype=C" \
-p 5432:5432 \
-d docker.io/postgres:14
```

Create a Postgres database and then create the test table by running this script from **the root of the repository**:

```bash
psql -d "$POSTGRES_DB" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
  -f integration-tests/psql/create_test_table_postgres.sql
```

Populate the table by running this command from **the root of the repository**:

```bash
psql -d "$POSTGRES_DB" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
  -c "\copy test FROM '$(pwd)/testing/data/csv/aggregate_test_100.csv' WITH (FORMAT csv, HEADER true);"
```

### `s3select`
Set the following environment variables as appropriate for your environment.

- `AWS_DEFAULT_REGION`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_ACCESS_KEY_ID`
- `S3_BUCKET_NAME`

If you have already set up `awscli` on your system, you can set the AWS SDK variables as follows:
```bash
export AWS_DEFAULT_REGION=$(aws configure get default.region)
export AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_secret_access_key)
export AWS_ACCESS_KEY_ID=$(aws configure get default.aws_access_key_id)
```

## Run Tests

Run `pytest` from **the root of the repository**.
