# treasuredata_fdw
[<img src="https://travis-ci.org/komamitsu/treasuredata_fdw.svg?branch=master"/>](https://travis-ci.org/komamitsu/treasuredata_fdw)

PostgreSQL Foreign Data Wrapper for Treasure Data

## Installation
This FDW uses [td-client-rust](https://github.com/komamitsu/td-client-rust). So you need to install [Rust](https://www.rust-lang.org/) first.

With [PGXN client](http://pgxnclient.projects.pgfoundry.org/):

```
$ pgxn install treasuredata_fdw
```

From source:

```
$ git clone https://github.com/komamitsu/treasuredata_fdw.git
$ cd treasuredata_fdw
$ make && sudo make install
```

When building this FDW on macOS, you may fail to build due to missing OpenSSL header files (https://github.com/sfackler/rust-openssl/issues/255). The following commands would solve the error.
```
export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include
export DEP_OPENSSL_INCLUDE=/usr/local/opt/openssl/include
```

## Setup
Connect to your PostgreSQL and create an extension and foreign server

```
CREATE EXTENSION treasuredata_fdw;

CREATE SERVER treasuredata_server FOREIGN DATA WRAPPER treasuredata_fdw;
```

## Update version

To update an existing treasuredata_fdw installation from versions earlier than 1.1 you can take the following steps:

- Download and install treasuredata_fdw version 1.1 using instructions from the "Instllation" section
- Restart the PostgreSQL server
- Run

```
ALTER EXTENSION treasuredata_fdw UPDATE;
```

## Usage
Specify your API key, database, query engine type ('presto' or 'hive') in CREATE FOREIGN TABLE statement. You can specify either your table name or query for Treasure Data directly.

```
CREATE FOREIGN TABLE sample_datasets (
    time integer,
    "user" varchar,
    host varchar,
    path varchar,
    referer varchar,
    code integer,
    agent varchar,
    size integer,
    method varchar
)
SERVER treasuredata_server OPTIONS (
    apikey 'your_api_key',
    database 'sample_datasets',
    query_engine 'presto',
    table 'www_access',
    import_file_size '67108864',
    atomic_import 'true'
);

SELECT code, count(1)
FROM sample_datasets
WHERE time BETWEEN 1412121600 AND 1414800000
GROUP BY code;

 code | count
------+-------
  404 |    17
  200 |  4981
  500 |     2
(3 rows)

```

Also, you can specify other API endpoint.

```
SERVER treasuredata_fdw OPTIONS (
    endpoint 'https://ybi.jp-east.idcfcloud.com'
    apikey 'your_api_key',
        :
```

## Table Options

- apikey : API Key for Treasure Data. See [Get API Keys](https://docs.treasuredata.com/articles/get-apikey).
- database : Database name on Treasure Data that the foreign table corresponds to.
- table : Table name on Treasure Data that the foreign table corresponds to.
- query_engine : Query engine name (`presto` or `hive`) that queries on the foreign table use.
- endpoint: Treasure Data's API endpoint (optional).
- import_file_size : Approximate maximum size of chunk files uploaded to Treasure Data. The default value is `134217728` (128MB).
- atomic_import : Flag (`true` or `false`) of whether uploaded chunk files get visible atomically. The default value is `false`


## INSERT INTO statement

This FDW supports `INSERT INTO` statement. With `atomic_import` is `false`, the FDW imports INSERTed rows as follows.

1. At the beginning of `INSERT INTO` query, an empty chunk file is created.
2. Each INSERTed row is appended to the chunk file.
3. If the written size exceeds a threshold specified by `import_file_size`, the chunk file is uploaded to Treasure Data and imported into the target table. And then a new empty chunk file is created again.
4. When all INSERTed rows are appended, the last chunk file is uploaded to Treasure Data and imported into the target table.

With `atomic_import` is `true`, the FDW imports INSERTed rows as follows.

1. At the beginning of `INSERT INTO` query, an empty chunk file is created. And a temporary table is created on Treasure Data.
2. Each INSERTed row is appended to the chunk file.
3. If the written size exceeds a threshold specified by `import_file_size`, the chunk file is uploaded to Treasure Data and imported into the temporary table. And then a new empty chunk file is created again.
4. When all INSERTed rows are appended, the last chunk file is uploaded to Treasure Data and imported into the temporary table.
5. Finally, the imported rows in the temporary table are atomically copied to the target table using `INSERT INTO (target table) SELECT * FROM (temporary table)` query on Treasure Data.

Pros and Cons of `atomic_import` are:

- Pros : Even if some chunk files are uploaded and imported to Treasure Data, they are rolled back when the `INSERT INTO` query is aborted after that.
- Cons : It needs to issue an `INSERT INTO (target table) SELECT * FROM (temporary table)` query on Treasure Data. It takes an extra time and resource to finish the `INSERT INTO` statement.


## Prepare Linux development environment

```
$ docker/build.sh
$ docker/run.sh
```
And then, follow the instructions from `run.sh`.

## Regression test

```
$ TD_TEST_APIKEY=<your_api_key> ./setup_regress <hive|presto>
$ make installcheck
```
