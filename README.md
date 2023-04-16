[Experimental] JDBC Driver for YDB
---
This is an experimental version of JDBC driver for YDB. It is in active development and is not intended for use in production environments.

## Building
All tests are run without Docker by default.
To enable all tests make sure you have Docker or Docker Machine installed then run `mvn install -DSKIP_DOCKER_TESTS=false`

## Quickstart

1) Drop in [JDBC driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases) to classpath or pick this file in IDEA
2) Connect to YDB
   * Local or remote Docker (anonymous authentication): `jdbc:ydb:grpc://localhost:2135/local`
   * Connect with token: `jdbc:ydb:grpc://<host>:2135/path/to/database&token=~/my_token`
   * Connect with service account: `jdbc:ydb:grpcs://<host>:2136/path/to/database&saFile=~/sa_key.json`
3) Execute queries, see example in [YdbDriverExampleTest.java](src/test/java/tech/ydb/jdbc/YdbDriverExampleTest.java)
