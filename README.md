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
   * Local cluster: `jdbc:ydb:grpcs://<host>:2136/Root/testdb` + username and password configured
   * Connect with token to the cloud instance: `jdbc:ydb:grpcs://<host>:2135/path/to/database?token=file:~/my_token`
   * Connect with service account to the cloud instance: `jdbc:ydb:grpcs://<host>:2136/path/to/database?saFile=file:~/sa_key.json`
3) Execute queries, see example in [YdbDriverExampleTest.java](src/test/java/tech/ydb/jdbc/YdbDriverExampleTest.java)

## Authentication modes

YDB JDBC Driver supports the following [authentication modes](https://ydb.tech/en/docs/reference/ydb-sdk/auth):
* Anonymous: no authentication, used when username and password are not specified and no other authentication properties configured;
* Static Credentials: used when username and password are specified;
* Access Token: used when `token` property is configured, needs YDB authentication token as printed by the `ydb auth get-token` CLI command;
* Metadata: used when `useMetadata` property is set to `true`, extracts the authentication data from the metadata of a virtual machine, serverless container or a serverless function running in a cloud environment;
* Service Account Key: used when `saFile` property is configured, extracts the service account key and uses it for authentication.

## Driver properties reference

Driver supports the following configuration properties, which can be specified in the URL or passed via extra properties:
* `saFile` - service account key for authentication, can be passed either as literal JSON value or as a file reference;
* `token` - token value for authentication, can be passed either as literal value or as a file reference;
* `useMetadata` - boolean value, true if metadata authentication should be used, false otherwise (and default);
* `localDatacenter` - name of the datacenter local to the application being connected;
* `secureConnection` - boolean value, true if TLS should be enforced (normally configured via `grpc://` or `grpcs://` scheme in the JDBC URL);
* `secureConnectionCertificate` - custom CA certificate for TLS connections, can be passed either as literal value or as a file reference.

File references for `saFile`, `token` or `secureConnectionCertificate` must be prefixed with the `file:` URL scheme, for example:
* `saFile=file:~/mysaley1.json`
* `token=file:/opt/secret/token-file`
* `secureConnectionCertificate=file:/etc/ssl/cacert.cer`
