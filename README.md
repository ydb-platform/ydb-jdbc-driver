[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb-jdbc-driver/blob/master/LICENSE)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Frepo1.maven.org%2Fmaven2%2Ftech%2Fydb%2Fjdbc%2Fydb-jdbc-driver%2Fmaven-metadata.xml)](https://mvnrepository.com/artifact/tech.ydb.jdbc/ydb-jdbc-driver)
[![Build](https://img.shields.io/github/actions/workflow/status/ydb-platform/ydb-jdbc-driver/build.yaml)](https://github.com/ydb-platform/ydb-jdbc-driver/actions/workflows/build.yaml)
[![CI](https://img.shields.io/github/actions/workflow/status/ydb-platform/ydb-jdbc-driver/ci.yaml?label=CI)](https://github.com/ydb-platform/ydb-jdbc-driver/actions/workflows/ci.yaml)
[![Codecov](https://img.shields.io/codecov/c/github/ydb-platform/ydb-jdbc-driver)](https://app.codecov.io/gh/ydb-platform/ydb-jdbc-driver)

## JDBC Driver for YDB

### Quickstart

1) Drop in [JDBC driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases) to classpath or pick this file in IDE
2) Connect to YDB
   * Local or remote Docker (anonymous authentication):<br>`jdbc:ydb:grpc://localhost:2136/local`
   * Self-hosted cluster:<br>`jdbc:ydb:grpcs://<host>:2135/Root/testdb?secureConnectionCertificate=~/myca.cer`
   * Connect with token to the cloud instance:<br>`jdbc:ydb:grpcs://<host>:2135/path/to/database?tokenFile=~/my_token`
   * Connect with service account to the cloud instance:<br>`jdbc:ydb:grpcs://<host>:2135/path/to/database?saKeyFile=~/sa_key.json`
3) Execute queries, see example in [YdbDriverExampleTest.java](jdbc/src/test/java/tech/ydb/jdbc/YdbDriverExampleTest.java)

### Usage with Maven
The recommended way to use the YDB JDBC driver in your project is to consume it from Maven.
Specify the YDB JDBC driver in the dependencies:

```xml
<dependencies>
    <!-- Base version -->
    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver</artifactId>
        <version>2.3.16</version>
    </dependency>

    <!-- Shaded version with included dependencies -->
    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver-shaded</artifactId>
        <version>2.3.16</version>
    </dependency>
</dependencies>
```
### Authentication modes

YDB JDBC Driver supports the following [authentication modes](https://ydb.tech/en/docs/reference/ydb-sdk/auth):
* `Anonymous`: no authentication, used when username and password are not specified and no other authentication properties configured;
* `Static Credentials`: used when username and password are specified;
* `Access Token`: used when `token` property is configured, needs YDB authentication token as printed by the `ydb auth get-token` CLI command;
* `Metadata`: used when `useMetadata` property is set to `true`, extracts the authentication data from the metadata of a virtual machine, serverless container or a serverless function running in a cloud environment;
* `Service Account Key`: used when `saFile` property is configured, extracts the service account key and uses it for authentication.

### Driver properties reference

Driver supports the following configuration properties, which can be specified in the URL or passed via extra properties:
* `saKeyFile` - file location of service account key for authentication;
* `tokenFile` - file location with token value for token based authentication;
* `iamEndpoint` - custom IAM endpoint for authentication via service account key;
* `useMetadata` - boolean value, true if metadata authentication should be used, false otherwise (and default);
* `metadataURL` - custom metadata endpoint;
* `localDatacenter` - name of the datacenter local to the application being connected;
* `secureConnection` - boolean value, true if TLS should be enforced (normally configured via `grpc://` or `grpcs://` scheme in the JDBC URL);
* `secureConnectionCertificate` - custom CA certificate for TLS connections, can be passed either as literal value or as a file reference.

### Using TableService mode
By default JDBC driver executes all queries via QueryService, which uses grpc streams for the results recieving.
If your database instance doesn't support this service, you can use old TableService mode by passing property `useQueryService=false` to the JDBC URL.

### Building
By default all tests are run using a local YDB instance in Docker (if host has Docker or Docker Machine installed)
To disable these tests run `mvn test -DYDB_DISABLE_INTEGRATION_TESTS=true`

