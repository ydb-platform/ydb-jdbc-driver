## 2.3.10 ##

* Added .metadata folder to system tables
* Added support of GRANT/REVOKE keywords

## 2.3.9 ##

* Added option 'forceJdbcParameters' to detect ? in whole query text
* Added option 'tokenProvider' to use custom auth provider
* Upgrade to Java SDK 2.3.13

## 2.3.8 ##

* Added option 'grpcCompression'
* Added bulk upsert validation
* All grpc calls isolated from parent context

## 2.3.7 ##

* Added slf4j-to-jul to shaded jar
* Upgrade to use Java SDK v2.3.8

## 2.3.6 ##

* Added jdbc_table which can be converted to AS_TABLE($list)

## 2.3.5 ##

* Disabled stream reader sets for SCAN queries

## 2.3.4 ##

* Upgraded to YDB Java SDK 2.3.6
* Fixed closing of ResultSet on prepareStatement and createStatement
* Added support of convertion IN (?, ?, ?) to IN $list
* Added option cachedTransportsCount to shard JDBC connections

## 2.3.3 ##

* Upgraded to YDB Java SDK 2.3.5
* Added support of setString for all YDB types
* Fixed disablePrepareDataQuery option
* Added support of UUID and custom decimal types

## 2.3.2 ##

* Fixed typo in YdbTracer

## 2.3.1 ##

* Updated transaction tracer implementation
* Fixed table description cache

## 2.3.0 ##

* QueryService is enabled by default

## 2.2.14 ##

* Fixed saKeyFile option

## 2.2.13 ##

* Fixed usePrefixPath for preparing of YQL statements

## 2.2.12 ##

* Added option usePrefixPath
* Added options saKeyFile and tokenFile
* Upgraded to YDB Java SDK 2.3.3

## 2.2.11 ##

* Added transaction tracer
* Added option useStreamResultSets to switch on streamable result sets
* Updated options to control query special modes

## 2.2.10 ##

* Added auto batch mode for simple UPDATE/DELETE/REPLACE

## 2.2.9 ##

* Removed cleaning of unclosed result set
* Added checks of invalid values of Decimal

## 2.2.8 ##

* Added support of stream cancelling

## 2.2.7 ##

* Fixed concurrency issues in QueryServiceExecutor
* Fixed QueryClient pool autoresize

## 2.2.6 ##

* Added logs for QueryClient executor

## 2.2.5 ##

* Added lazy result set implementation
* Added flag to forced usage of BulkUpsert and ScanQuery
* Fixed error of LIMIT & OFFSET keywords parsing

## 2.2.4 ##

* Added support of BulkUpsert
* Upgraded to YDB Java SDK v2.3.0

## 2.2.3 ##

* Added custom sql types for usage of native YDB types

## 2.2.2 ##

* Added optional query stats collector

## 2.2.1 ##

* Fixed usage of datetime types

## 2.2.0 ##

* Added support of queries with RETURNING
* Added auto batch mode for simple UPSERT/INSERT
* Forced usage UInt64 for OFFSET/LIMIT parameters

## 2.1.6 ##

* Fixed QueryClient leaks
* Added support of query explaining for useQueryService mode

## 2.1.5 ##

* Fixed issue processing for scheme queries

## 2.1.4 ##

* Added support of null and void types
* Upgraded to YDB Java SDK 2.2.6
* Added reading of query service warning issues
* Updated relocations for shaded JAR

## 2.1.3 ##

* Added relocations for shaded JAR

## 2.1.2 ##

* Fixed SQL query parser

## 2.1.1 ##

* Fixed a race on the YDB context creating
* Fixed leak of YDB context on closing
* Upgraded to YDB Java SDK 2.2.2

## 2.1.0 ##

* Added support for QueryService
* Added configs for custom iam enpoint and metadata URL
* Upgraded to YDB Java SDK 2.2.0

## 2.0.7 ##

* Added getter for GrpcTransport to YdbContext

## 2.0.6 ##

* Upgraded Java SDK version
* Fixed Int6/Int16 auto detecting
* Fixed standard transaction levels

## 2.0.5 ##

* Extended usage of standard JDBC exception classes
* Fixed too small default timeout

## 2.0.4 ##

* Fixed problem with prepareDataQuery for YQL-specific queries
* Added prepared queries cache
* Updated implementation of getUpdateCount() and getMoreResults()
* Added usage of standard JDBC exception classes SQLRecoverableException and SQLTransientException

## 2.0.3 ##

* Added SqlState info to YDB exceptions
* Fixed getUpdateCount() after getMoreResults()
* Fixed columns info for empty result sets from DatabaseMetaData

## 2.0.2 ##

* Removed obsolete options

## 2.0.1 ##

* Added column tables to getTables() method
* Added parameter `forceQueryMode` to use for specifying the type of query
* Execution of scan or scheme query inside active transaction will throw exception

## 2.0.0 ##

* Auto detect of YDB query type
* Support of batch queries
* Support of standart positional parameters
* Added the ability to set the YQL variable by alphabetic order

