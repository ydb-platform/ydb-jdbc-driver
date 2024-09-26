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

