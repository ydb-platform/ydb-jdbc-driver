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

