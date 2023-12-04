## 2.0.4 ##

* Fixed problem with prepareDataQuery for YQL-specific queries
* Added prepared queries cache
* Updated implementation of getUpdateCount() and getMoreResults()
* Added usage of standart JDBC exception classes SQLRecoverableException and SQLTransientException

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

