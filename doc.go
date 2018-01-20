/**
* package btcexport provides a service for parsing the blockchain as stored by a
* btcd full node and exporting it as CSV in chunked files. The BlockExporter
* struct runs a concurrent process to perform the export and BlockEncoder
* defines the transformation from deserialized btcd structures to CSV records.
* The BlockExporter supports multiple file output backends, currently including
* filesystem output and uploads to Amazon S3 buckets.
 */
package btcexport
