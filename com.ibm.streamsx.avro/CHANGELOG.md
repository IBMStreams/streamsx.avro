# Changes
==========

## latest/develop
* [#47](https://github.com/IBMStreams/streamsx.avro/issues/47) Makefiles of sample application prepared for CP4D Streams build service, supports build with [VS Code](https://marketplace.visualstudio.com/items?itemName=IBM.ibm-streams)


## v1.5.0

* [#46](https://github.com/IBMStreams/streamsx.avro/issues/46) Dependency to third-party libraries updated. Using org.apache.avro:1.10.0 to resolve vulnerability CVE-2020-8840 in jackson-databind-2.9.9.3.jar

## v1.4.2

* Globalization support: Messages updated

## v1.4.1

* Globalization support: Messages updated

## v1.4.0

* Globalization support: Error messages externalized

## v1.3.0:

* [#36](https://github.com/IBMStreams/streamsx.avro/issues/36) Resolved vulenaribility
* Update to Avro version 1.9.1

## v1.2.2:

* [#29](https://github.com/IBMStreams/streamsx.avro/issues/29) Resolved vulenaribility

## v1.2.1:

* new testframework allows better individual test case timeout control

## v1.2.0:

* toolkit build script and global build script with common targets available
* new structure of samples directory - each sample is now a separate project
* samples can now be build from command line (`make`) or from Streams Studio
* new Tuple to Avro sample
* detailed test suite available
* improved documentation is available
* new icons
* use of updated avro library (1.8.2)
* add Consisten Region compile time checks in all operators
* improved parameter checks during operator initialization
* removed parameter `ignoreParsingError` from `TupleToAvro` operator - tuples have a fixed structure
* removed parameter `avroSchemaEmbedded` from operator `AvroToJSON` - operator expects the embedded avro schema if no schema file is configured

## v1.1.0:

* toolkit version is 1.1.0
* initial release for toolkit, samples and test contributed by [fketelaars](https://github.com/fketelaars)





