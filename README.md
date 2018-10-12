# Streams Apache Avro toolkit

This toolkit supports serialization and deserialization of messages in an [Apache Avro](https://avro.apache.org/) format. It provides two operators, `AvroToJSON` and `JSONToAvro`, which respectively are used to convert an Avro message (with or without embedded schema) to a JSON string and a JSON string into an Avro message (or a block of Avro messages with embedded schema). Avro messages are in a binary format; hence they are represented as a Streams `blob` type.

The toolkit contains examples on how to use the operators.

## Use of the toolkit
Download a release package from [Releases](https://github.com/IBMStreams/streamsx.avro/releases) .
Untar or unzip the archive into a directory of your choice. 
Add this directory to the toolkit path of your Streams installation by either:
* expand the environment variable `STREAMS_SPLPATH` with your toolkit directory
* add the `-t` option to the Streams Compiler `sc` command
* edit the Toolkit Locations in the Streams Explorer of Streams Studio

## Develop the toolkit
See [Develop](DEVELOPMENT.md)

## What's new

### Version 1.1.0:
* toolkit version is 1.1.0
* initial release for toolkit, samples and test contributed by [fketelaars](https://github.com/fketelaars)

### Version 1.2.0:
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

