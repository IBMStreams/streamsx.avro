# Streams Apache Avro toolkit

This toolkit supports serialization and deserialization of messages in an [Apache Avro](https://avro.apache.org/) format. It provides two operators, `AvroToJSON` and `JSONToAvro`, which respectively are used to convert an Avro message (with or without embedded schema) to a JSON string and a JSON string into an Avro message (or a block of Avro messages with embedded schema). Avro messages are in a binary format; hence they are represented as a Streams `blob` type.

The toolkit contains examples on how to use the operators.

## Changes
[CHANGELOG.md](com.ibm.streamsx.avro/CHANGELOG.md)

## Use of the toolkit
Download a release package from [Releases](https://github.com/IBMStreams/streamsx.avro/releases) .
Untar or unzip the archive into a directory of your choice. 
Add this directory to the toolkit path of your Streams installation by either:
* expand the environment variable `STREAMS_SPLPATH` with your toolkit directory
* add the `-t` option to the Streams Compiler `sc` command
* edit the Toolkit Locations in the Streams Explorer of Streams Studio

## Develop the toolkit
See [Develop](DEVELOPMENT.md)
