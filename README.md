# Streams Apache Avro toolkit (incubating)
This toolkit to support serialization and deserialization of messages in an [Apache Avro](https://avro.apache.org/) format. It provides two operators, `AvroToJSON` and `JSONToAvro`, which respectively are used to convert an Avro message (with or without embedded schema) to a JSON string and a JSON string into an Avro message (or a block of Avro messages with embedded schema). Avro messages are in a binary format, hence they are represented as a Streams `blob` type.

The toolkit contains examples on how to use the operators.

## Installation of the toolkit
Installation instructions can be found here: [Installing the toolkit](documentation/Installation.md)

## Getting started
Once installation is done, here you can find how to use the toolkit: [Using the CDCStreams toolkit](documentation/Usage.md)

## Troubleshooting
If the Streams application fails, please check: [Troubleshooting](documentation/Troubleshooting.md)
