# Streams Apache Avro toolkit

This toolkit supports serialization and deserialization of messages in an [Apache Avro](https://avro.apache.org/) format. It provides two operators, `AvroToJSON` and `JSONToAvro`, which respectively are used to convert an Avro message (with or without embedded schema) to a JSON string and a JSON string into an Avro message (or a block of Avro messages with embedded schema). Avro messages are in a binary format, hence they are represented as a Streams `blob` type.

The toolkit contains examples on how to use the operators.

## Installation of the toolkit
Installation instructions can be found here: [Installing the toolkit](documentation/Installation.md)

