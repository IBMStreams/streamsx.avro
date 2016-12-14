# Streams Apache Avro toolkit (incubating)
This toolkit to support serialization and deserialization of messages in an [Apache Avro](https://avro.apache.org/) format. It provides two operators, `AvroToJSON` and `JSONToAvro`, which respectively are used to convert an Avro message into a JSON string and a JSON string into an Avro message. Avro messages are in a binary format, hence they are represented as a Streams `blob` type.

The toolkit contains examples on how to use the operators.
