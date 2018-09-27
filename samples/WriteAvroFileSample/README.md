# WriteAvroFileSample

## Description

This sample demonstrates how you can use the JSONToAvro operator from the com.ibm.streamsx.avro toolkit.
In this sample a file is written in avro format.

## Use

Build application:

`make`

Run standalone:

`./output/bin/standalone`

You can examine the output file `twitter_{localtime:%Y%m%d_%H%M%S}_{id}.avro` in the `/tmp` directory.


Clean:

`make clean`


## Utilized Toolkits

 - com.ibm.streamsx.json
 - com.ibm.streamsx.avro
