# README --  FrameworkTests

This directory provides an automatic test for a number of operators of the inet toolkit.

## Test Execution

To start the full test execute:  
`./runTest.sh`

To start a quick test, execute:  
`./runTest.sh --category quick`

This script installs the test framework in directory `scripts` and starts the test execution. The script delivers the following result codes:  
0     : all tests Success  
20    : at least one test fails  
25    : at least one test error  
26    : Error during suite execution  
130   : SIGINT received  
other : another fatal error has occurred  

More options are available and explained with command:  
`./runTest.sh --help`

## Test Sequence

The `runTest.sh` installs the test framework into directory `scripts` and starts the test framework. The test framework 
checks if there is a running Streams instance and starts the ftp test server and the the http test server automatically. 

If the Streams instance is not running, a domain and an instance is created from the scratch and started. You can force the 
creation of instance and domain with command line option `--clean`

The path to the avro toolkit is defined with the variable `TTPR_streamsxAvroToolkit`
The path to the avro samples is defined with the variable `TTRO_streamsxAvroSamplesPath`
The standard properties file expects the avro toolkit is expected in directory `../../com.ibm.streamsx.avro/` and it must be built with the current Streams version. 
The avro toolkit samples are expected in `../../samples`. 

Use command line option `-D <name>=<value>` to set external variables or provide a new properties file with command line option 
`--properties <filename>`. The standard properties file is `tests/TestProperties.sh`.

## Requirements

The test framework requires an valid Streams installation and environment.

