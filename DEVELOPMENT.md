# Develop Streams Apache Avro toolkit

## Downloading the Streams Avro toolkit
Download the full toolkit requires git. Enter a directory of your choice and execute :

`cd yourDirectory`

`git clone https://github.com/IBMStreams/streamsx.avro.git`

## Build the toolkit using ant
These steps must be run on the Streams server (or the Streams Quick Start Edition) and require the Ant and Maven tools. Additionally, environment variable M2_HOME must be set to the Maven home directory.
* Set the Streams environment variables by sourcing the `streamsprofile.sh` script which can be found in the Streams installation `bin` directory
* Set environment variable `M2_HOME` to the Maven home directory; if you build the toolkit on a Streams QSE image, Maven is already installed and M2_HOME set
* Go to the toolkit's main directory that holds the `build.xml` file, for example: `cd /yourDirectory/streamsx.avro`
* Run `ant`
* To check out more targets of the build script run: `ant -p`

## Test the toolkit
To run the complete test suite, execute:

`ant test`

To run the quick test suite, execute:

`ant test-quick`

To clean all test artifacts, execute:

`ant test-clean`

For more information read the file [TEST](tests/frameworktests/README.md) .

