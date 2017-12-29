# Installing the Streams Avro toolkit
To use the Streams Avro toolkit, you must first build it using the ant tool. During the build process, Apache Maven is used to download the Apache Avro components that are needed to convert from and to Avro messages. 

## Downloading the Streams Avro toolkit
* Download the full toolkit as a zip file: `https://github.com/IBMStreams/streamsx.avro/archive/master.zip` into a temporary directory, for example `/tmp`
* Unzip the `master.zip` file

## Build the toolkit using ant
These steps must be run on the Streams server (or the Streams Quick Start Edition) and require the Ant and Maven tools. Additionally, environment variable M2_HOME must be set to the Maven home directory.
* Set the Streams environment variables by sourcing the `streamsprofile.sh` script which can be found in the Streams installation `bin` directory
* Set environment variable `M2_HOMEE` to the Maven home directory; if you build the toolkit on a Streams QSE image, Maven is already installed and M2_HOME set
* Go to the toolkit's main directory that holds the `build.xml` file, for example: `cd /tmp/streamsx.avro`
* Run `ant`

## Implementing the Avro toolkit on the Streams server
To register the toolkit in IBM Streams, it must be moved to a directory in the toolkit path.
* Move the `com.ibm.streamsx.avro` directory to `$STREAMS_INSTALL/toolkits`; this will make it visible to IBM Streams and Streams Studio. Alternatively you can move it to a different directory and include it in the Streams toolkit path (STREAMS_SPLPATH environment variable).
* Move the `com.ibm.streamsx.avro.sample` directory to the `$STREAMS_INSTALL/samples directory to make the Avro samples visible when importing a sample Streams application