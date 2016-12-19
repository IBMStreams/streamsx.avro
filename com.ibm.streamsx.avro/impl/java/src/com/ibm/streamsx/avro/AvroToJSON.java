//
// *******************************************************************************
// * Copyright (C)2016, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//

package com.ibm.streamsx.avro;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.log4j.TraceLevel;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.Blob;

/**
 * Processes Avro tuples and converts them to a JSON string
 * 
 */

@PrimitiveOperator(name = "AvroToJSON", namespace = "com.ibm.streamsx.avro", description = AvroToJSON.DESC)
@InputPorts({
		@InputPortSet(description = "Port that ingests tuples", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Icons(location16 = "icons/AvroToJSON_16x16.png", location32 = "icons/AvroToJSON_32x32.png")
@Libraries(value = { "opt/downloaded/*" })
public class AvroToJSON extends AbstractOperator {

	private static Logger LOGGER = Logger.getLogger(AvroToJSON.class);

	private String inputAvroAttribute = null;
	private final String DEFAULT_INPUT_AVRO_ATTRIBUTE = "avroBlob";
	private String outputJsonAttribute = null;
	private final String DEFAULT_OUTPUT_JSON_ATTRIBUTE = "jsonString";

	protected String avroSchemaFile = "";
	protected boolean avroSchemaEmbedded = false;
	Schema schema;

	@Parameter(optional = true, description = "The input stream attribute which contains the input Avro blob. This attribute must be of type blob. Default is the sole output attribute when the schema has one attribute otherwise `avroBlob`.")
	public void setInputAvroAttribute(String inputAvroAttribute) {
		this.inputAvroAttribute = inputAvroAttribute;
	}

	@Parameter(optional = true, description = "The output stream attribute which contains the output JSON string. This attribute must be of `rstring` or `ustring` type. Default is the sole input attribute when the schema has one attribute otherwise `jsonString`.")
	public void setOutputJsonAttribute(String outputJsonAttribute) {
		this.outputJsonAttribute = outputJsonAttribute;
	}

	@Parameter(optional = true, description = "File that contains the Avro schema to deserialize the binary message.")
	public void setAvroSchemaFile(String avroSchemaFile) {
		this.avroSchemaFile = avroSchemaFile;
	}

	@Parameter(optional = true, description = "Is the Avro schema embedded in the input Avro blob?")
	public void setAvroSchemaEmbedded(boolean avroSchemaEmbedded) {
		this.avroSchemaEmbedded = avroSchemaEmbedded;
	}

	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * 
	 * @param operatorContext
	 *            OperatorContext for this operator.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext operatorContext) throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(operatorContext);
		LOGGER.log(TraceLevel.TRACE, "Operator " + operatorContext.getName() + " initializing in PE: "
				+ operatorContext.getPE().getPEId() + " in Job: " + operatorContext.getPE().getJobId());

		StreamSchema ssOp0 = getOutput(0).getStreamSchema();
		StreamSchema ssIp0 = getInput(0).getStreamSchema();

		// If no input Avro blob attribute specified, use default
		if (inputAvroAttribute == null) {
			if (ssIp0.getAttributeCount() == 1) {
				inputAvroAttribute = ssIp0.getAttribute(0).getName();
			} else {
				inputAvroAttribute = DEFAULT_INPUT_AVRO_ATTRIBUTE;
			}
		}
		LOGGER.log(TraceLevel.TRACE, "Input Avro attribute: " + inputAvroAttribute);

		// If no output JSON attribute specified, use default
		if (outputJsonAttribute == null) {
			if (ssOp0.getAttributeCount() == 1) {
				outputJsonAttribute = ssOp0.getAttribute(0).getName();
			} else {
				outputJsonAttribute = DEFAULT_OUTPUT_JSON_ATTRIBUTE;
			}
		}
		LOGGER.log(TraceLevel.TRACE, "Output JSON attribute: " + outputJsonAttribute);

		// Get the Avro schema file to parse the Avro messages
		if (!avroSchemaFile.isEmpty()) {
			LOGGER.log(TraceLevel.TRACE, "Retrieving and parsing Avro schema file " + avroSchemaFile);
			InputStream avscInput = new FileInputStream(avroSchemaFile);
			Schema.Parser parser = new Schema.Parser();
			schema = parser.parse(avscInput);
		}

		// If the schema is embedded, the schema file must not be specified
		if (avroSchemaEmbedded && !avroSchemaFile.isEmpty())
			throw new Exception("Parameter avroSchema cannot be specified if the schema is embedded in the message.");

		LOGGER.log(TraceLevel.TRACE, "AvroToJSON operator initialized, ready to receive tuples");

	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
	 * 
	 */
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {

		// Create a new tuple for output port 0
		StreamingOutput<OutputTuple> outStream = getOutput(0);
		OutputTuple outTuple = outStream.newTuple();
		outTuple.assign(tuple);

		// Get the incoming binary Avro record(s)
		Blob avroMessage = tuple.getBlob(inputAvroAttribute);
		if (LOGGER.isTraceEnabled())
			LOGGER.log(TraceLevel.TRACE, "Processing Avro message with length " + avroMessage.getLength());

		// Submit JSON tuples based on the Avro content received in the Blob
		if (!avroSchemaEmbedded) {
			processAvroMessage(avroMessage, outStream, outTuple, schema);
		} else {
			processAvroMessage(avroMessage, outStream, outTuple);
		}
	}

	/**
	 * Processes an Avro Blob containing a single message and with no embedded
	 * schema. This is the pattern when Avro objects are passed over messaging
	 * infrastructure such as Apache Kafka.
	 * 
	 * @param avroMessage
	 *            The Blob that holds the single Avro object
	 * @param outStream
	 *            The stream to which the JSON string must be submitted
	 * @param outTuple
	 *            The tuple holding the JSON string
	 * @param schema
	 *            The schema of the Avro object
	 * @throws Exception
	 */
	private void processAvroMessage(Blob avroMessage, StreamingOutput<OutputTuple> outStream, OutputTuple outTuple,
			Schema schema) throws Exception {
		GenericDatumReader<GenericRecord> consumer = new GenericDatumReader<GenericRecord>(schema);
		ByteArrayInputStream consumedByteArray = new ByteArrayInputStream(avroMessage.getData());
		Decoder consumedDecoder = DecoderFactory.get().binaryDecoder(consumedByteArray, null);
		GenericRecord consumedDatum = consumer.read(null, consumedDecoder);
		if (LOGGER.isTraceEnabled())
			LOGGER.log(TraceLevel.TRACE, "JSON representation of Avro message: " + consumedDatum.toString());
		// Submit new tuple to output port 0
		outTuple.setString(outputJsonAttribute, consumedDatum.toString());
		outStream.submit(outTuple);
	}

	/**
	 * Processes a blob which contains one or more Avro objects and has the
	 * schema embedded. This is the pattern when Avro objects are read from a
	 * file (either local file system or HDFS). Every Avro object in the blob is
	 * converted to JSON and then submitted to the output port.
	 * 
	 * @param avroMessage
	 *            The Blob that holds one or more Avro objects and the schema
	 * @param outStream
	 *            The stream to which the JSON string must be submitted
	 * @param outTuple
	 *            The tuple holding the JSON string
	 * @throws Exception
	 */
	private void processAvroMessage(Blob avroMessage, StreamingOutput<OutputTuple> outStream, OutputTuple outTuple)
			throws Exception {
		ByteArrayInputStream is = new ByteArrayInputStream(avroMessage.getData());
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(is, reader);
		GenericRecord consumedDatum = null;
		while (dataFileReader.hasNext()) {
			consumedDatum = dataFileReader.next(consumedDatum);
			// Submit new tuple to output port 0
			outTuple.setString(outputJsonAttribute, consumedDatum.toString());
			outStream.submit(outTuple);
		}
		is.close();
		dataFileReader.close();
	}

	static final String DESC = "This operator binary Avro messages into a JSON string."
			+ " If an invalid Avro message is found in the input, the operator will fail.";

}
