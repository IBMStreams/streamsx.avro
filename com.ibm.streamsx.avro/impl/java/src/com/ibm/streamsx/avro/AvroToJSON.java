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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
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
	Schema schema;

	@Parameter(optional = true, description = "The input stream attribute which contains the output Avro blob. This attribute must be of type blob. Default is the sole output attribute when the schema has one attribute otherwise `avroBlob`.")
	public void setInputAvroAttribute(String inputAvroAttribute) {
		this.inputAvroAttribute = inputAvroAttribute;
	}

	@Parameter(optional = true, description = "The output stream attribute which contains the output JSON string. This attribute must be of `rstring` or `ustring` type. Default is the sole input attribute when the schema has one attribute otherwise `jsonString`.")
	public void setOutputJsonAttribute(String outputJsonAttribute) {
		this.outputJsonAttribute = outputJsonAttribute;
	}

	@Parameter(name = "avroSchemaFile", optional = false, description = "File that contains the Avro schema to deserialize the binary message.")
	public void setAvroSchemaFile(String avroSchemaFile) {
		this.avroSchemaFile = avroSchemaFile;
	}

	public String getAvroSchemaFile() {
		return avroSchemaFile;
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
		LOGGER.log(TraceLevel.TRACE, "Retrieving and parsing Avro schema file " + getAvroSchemaFile());
		InputStream avscInput = new FileInputStream(getAvroSchemaFile());
		Schema.Parser parser = new Schema.Parser();
		schema = parser.parse(avscInput);

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

		// Get the incoming binary Avro record(s)
		Blob avroMessage = tuple.getBlob(inputAvroAttribute);
		if (LOGGER.isTraceEnabled())
			LOGGER.log(TraceLevel.TRACE, "Processing Avro message with length " + avroMessage.getLength());

		// Convert the BLOB to GenericRecord object
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

	static final String DESC = "This operator binary Avro messages into a JSON string."
			+ " If an invalid Avro message is found in the input, the operator will fail.";

}
