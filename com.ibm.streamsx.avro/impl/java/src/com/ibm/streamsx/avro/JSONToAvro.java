//
// *******************************************************************************
// * Copyright (C)2016, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//

package com.ibm.streamsx.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
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
import com.ibm.streams.operator.types.ValueFactory;

/**
 * Processes Avro tuples and converts them to a JSON string
 * 
 */

@PrimitiveOperator(name = "JSONToAvro", namespace = "com.ibm.streamsx.avro", description = JSONToAvro.DESC)
@InputPorts({
		@InputPortSet(description = "Port that ingests JSON records", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({
		@OutputPortSet(description = "Port that produces Avro records", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Icons(location16 = "icons/JSONToAvro_16x16.png", location32 = "icons/JSONToAvro_32x32.png")
@Libraries(value = { "opt/downloaded/*" })
public class JSONToAvro extends AbstractOperator {

	private static Logger LOGGER = Logger.getLogger(JSONToAvro.class);

	private String inputJsonAttribute = null;
	private final String DEFAULT_INPUT_JSON_ATTRIBUTE = "jsonString";
	private String outputAvroAttribute = null;
	private final String DEFAULT_OUTPUT_AVRO_ATTRIBUTE = "avroBlob";

	private String avroSchemaFile = null;
	private Schema schema;

	@Parameter(optional = true, description = "The input stream attribute which contains the input JSON string. This attribute must be of `rstring` or `ustring` type. Default is the sole input attribute when the schema has one attribute otherwise `jsonString`.")
	public void setInputJsonAttribute(String inputJsonAttribute) {
		this.inputJsonAttribute = inputJsonAttribute;
	}

	@Parameter(optional = true, description = "The ouput stream attribute which contains the output Avro blob. This attribute must be of type blob. Default is the sole output attribute when the schema has one attribute otherwise `avroBlob`.")
	public void setOutputAvroAttribute(String outputAvroAttribute) {
		this.outputAvroAttribute = outputAvroAttribute;
	}

	@Parameter(optional = false, description = "File that contains the Avro schema to serialize the binary message.")
	public void setAvroSchemaFile(String avroSchemaFile) {
		this.avroSchemaFile = avroSchemaFile;
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

		// If no input JSON attribute specified, use default
		if (inputJsonAttribute == null) {
			if (ssIp0.getAttributeCount() == 1) {
				inputJsonAttribute = ssIp0.getAttribute(0).getName();
			} else {
				inputJsonAttribute = DEFAULT_INPUT_JSON_ATTRIBUTE;
			}
		}
		LOGGER.log(TraceLevel.TRACE, "Input JSON attribute: " + inputJsonAttribute);

		// If no output Avro blob attribute specified, use default
		if (outputAvroAttribute == null) {
			if (ssOp0.getAttributeCount() == 1) {
				outputAvroAttribute = ssOp0.getAttribute(0).getName();
			} else {
				outputAvroAttribute = DEFAULT_OUTPUT_AVRO_ATTRIBUTE;
			}
		}
		LOGGER.log(TraceLevel.TRACE, "Output Avro attribute: " + outputAvroAttribute);

		// Get the Avro schema file to parse the Avro messages
		LOGGER.log(TraceLevel.TRACE, "Retrieving and parsing Avro schema file " + avroSchemaFile);
		InputStream avscInput = new FileInputStream(avroSchemaFile);
		Schema.Parser parser = new Schema.Parser();
		schema = parser.parse(avscInput);

		LOGGER.log(TraceLevel.TRACE, "JSONToAvro operator initialized, ready to receive tuples");

	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
	 * 
	 */
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {

		String jsonInput = tuple.getString(inputJsonAttribute);

		if (LOGGER.isTraceEnabled())
			LOGGER.log(TraceLevel.TRACE, "Input JSON string: " + jsonInput);

		// Create a new tuple for output port 0 and copy over any matching
		// attributes
		StreamingOutput<OutputTuple> outStream = getOutput(0);
		OutputTuple outTuple = outStream.newTuple();
		outTuple.assign(tuple);

		// Convert the JSON string to a GenericRecord object
		final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		ByteArrayInputStream jsonByteArray = new ByteArrayInputStream(jsonInput.getBytes(StandardCharsets.UTF_8));
		ByteArrayOutputStream avroByteArray = new ByteArrayOutputStream();
		DataInputStream jsonDis = new DataInputStream(jsonByteArray);
		GenericDatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<GenericRecord>(schema);
		Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonDis);
		Encoder encoder = EncoderFactory.get().binaryEncoder(avroByteArray, null);
		GenericRecord datum = datumReader.read(null, decoder);
		avroWriter.write(datum, encoder);
		encoder.flush();

		if (LOGGER.isTraceEnabled())
			LOGGER.log(TraceLevel.TRACE, "Length of generated Avro message: " + avroByteArray.size());

		// Submit new tuple to output port 0
		outTuple.setBlob(outputAvroAttribute, ValueFactory.newBlob(avroByteArray.toByteArray()));
		outStream.submit(outTuple);
	}

	static final String DESC = "This operator binary Avro messages into a JSON string."
			+ " If an invalid Avro message is found in the input, the operator will fail.";

}
