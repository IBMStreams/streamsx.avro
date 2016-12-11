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
import com.ibm.streams.operator.StreamingData.Punctuation;
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

	protected OperatorContext operatorContext;

	protected String avroSchemaFile = "";
	Schema schema;

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
		this.operatorContext = operatorContext;
		LOGGER.log(TraceLevel.TRACE, "Operator " + operatorContext.getName() + " initializing in PE: "
				+ operatorContext.getPE().getPEId() + " in Job: " + operatorContext.getPE().getJobId());

		// Get the Avro schema file to parse the Avro messages
		LOGGER.log(TraceLevel.TRACE, "Retrieving and parsing Avro schema file " + getAvroSchemaFile());
		InputStream avscInput = new FileInputStream(getAvroSchemaFile());
		Schema.Parser parser = new Schema.Parser();
		schema = parser.parse(avscInput);

		LOGGER.log(TraceLevel.TRACE, "AvroToJSON operator initialized, ready to receive tuples");

	}

	/**
	 * Notification that initialization is complete and all input and output
	 * ports are connected and ready to receive and submit tuples.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		// This method is commonly used by source operators.
		// Operators that process incoming tuples generally do not need this
		// notification.
		OperatorContext context = getOperatorContext();
		LOGGER.log(TraceLevel.TRACE, "Operator " + context.getName() + " all ports are ready in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
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
		Blob avroMessage = tuple.getBlob(0);
		LOGGER.log(TraceLevel.TRACE, "Processing Avro message with length " + avroMessage.getLength());

		// Convert the BLOB to GenericRecord object
		GenericDatumReader<GenericRecord> consumer = new GenericDatumReader<GenericRecord>(schema);
		ByteArrayInputStream consumedByteArray = new ByteArrayInputStream(avroMessage.getData());
		Decoder consumedDecoder = DecoderFactory.get().binaryDecoder(consumedByteArray, null);
		GenericRecord consumedDatum = consumer.read(null, consumedDecoder);
		LOGGER.log(TraceLevel.TRACE, "JSON representation of Avro message: " + consumedDatum.toString());

		// Submit new tuple to output port 0
		outTuple.setString(0, consumedDatum.toString());
		outStream.submit(outTuple);
	}

	/**
	 * Process an incoming punctuation that arrived on the specified port.
	 * 
	 * @param stream
	 *            Port the punctuation is arriving on.
	 * @param mark
	 *            The punctuation mark
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
		// For window markers, punctuate all output ports
		super.processPunctuation(stream, mark);
	}

	/**
	 * Shutdown this operator.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	public synchronized void shutdown() throws Exception {
		OperatorContext context = getOperatorContext();
		LOGGER.log(TraceLevel.TRACE, "Operator " + context.getName() + " shutting down in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
		// Must call super.shutdown()
		super.shutdown();
	}

	static final String DESC = "This operator binary Avro messages into a JSON string."
			+ " If an invalid Avro message is found in the input, the operator will fail.";

}
