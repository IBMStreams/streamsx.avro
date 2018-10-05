//
// *******************************************************************************
// * Copyright (C)2018, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//

package com.ibm.streamsx.avro;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
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
import com.ibm.streams.operator.StreamingData.Punctuation;
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
import com.ibm.streamsx.avro.convert.TupleToAvroConverter;

/**
 * Processes tuples and converts them to Avro
 * 
 */

@PrimitiveOperator(name = "TupleToAvro", namespace = "com.ibm.streamsx.avro", description = TupleToAvro.DESC)
@InputPorts({
		@InputPortSet(description = "Port that ingests tuples", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({
		@OutputPortSet(description = "Port that produces Avro records", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Icons(location16 = "icons/TupleToAvro_16.gif", location32 = "icons/TupleToAvro_32.gif")
@Libraries(value = { "opt/downloaded/*" })
public class TupleToAvro extends AbstractOperator {

	private static Logger LOGGER = Logger.getLogger(TupleToAvro.class);

	private String outputAvroMessage = null;
	private final String DEFAULT_OUTPUT_AVRO_MSG_ATTRIBUTE = "avroMessage";

	private String avroMessageSchemaFile = null;
	private boolean embedAvroSchema = false;
	private boolean submitOnPunct = false;
	private long bytesPerMessage = 0;
	private long tuplesPerMessage = 0;
	private long timePerMessage = 0;
	private boolean ignoreParsingError = false;
	private Schema messageSchema;

	@Parameter(optional = true, description = "The ouput stream attribute which contains the output Avro message(s). This attribute must be of type blob. Default is the sole output attribute when the schema has one attribute otherwise `avroMessage`.")
	public void setOutputAvroMessage(String outputAvroMessage) {
		this.outputAvroMessage = outputAvroMessage;
	}

	@Parameter(optional = false, description = "File that contains the Avro schema to serialize the Avro message(s).")
	public void setAvroMessageSchemaFile(String avroMessageSchemaFile) {
		this.avroMessageSchemaFile = avroMessageSchemaFile;
	}

	@Parameter(optional = true, description = "Embed the schema in the generated Avro message. "
			+ "When generating Avro messages that must be persisted to a file system, "
			+ "the schema is expected to be included in the file. If this parameter is set to true, "
			+ "incoming tuples are batched and a large binary object that contains the Avro schema "
			+ "and 1 or more messages is generated. Also, you must specify one of the parameters (submitOnPunct, "
			+ "bytesPerMessage, tuplesPerMessage, timePerMessage) that controls "
			+ "when Avro message block is submitted to the output port."
			+ "After submitting the Avro message to the output port, a "
			+ "punctuation is generated so that the receiving operator can potentially create a new file.")
	public void setEmbedAvroSchema(Boolean embedAvroSchema) {
		this.embedAvroSchema = embedAvroSchema;
	}

	@Parameter(optional = true, description = "Only valid if Avro schema is embedded in the output. When set to true, this "
			+ "the operator will submit the block of Avro messages what was built and generate a punctuation so that the "
			+ "receiving operator can potentially create a new file.")
	public void setSubmitOnPunct(Boolean submitOnPunct) {
		this.submitOnPunct = submitOnPunct;
	}

	@Parameter(optional = true, description = "Only valid if Avro schema is embedded in the output. This parameter controls "
			+ "the minimum size in bytes that the Avro message block should be before it is submitted to the output"
			+ "port.")
	public void setBytesPerMessage(Long bytesPerMessage) {
		this.bytesPerMessage = bytesPerMessage;
	}

	@Parameter(optional = true, description = "Only valid if Avro schema is embedded in the output. This parameter controls "
			+ "the minimum number of tuples that the Avro message block should contain before it is submitted to the output"
			+ "port.")
	public void setTuplesPerMessage(Long tuplesPerMessage) {
		this.tuplesPerMessage = tuplesPerMessage;
	}

	@Parameter(optional = true, description = "Only valid if Avro schema is embedded in the output. This parameter controls "
			+ "the maximum time in seconds before the Avro message block is submitted to the output port.")
	public void setTimePerMessage(Long timePerMessage) {
		this.timePerMessage = timePerMessage;
	}

	@Parameter(optional = true, description = "Ignore any tuple or Avro parsing errors. When set to true, errors that "
			+ "occur when parsing the incoming tuple or constructing the Avro tuple(s) will be ignored and the incoming tuple(s) "
			+ "will be skipped.")
	public void setIgnoreParsingError(Boolean ignoreParsingError) {
		this.ignoreParsingError = ignoreParsingError;
	}

	// Variables
	StreamingOutput<OutputTuple> outStream;
	OutputTuple outTuple;
	GenericDatumWriter<GenericRecord> avroWriter;
	DataFileWriter<GenericRecord> avroDataFileWriter;
	ByteArrayOutputStream avroMessageByteArray = new ByteArrayOutputStream();
	ByteArrayOutputStream avroBlockByteArray = new ByteArrayOutputStream();
	long lastSubmitted = System.currentTimeMillis();
	int numberOfBatchedMessages = 0;

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

		// If no output Avro message attribute specified, use default
		if (outputAvroMessage == null) {
			if (ssOp0.getAttributeCount() == 1) {
				outputAvroMessage = ssOp0.getAttribute(0).getName();
			} else {
				outputAvroMessage = DEFAULT_OUTPUT_AVRO_MSG_ATTRIBUTE;
			}
		}
		LOGGER.log(TraceLevel.TRACE, "Output Avro message attribute: " + outputAvroMessage);

		// Get the Avro schema file to parse the Avro messages
		LOGGER.log(TraceLevel.TRACE, "Retrieving and parsing Avro message schema file " + avroMessageSchemaFile);
		InputStream avscInput = new FileInputStream(avroMessageSchemaFile);
		Schema.Parser parser = new Schema.Parser();
		messageSchema = parser.parse(avscInput);

		// Check Streams and Avro schema
		boolean validMapping = TupleToAvroConverter.isValidTupleToAvroMapping(operatorContext.getName(), ssIp0,
				messageSchema);
		if (!validMapping) {
			throw new Exception("Streams input tuple schema cannot be mapped to Avro output schema.");
		}

		LOGGER.log(TraceLevel.TRACE, "Embed Avro schema in generated output Avro message block: " + embedAvroSchema);
		LOGGER.log(TraceLevel.TRACE, "Submit Avro message block when punctuation is received: " + submitOnPunct);
		LOGGER.log(TraceLevel.TRACE, "Ignore parsing error: " + ignoreParsingError);

		// submitOnPunct is only valid if Avro schema is embedded in the output
		if (submitOnPunct && !embedAvroSchema)
			throw new Exception(
					"Parameter submitOnPunct can only be set to true if Avro schema is embedded in the output.");
		// If Avro schema is embedded in the output, submitOnPunct is mandatory
		if (embedAvroSchema && !submitOnPunct && tuplesPerMessage == 0 && bytesPerMessage == 0 && timePerMessage == 0)
			throw new Exception("If Avro schema is embedded in the output, you must specify one of the thresholds when "
					+ "the tuple must be submitted (submitOnPunct, bytesPerMessage, timePerMessage, tuplesPerMessage).");

		// Prepare and initialize variables that don't change for every input
		// record
		avroWriter = new GenericDatumWriter<GenericRecord>(messageSchema);
		avroDataFileWriter = new DataFileWriter<GenericRecord>(avroWriter);
		if (embedAvroSchema)
			avroDataFileWriter.create(messageSchema, avroBlockByteArray);
		numberOfBatchedMessages = 0;

		LOGGER.log(TraceLevel.TRACE, "TupleToAvro operator initialized, ready to receive tuples");

	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
	 */
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {

		if (LOGGER.isTraceEnabled())
			LOGGER.log(TraceLevel.TRACE, "Input tuple: " + tuple);

		// Create a new tuple for output port 0 and copy over any matching
		// attributes
		outStream = getOutput(0);
		outTuple = outStream.newTuple();
		outTuple.assign(tuple);

		// Parse the tuple input and assign to Avro datum
		GenericRecord datum = TupleToAvroConverter.convertTupleToAvro(tuple, inputStream.getStreamSchema(),
				messageSchema);

		try {
			// Encode the datum to Avro
			if (embedAvroSchema) {
				avroDataFileWriter.append(datum);
				avroDataFileWriter.flush();
				numberOfBatchedMessages++;
				// Check if any of the threshold parameters has been exceeded
				if (tuplesPerMessage != 0 && numberOfBatchedMessages >= tuplesPerMessage)
					submitAvroToOuput();
				if (bytesPerMessage != 0 && avroBlockByteArray.size() >= bytesPerMessage)
					submitAvroToOuput();
				if (timePerMessage != 0) {
					if (System.currentTimeMillis() >= (lastSubmitted + (1000 * timePerMessage)))
						submitAvroToOuput();
				}
			} else {
				Encoder encoder = EncoderFactory.get().binaryEncoder(avroMessageByteArray, null);
				avroWriter.write(datum, encoder);
				encoder.flush();
				submitAvroToOuput();
			}
		} catch (Exception e) {
			LOGGER.log(TraceLevel.ERROR,
					"Error while converting tuple to AVRO schema: " + e.getMessage() + ". Tuple: " + inputStream);
			// If parsing errors must not be ignored, make the operator fail
			if (!ignoreParsingError)
				throw new Exception(
						"Error while converting tuple to AVRO schema: " + e.getMessage() + ". Tuple: " + inputStream);
		}
	}

	// Submit the Avro byte array to the output port and reset byte array
	private void submitAvroToOuput() throws Exception {
		// Send block of messages with Avro schema included and punctuation
		if (embedAvroSchema) {
			if (numberOfBatchedMessages > 0) {
				if (LOGGER.isTraceEnabled())
					LOGGER.log(TraceLevel.TRACE, "Submitting " + numberOfBatchedMessages
							+ " Avro messages with a total length of " + avroBlockByteArray.size() + " bytes");
				outTuple.setBlob(outputAvroMessage, ValueFactory.newBlob(avroBlockByteArray.toByteArray()));
				outStream.submit(outTuple);
				outStream.punctuate(Punctuation.WINDOW_MARKER);
				// Reset for the next block
				avroBlockByteArray.reset();
				avroDataFileWriter.close();
				avroDataFileWriter.create(messageSchema, avroBlockByteArray);
				lastSubmitted = System.currentTimeMillis();
				numberOfBatchedMessages = 0;
			}
		} else { // Send individual message
			if (LOGGER.isTraceEnabled())
				LOGGER.log(TraceLevel.TRACE,
						"Submitting Avro message with length " + avroMessageByteArray.size() + " bytes");
			outTuple.setBlob(outputAvroMessage, ValueFactory.newBlob(avroMessageByteArray.toByteArray()));
			outStream.submit(outTuple);
			// Reset for the next message
			avroMessageByteArray.reset();
		}
	}

	/**
	 * Process the punctuation. If Avro messages are batched, the Avro message
	 * is submitted if a window punctuation is received and submitOnPunct is
	 * true, or when the final punctuation is received.
	 */
	public void processPunctuation(StreamingInput<Tuple> inputStream, Punctuation mark) throws Exception {
		// If Avro messages are batched, submit current batch and punctuation if
		// submitOnPunct
		if (embedAvroSchema) {
			if (submitOnPunct && mark == Punctuation.WINDOW_MARKER)
				submitAvroToOuput();
			if (mark == Punctuation.FINAL_MARKER)
				submitAvroToOuput();
		}
		// Else forward window punctuation mark to the output port
		else
			super.processPunctuation(inputStream, mark);
	}

	static final String DESC = "This operator converts Streams tuples into binary Avro messages. The input tuples can be"
			+ "nested types with lists and tuples, but the attribute types must be mappable to the Avro primitive types. "
			+ "boolean, float32, float64, int32, int64, rstring and ustring are respectively mapped to "
			+ "Boolean, Float, Double, Integer, Long, String";

}
