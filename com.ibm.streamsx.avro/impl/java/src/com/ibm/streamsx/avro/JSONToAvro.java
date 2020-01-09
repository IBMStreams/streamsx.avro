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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.log4j.TraceLevel;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.ValueFactory;

/**
 * Processes Avro tuples and converts them to a JSON string
 * 
 */

@PrimitiveOperator(name = JSONToAvro.OPER_NAME, namespace = "com.ibm.streamsx.avro", description = JSONToAvro.DESC)
@InputPorts({
		@InputPortSet(description = "Port that ingests JSON records.", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({
		@OutputPortSet(description = "Port that produces Avro records.", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Icons(location16 = "icons/JsonToAvro_16.gif", location32 = "icons/JsonToAvro_32.gif")
@Libraries(value = { "opt/downloaded/*" })
public class JSONToAvro extends AbstractOperator {

	public static final String OPER_NAME = "JSONToAvro";
	
	private static Logger tracer = Logger.getLogger(JSONToAvro.class.getName());

	private String inputJsonMessage = null;
	private final String DEFAULT_INPUT_JSON_MSG_ATTRIBUTE = "jsonMessage";
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

	@Parameter(optional = true, description = "The input stream attribute which contains the input JSON message string. This attribute must be of `rstring` or `ustring` type. Default is the sole input attribute when the schema has one attribute otherwise `jsonMessage`.")
	public void setInputJsonMessage(String inputJsonMessage) {
		this.inputJsonMessage = inputJsonMessage;
	}

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
			+ "incoming JSON tuples are batched and a large binary object that contains the Avro schema "
			+ "and 1 or more messages is generated. Also, you must specify one of the parameters (submitOnPunct, "
			+ "bytesPerMessage, tuplesPerMessage, timePerMessage) that controls "
			+ "when Avro message block is submitted to the output port."
			+ "After submitting the Avro message to the output port, a "
			+ "punctuation is generated so that the receiving operator can potentially create a new file.")
	public void setEmbedAvroSchema(Boolean embedAvroSchema) {
		this.embedAvroSchema = embedAvroSchema;
	}

	@Parameter(optional = true, description = "When set to true, the operator will submit the block of Avro messages what "
			+ "was built and generate a punctuation so that the receiving operator can potentially create a new file. Default is false. "
			+ "Only valid if Avro schema is embedded in the output.")
	public void setSubmitOnPunct(Boolean submitOnPunct) {
		this.submitOnPunct = submitOnPunct;
	}

	@Parameter(optional = true, description = "This parameter controls the minimum size in bytes that the Avro message "
			+ "block should be before it is submitted to the output port. Default value is 0l (disabled). Only valid if Avro "
			+ "schema is embedded in the output.")
	public void setBytesPerMessage(Long bytesPerMessage) {
		this.bytesPerMessage = bytesPerMessage;
	}

	@Parameter(optional = true, description = "This parameter controls the minimum number of tuples that the Avro message block "
			+ "should contain before it is submitted to the output port. Default is 0l (disabled). Only valid if Avro schema "
			+ "is embedded in the output.")
	public void setTuplesPerMessage(Long tuplesPerMessage) {
		this.tuplesPerMessage = tuplesPerMessage;
	}

	@Parameter(optional = true, description = "This parameter controls the maximum time in seconds before the Avro message block "
			+ "is submitted to the output port. Default value is 0l (disabled). Only valid if Avro schema is embedded in the output.")
	public void setTimePerMessage(Long timePerMessage) {
		this.timePerMessage = timePerMessage;
	}

	@Parameter(optional = true, description = "Ignore any JSON or Avro parsing errors. When set to true, errors that "
			+ "occur when parsing the incoming JSON tuple or constructing the Avro tuple(s) will be ignored and the incoming tuple(s) "
			+ "will be skipped. Default is false.")
	public void setIgnoreParsingError(Boolean ignoreParsingError) {
		this.ignoreParsingError = ignoreParsingError;
	}

	// Variables
	StreamingOutput<OutputTuple> outStream;
	OutputTuple outTuple;
	GenericDatumReader<GenericRecord> jsonReader;
	GenericDatumWriter<GenericRecord> avroWriter;
	DataFileWriter<GenericRecord> avroDataFileWriter;
	ByteArrayOutputStream avroMessageByteArray = new ByteArrayOutputStream();
	ByteArrayOutputStream avroBlockByteArray = new ByteArrayOutputStream();
	long lastSubmitted = System.currentTimeMillis();
	int numberOfBatchedMessages = 0;

	/**
	 * Compile time operator checks: Do not use the operator in a consistent region
	 * @param checker
	 *            The operator context
	 */
	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		if(consistentRegionContext != null) {
			checker.setInvalidContext(Messages.getString("AVRO_NOT_CONSISTENT_REGION", OPER_NAME), new Object[]{});
		}
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
		tracer.log(TraceLevel.TRACE, "Operator " + operatorContext.getName() + " initializing in PE: "
				+ operatorContext.getPE().getPEId() + " in Job: " + operatorContext.getPE().getJobId());

		StreamSchema ssOp0 = getOutput(0).getStreamSchema();
		StreamSchema ssIp0 = getInput(0).getStreamSchema();

		// If no input JSON attribute specified, use default
		if (inputJsonMessage == null) {
			if (ssIp0.getAttributeCount() == 1) {
				inputJsonMessage = ssIp0.getAttribute(0).getName();
			} else {
				inputJsonMessage = DEFAULT_INPUT_JSON_MSG_ATTRIBUTE;
			}
		}
		tracer.log(TraceLevel.TRACE, "Input JSON message attribute: " + inputJsonMessage);
		Attribute inputJsonMessageAttribute = ssIp0.getAttribute(inputJsonMessage);
		if (inputJsonMessageAttribute == null) {
			tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_INPUT_ATTRIBUTE_NOT_FOUND", "inputJsonMessage", inputJsonMessage));
			throw new IllegalArgumentException(Messages.getString("AVRO_INPUT_ATTRIBUTE_NOT_FOUND", "inputJsonMessage", inputJsonMessage));
		} else {
			MetaType attributeType = inputJsonMessageAttribute.getType().getMetaType();
			if (attributeType!=MetaType.RSTRING && attributeType!=MetaType.USTRING) {
				tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "inputJsonMessage", inputJsonMessage, "rstring or ustring"));
				throw new IllegalArgumentException(Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "inputJsonMessage", inputJsonMessage, "rstring or ustring"));
			}
		}

		// If no output Avro message attribute specified, use default
		if (outputAvroMessage == null) {
			if (ssOp0.getAttributeCount() == 1) {
				outputAvroMessage = ssOp0.getAttribute(0).getName();
			} else {
				outputAvroMessage = DEFAULT_OUTPUT_AVRO_MSG_ATTRIBUTE;
			}
		}
		tracer.log(TraceLevel.TRACE, "Output Avro message attribute: " + outputAvroMessage);
		Attribute outputAvroMessageAttribute = ssOp0.getAttribute(outputAvroMessage);
		if (outputAvroMessageAttribute == null) {
			tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_OUTPUT_ATTRIBUTE_NOT_FOUND", "outputAvroMessage", outputAvroMessage));
			throw new IllegalArgumentException(Messages.getString("AVRO_OUTPUT_ATTRIBUTE_NOT_FOUND", "outputAvroMessage", outputAvroMessage));
		} else {
			MetaType attributeType = outputAvroMessageAttribute.getType().getMetaType();
			if(attributeType!=MetaType.BLOB) {
				tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "outputAvroMessage", outputAvroMessage, "blob"));
				throw new IllegalArgumentException(Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "outputAvroMessage", outputAvroMessage, "blob"));
			}
		}

		// Get the Avro schema file to parse the Avro messages
		tracer.log(TraceLevel.TRACE, "Retrieving and parsing Avro message schema file " + avroMessageSchemaFile);
		InputStream avscInput = new FileInputStream(avroMessageSchemaFile);
		Schema.Parser parser = new Schema.Parser();
		messageSchema = parser.parse(avscInput);

		tracer.log(TraceLevel.TRACE, "Embed Avro schema in generated output Avro message block: " + embedAvroSchema);
		tracer.log(TraceLevel.TRACE, "Submit Avro message block when punctuation is received: " + submitOnPunct);
		tracer.log(TraceLevel.TRACE, "Ignore parsing error: " + ignoreParsingError);

		// submitOnPunct.. is only valid if Avro schema is embedded in the output
		if (!embedAvroSchema && ( submitOnPunct || (tuplesPerMessage != 0) || (bytesPerMessage != 0) || (timePerMessage != 0) ) )
			throw new Exception(
					"Parameters submitOnPunct, tuplesPerMessage, bytesPerMessage or timePerMessage can only be set if Avro schema is embedded in the output.");
		// If Avro schema is embedded in the output, submitOnPunct is mandatory
		if (embedAvroSchema && !submitOnPunct && tuplesPerMessage == 0 && bytesPerMessage == 0 && timePerMessage == 0)
			throw new Exception("If Avro schema is embedded in the output, you must specify one of the thresholds when "
					+ "the tuple must be submitted (submitOnPunct, bytesPerMessage, timePerMessage, tuplesPerMessage).");

		// Prepare and initialize variables that don't change for every input
		// record
		jsonReader = new GenericDatumReader<GenericRecord>(messageSchema);
		avroWriter = new GenericDatumWriter<GenericRecord>(messageSchema);
		avroDataFileWriter = new DataFileWriter<GenericRecord>(avroWriter);
		if (embedAvroSchema)
			avroDataFileWriter.create(messageSchema, avroBlockByteArray);
		numberOfBatchedMessages = 0;

		tracer.log(TraceLevel.TRACE, "JSONToAvro operator initialized, ready to receive tuples");

	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
	 */
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {

		String jsonInput = tuple.getString(inputJsonMessage);

		if (tracer.isTraceEnabled())
			tracer.log(TraceLevel.TRACE, "Input JSON string: " + jsonInput);

		// Create a new tuple for output port 0 and copy over any matching
		// attributes
		outStream = getOutput(0);
		outTuple = outStream.newTuple();
		outTuple.assign(tuple);

		// Decode the JSON string
		GenericRecord datum = null;
		try {
			Decoder decodedJson = DecoderFactory.get().jsonDecoder(messageSchema, jsonInput);
			datum = jsonReader.read(null, decodedJson);

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
			tracer.log(TraceLevel.ERROR, "Error while converting JSON string to AVRO schema: " + e.getMessage()
					+ ". JSON String: " + jsonInput);
			// If parsing errors must not be ignored, make the operator fail
			if (!ignoreParsingError)
				throw new Exception("Error while converting JSON string to AVRO schema: " + e.getMessage()
						+ ". JSON String: " + jsonInput);
		}
	}

	// Submit the Avro byte array to the output port and reset byte array
	private void submitAvroToOuput() throws Exception {
		// Send block of messages with Avro schema included and punctuation
		if (embedAvroSchema) {
			if (numberOfBatchedMessages > 0) {
				if (tracer.isTraceEnabled())
					tracer.log(TraceLevel.TRACE, "Submitting " + numberOfBatchedMessages
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
			if (tracer.isTraceEnabled())
				tracer.log(TraceLevel.TRACE,
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

	static final String DESC = "This operator converts JSON strings into binary Avro messages.\\n\\n"
			+ "If an input or output message attribute is not found or has an incompatible type, the operator will fail. "
			+ "If an invalid JSON string is found in the input, the operator will fail if parameter `ignoreParsingError` is false.\\n\\n"
			+ "If parameter `embedAvroSchema` is false, the operator passes window punctuation marker transparently to the output port. "
			+ "If parameter `embedAvroSchema` is true, the operator generates window punctuation markers.\\n\\n"
			+ "This operator must not be used inside a consistent region.";

}
