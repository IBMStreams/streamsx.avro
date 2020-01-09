//
// *******************************************************************************
// * Copyright (C)2018, International Business Machines Corporation and *
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
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
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
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.types.Blob;

/**
 * Processes Avro tuples and converts them to a JSON string
 * 
 */

@PrimitiveOperator(name = AvroToJSON.OPER_NAME, namespace = "com.ibm.streamsx.avro", description = AvroToJSON.DESC)
@InputPorts({
		@InputPortSet(description = "Port that receives the Apache Avro data blocks. Window punctuation markers are passed to the output port.", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples with the JSON message string and optionally with the JSON key string. Window punctuation markers are forwarded from the input port.", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Icons(location16 = "icons/AvroToJson_16.gif", location32 = "icons/AvroToJson_32.gif")
@Libraries(value = { "opt/downloaded/*" })
public class AvroToJSON extends AbstractOperator {

	public static final String OPER_NAME = "AvroToJSON";
	
	private static Logger tracer = Logger.getLogger(AvroToJSON.class.getName());

	private String inputAvroMessage = null;
	private final String DEFAULT_INPUT_AVRO_MSG_ATTRIBUTE = "avroMessage";
	private String inputAvroKey = null;
	private final String DEFAULT_INPUT_AVRO_KEY_ATTRIBUTE = "avroKey";
	private String outputJsonMessage = null;
	private final String DEFAULT_OUTPUT_JSON_MSG_ATTRIBUTE = "jsonMessage";
	private String outputJsonKey = null;
	private final String DEFAULT_OUTPUT_JSON_KEY_ATTRIBUTE = "jsonKey";

	protected String avroMessageSchemaFile = "";
	protected String avroKeySchemaFile = "";
	protected boolean avroSchemaEmbedded = true;
	Schema messageSchema;
	Schema keySchema;

	@Parameter(optional = true, description = "The input stream attribute which contains the input Avro message blob. This attribute must be of type blob. Default is the sole output attribute when the schema has one attribute otherwise `avroMessage`.")
	public void setInputAvroMessage(String inputAvroMessage) {
		this.inputAvroMessage = inputAvroMessage;
	}

	@Parameter(optional = true, description = "The input stream attribute which contains the input Avro key blob. This attribute must be of type blob. If not specified, the default attribute is `avroKey`.")
	public void setInputAvroKey(String inputAvroKey) {
		this.inputAvroKey = inputAvroKey;
	}

	@Parameter(optional = true, description = "The output stream attribute which contains the output JSON message string. This attribute must be of `rstring` or `ustring` type. Default is the sole output attribute when the schema has one attribute otherwise `jsonMessage`.")
	public void setOutputJsonMessage(String outputJsonMessage) {
		this.outputJsonMessage = outputJsonMessage;
	}

	@Parameter(optional = true, description = "The output stream attribute which contains the output JSON key string. This attribute must be of `rstring` or `ustring` type. If not specified, the default attribute is `jsonKey`.")
	public void setOutputJsonKey(String outputJsonKey) {
		this.outputJsonKey = outputJsonKey;
	}

	@Parameter(optional = true, description = "File that contains the Avro schema to deserialize the binary Avro message. If this parameter is non empty, the operator works in mode `No Avro Schema Embedded`.")
	public void setAvroMessageSchemaFile(String avroMessageSchemaFile) {
		this.avroMessageSchemaFile = avroMessageSchemaFile;
		if (!avroMessageSchemaFile.isEmpty())
			avroSchemaEmbedded = false;
	}

	@Parameter(optional = true, description = "File that contains the Avro schema to deserialize the binary Avro key. If this parameter is non empty, the operator works in mode `No Avro Schema Embedded`.")
	public void setAvroKeySchemaFile(String avroKeySchemaFile) {
		this.avroKeySchemaFile = avroKeySchemaFile;
		if (!avroKeySchemaFile.isEmpty())
			avroSchemaEmbedded = false;
	}

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

		// If no input Avro message blob attribute specified, use default
		if (inputAvroMessage == null) {
			if (ssIp0.getAttributeCount() == 1) {
				inputAvroMessage = ssIp0.getAttribute(0).getName();
			} else {
				inputAvroMessage = DEFAULT_INPUT_AVRO_MSG_ATTRIBUTE;
			}
		}
		tracer.log(TraceLevel.TRACE, "Input Avro message attribute: " + inputAvroMessage);

		// If no Avro key attribute specified, check if optional attribute is
		// available in the input tuple
		if (inputAvroKey == null)
			if (ssIp0.getAttribute(DEFAULT_INPUT_AVRO_KEY_ATTRIBUTE) != null)
				inputAvroKey = DEFAULT_INPUT_AVRO_KEY_ATTRIBUTE;
		if (inputAvroKey != null)
			tracer.log(TraceLevel.TRACE, "Input Avro key attribute: " + inputAvroKey);

		// If no output JSON message attribute specified, use default
		if (outputJsonMessage == null) {
			if (ssOp0.getAttributeCount() == 1) {
				outputJsonMessage = ssOp0.getAttribute(0).getName();
			} else {
				outputJsonMessage = DEFAULT_OUTPUT_JSON_MSG_ATTRIBUTE;
			}
		}
		tracer.log(TraceLevel.TRACE, "Output JSON message attribute: " + outputJsonMessage);
		Attribute outputJsonMessageAttribute = ssOp0.getAttribute(outputJsonMessage);
		if (outputJsonMessageAttribute == null) {
			tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_OUTPUT_ATTRIBUTE_NOT_FOUND", "outputJsonMessage", outputJsonMessage));
			throw new IllegalArgumentException(Messages.getString("AVRO_OUTPUT_ATTRIBUTE_NOT_FOUND", "outputJsonMessage", outputJsonMessage));
		} else {
			MetaType attributeType = outputJsonMessageAttribute.getType().getMetaType();
			if(attributeType!=MetaType.USTRING && attributeType!=MetaType.RSTRING) {
				tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "outputJsonMessage", outputJsonMessage, "rstring or ustring"));
				throw new IllegalArgumentException(Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "outputJsonMessage", outputJsonMessage, "rstring or ustring"));
			}
		}

		// If no JSON key attribute specified, check if optional attribute is
		// available in the output tuple
		if (outputJsonKey == null) {
			if (ssIp0.getAttribute(DEFAULT_OUTPUT_JSON_KEY_ATTRIBUTE) != null) {
				outputJsonKey = DEFAULT_OUTPUT_JSON_KEY_ATTRIBUTE;
			}
		}
		if (outputJsonKey != null) {
			tracer.log(TraceLevel.TRACE, "Output JSON key attribute: " + outputJsonKey);
			Attribute attribute = ssOp0.getAttribute(outputJsonKey);
			if (attribute == null) {
				tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_OUTPUT_ATTRIBUTE_NOT_FOUND", "outputJsonKey", outputJsonKey));
				throw new IllegalArgumentException(Messages.getString("AVRO_OUTPUT_ATTRIBUTE_NOT_FOUND", "outputJsonKey", outputJsonKey));
			} else {
				MetaType attributeType = attribute.getType().getMetaType();
				if(attributeType!=MetaType.USTRING && attributeType!=MetaType.RSTRING) {
					tracer.log(TraceLevel.ERROR, Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "outputJsonKey", outputJsonKey, "rstring or ustring"));
					throw new IllegalArgumentException(Messages.getString("AVRO_ATTRIBUTE_WRONG_TYPE", "outputJsonKey", outputJsonKey, "rstring or ustring"));
				}
			}
		}

		// Get the Avro message schema file to parse the Avro messages
		if (!avroMessageSchemaFile.isEmpty()) {
			tracer.log(TraceLevel.TRACE, "Retrieving and parsing Avro message schema file " + avroMessageSchemaFile);
			InputStream avscMessageInput = new FileInputStream(avroMessageSchemaFile);
			messageSchema = new Schema.Parser().parse(avscMessageInput);
		}

		// Get the Avro key schema file to parse the Avro messages
		if (!avroKeySchemaFile.isEmpty()) {
			tracer.log(TraceLevel.TRACE, "Retrieving and parsing Avro key schema file " + avroKeySchemaFile);
			InputStream avscKeyInput = new FileInputStream(avroKeySchemaFile);
			keySchema = new Schema.Parser().parse(avscKeyInput);
		}

		// If the schema is embedded in the message, the schema file must not be specified
		if (!avroSchemaEmbedded && avroKeySchemaFile.isEmpty() && (inputAvroKey != null))
			throw new IllegalArgumentException("Operator mode is No Avro Schema Embedded, inputAvroKey is present but no parameter avroKeySchemaFile is present.");
		
		tracer.log(TraceLevel.TRACE, "AvroToJSON operator initialized, ready to receive tuples");

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

		// Get the incoming binary Avro message record(s)
		Blob avroMessage = tuple.getBlob(inputAvroMessage);
		if (tracer.isTraceEnabled())
			tracer.log(TraceLevel.TRACE, "Processing Avro message with length " + avroMessage.getLength());
		// Get the incoming binary Avro key (if specified)
		Blob avroKey = null;
		if (inputAvroKey != null) {
			avroKey = tuple.getBlob(inputAvroKey);
			if (tracer.isTraceEnabled())
				tracer.log(TraceLevel.TRACE, "Processing Avro key with length " + avroKey.getLength());
		}

		// Submit JSON tuples based on the Avro content received in the Blob
		try {
			if (!avroSchemaEmbedded) {
				processAvroMessage(avroMessage, avroKey, outStream, outTuple, messageSchema, keySchema);
			} else {
				processAvroMessage(avroMessage, outStream, outTuple);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Processes an Avro Blob containing a single message and with no embedded
	 * schema. This is the pattern when Avro objects are passed over messaging
	 * infrastructure such as Apache Kafka.
	 * 
	 * @param avroMessage
	 *            The Blob that holds the single Avro message object
	 * @param avroKey
	 *            The Blob that holds the single Avro key object (if passed)
	 * @param outStream
	 *            The stream to which the JSON string must be submitted
	 * @param outTuple
	 *            The tuple holding the JSON string
	 * @param messageSchema
	 *            The schema of the Avro messsage object
	 * @param keySchema
	 *            The schema of the Avro key object
	 * @throws Exception
	 */
	private void processAvroMessage(Blob avroMessage, Blob avroKey, StreamingOutput<OutputTuple> outStream,
			OutputTuple outTuple, Schema messageSchema, Schema keySchema) throws Exception {
		// Deserialize message
		GenericDatumReader<GenericRecord> consumer = new GenericDatumReader<GenericRecord>(messageSchema);
		ByteArrayInputStream consumedByteArray = new ByteArrayInputStream(avroMessage.getData());
		Decoder consumedDecoder = DecoderFactory.get().binaryDecoder(consumedByteArray, null);
		GenericRecord consumedDatum = consumer.read(null, consumedDecoder);
		if (tracer.isTraceEnabled())
			tracer.log(TraceLevel.TRACE, "JSON representation of Avro message: " + consumedDatum.toString());
		outTuple.setString(outputJsonMessage, consumedDatum.toString());
		// Deserialize key (if specified)
		if (avroKey != null) {
			consumer = new GenericDatumReader<GenericRecord>(keySchema);
			consumedByteArray = new ByteArrayInputStream(avroKey.getData());
			consumedDecoder = DecoderFactory.get().binaryDecoder(consumedByteArray, null);
			consumedDatum = consumer.read(null, consumedDecoder);
			if (tracer.isTraceEnabled())
				tracer.log(TraceLevel.TRACE, "JSON representation of Avro key: " + consumedDatum.toString());
			if (outputJsonKey != null)
				outTuple.setString(outputJsonKey, consumedDatum.toString());
		}
		// Submit new tuple to output port 0
		outStream.submit(outTuple);
	}

	/**
	 * Processes a blob which contains one or more Avro messages and has the
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
			if (tracer.isTraceEnabled())
				tracer.log(TraceLevel.TRACE, "JSON representation of Avro message: " + consumedDatum.toString());
			// Submit new tuple to output port 0
			outTuple.setString(outputJsonMessage, consumedDatum.toString());
			outStream.submit(outTuple);
		}
		is.close();
		dataFileReader.close();
	}

	static final String DESC = "This operator converts binary Avro messages and optionally message keys into a JSON string. "
			+ "The operator has two operation modes::\\n"
			+ "* Avro Schema Embedded: The operator processes a blob which contains one or more Avro messages and has the schema embedded. "
			+ "This is the pattern when Avro objects are read from a file (either local file system or HDFS). Every Avro "
			+ "object in the blob is converted to JSON and then submitted to the output port. This operation mode is entered "
			+ "if both parameters `avroMessageSchemaFile` and `avroKeySchemaFile` are not existing or have an empty value.\\n"
			+ "* No Avro Schema Embedded: The operator processes an Avro Blob containing a single message and with no embedded "
			+ "schema. This is the pattern when Avro objects are passed over messaging infrastructure such as Apache Kafka. "
			+ "This operation mode is entered if one or both parameters `avroMessageSchemaFile` and `avroKeySchemaFile` "
			+ "are specified.\\n"
			+ "If an input or output message or key attribute is not found or has an incompatible type, the operator will fail.\\n"
			+ "This operator must not be used inside a consistent region.";

}
