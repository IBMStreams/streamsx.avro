
package com.ibm.streamsx.avro.test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.ValueFactory;

/**
 * Sample source operator using a {@code process()} method. Operator that reads
 * the Java System property set and submits an output tuple for each system
 * property. A single output port is assumed with attributes {@code name} for
 * the property name, {@code value} for its value and {@code tags} for a set of
 * tags set from parameters.
 * <P>
 * The class handles two parameters to provide a simple tagging scheme. This is
 * used to demonstrate parameter handling. <BR>
 * If the parameter <code>tagged</code> is set then any tuple for a system
 * property that starts with <code>tagged</code>'s value will have its
 * <code>tags</code> attribute set to the value of the <code>tags</code>
 * parameter.
 * </P>
 * <P>
 * The parameter <code>initDelay</code> is inherited from the parent class
 * TupleProducer.
 * </P>
 * <P>
 * This operator provided as the sample Java primitive operator <BR>
 * {@code com.ibm.streams.javaprimitivesamples.sources.SystemPropertySource}
 * <BR>
 * in the sample {@code JavaOperators} toolkit located in: <BR>
 * {@code $STREAMS_INSTALL/samples/spl/feature/JavaOperators}
 * </P>
 * 
 * @see com.ibm.streams.operator.samples.patterns.TupleProducer
 */
@PrimitiveOperator(name = "AvroBeacon", namespace = "com.ibm.streamsx.avro.test", description = "Produces Avro messages and submits BLOB tuples", comment = AvroBeacon.IBM_COPYRIGHT)
@OutputPorts({
		@OutputPortSet(cardinality = 1, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating, description = "Port requiring `name` and `value` attributes representing a system property. Optional `tags` attribute containing tags for the property") })
public class AvroBeacon extends ProcessTupleProducer {

	private String tagged;

	private Set<String> tags;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
	}

	/**
	 * If parameter {@code tagged} is set then {@code tags} is required.
	 * 
	 * @param checker
	 *            Checker for this invocation
	 */
	@ContextCheck
	public static void checkTaggedParameters(OperatorContextChecker checker) {
		checker.checkDependentParameters("tagged", "tags");
		OperatorContext context = checker.getOperatorContext();
		if (context.getParameterNames().contains("tagged")) {
			checker.checkRequiredAttributes(context.getStreamingOutputs().get(0), "tags");
		}
	}

	/**
	 * Check the output attributes name and value are present.
	 */
	@ContextCheck
	public static void checkOuputAttributes(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		checker.checkRequiredAttributes(context.getStreamingOutputs().get(0), "avroBlob");
	}

	/**
	 * Set the tagged parameter.
	 * 
	 * @param tagged
	 *            Prefix indicated tagged system properties.
	 */
	@Parameter(optional = true, description = "Prefix for property names to be tagged with the value of `tags`.")
	public void setTagged(String tagged) {
		this.tagged = tagged;
	}

	/**
	 * Set the tags parameter.
	 * 
	 * @param tags
	 *            Tags to be associated with tagged properties.
	 */
	@Parameter(optional = true, description = "Tags to set in `tags` output attribute when the property name starts with the value of `taggged`.")
	public void setTags(List<String> tags) {
		this.tags = new HashSet<String>(tags);
	}

	@Override
	protected void process() throws Exception {

		InputStream avscInput = new FileInputStream("data/twitter.avsc");
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(avscInput);

		final StreamingOutput<OutputTuple> out = getOutput(0);

		/* Serializing to a byte array */
		for (int i = 0; i < 100; i++) {
			GenericRecord producedDatum = new GenericData.Record(schema);
			producedDatum.put("username", "Frank");
			producedDatum.put("tweet", "This Avro message really rocks: " + i);
			producedDatum.put("timestamp", new Long(1048298232L + i));
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			Encoder e = EncoderFactory.get().binaryEncoder(os, null);
			writer.write(producedDatum, e);
			e.flush();
			byte[] byteData = os.toByteArray();
			os.close();

			/* Now submit tuple */
			OutputTuple tuple = out.newTuple();
			Blob blobData = ValueFactory.newBlob(byteData);
			tuple.setBlob(0, blobData);
			out.submit(tuple);
		}

		// Make the set of tuples a window.
		out.punctuate(Punctuation.WINDOW_MARKER);

	}

	/**
	 * Iterate over all the system properties submitting a tuple for each name
	 * value pair.
	 */
	protected void getAvroFileAndSubmit() throws Exception {

	}
}