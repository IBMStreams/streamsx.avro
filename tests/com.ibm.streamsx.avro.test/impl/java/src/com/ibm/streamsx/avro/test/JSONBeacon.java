
package com.ibm.streamsx.avro.test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ibm.json.java.JSONObject;
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
@PrimitiveOperator(name = "JSONBeacon", namespace = "com.ibm.streamsx.avro.test", description = "Generates JSON strings and submits as tuples", comment = JSONBeacon.IBM_COPYRIGHT)
@OutputPorts({
		@OutputPortSet(cardinality = 1, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating, description = "Port requiring `name` and `value` attributes representing a system property. Optional `tags` attribute containing tags for the property") })
public class JSONBeacon extends ProcessTupleProducer {

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
		checker.checkRequiredAttributes(context.getStreamingOutputs().get(0), "jsonString");
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

		final StreamingOutput<OutputTuple> out = getOutput(0);

		JSONObject json = new JSONObject();
		for (int i = 0; i < 100; i++) {
			json.put("username", "Frank");
			json.put("tweet", "This JSON message also rocks: " + i);
			json.put("timestamp", new Long(1048298240L + 1));

			/* Now submit tuple */
			OutputTuple tuple = out.newTuple();
			String jsonString = json.serialize();
			tuple.setString(0, jsonString);
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