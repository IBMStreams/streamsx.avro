
package com.ibm.streamsx.avro.test;

import java.math.BigDecimal;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;
import com.ibm.streams.operator.types.XML;

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
@PrimitiveOperator(name = "TupleBeacon", namespace = "com.ibm.streamsx.avro.test", description = "Generates and submits tuples", comment = TupleBeacon.IBM_COPYRIGHT)
@OutputPorts({
		@OutputPortSet(cardinality = 1, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating, description = "Port requiring `name` and `value` attributes representing a system property. Optional `tags` attribute containing tags for the property") })
public class TupleBeacon extends ProcessTupleProducer {

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

		for (int i = 0; i < 100; i++) {
			String username = "Frank";
			String tweet = "This JSON message also rocks: " + i;
			Long timestamp = new Long(1048298240L + i);

			StreamSchema schema = out.getStreamSchema();
			StreamSchema locationSchema = ((TupleType) schema.getAttribute("location").getType()).getTupleSchema();
			for (String attributeName : schema.getAttributeNames()) {
				Type type = schema.getAttribute(attributeName).getType();
			}

			Map<String, Object> locationMap = new HashMap<String, Object>();
			locationMap.put("country", new RString("DK"));
			locationMap.put("lat", new Float(41.24 + i));
			locationMap.put("lon", new Float(-5.1 - i));
			Tuple locationTuple = locationSchema.getTuple(locationMap);

			List<RString> retweetList = new ArrayList<RString>();
			for (int j = 0; j < 10; j++)
				retweetList.add(new RString("User " + j));

			List<Tuple> followersList = new ArrayList<Tuple>();
			Type followerSchemaType = ((CollectionType) schema.getAttribute("followers").getType()).getElementType();
			StreamSchema followerSchema = ((TupleType) followerSchemaType).getTupleSchema();
			for (int j = 0; j < 5; j++) {
				Map<String, Object> followerMap = new HashMap<String, Object>();
				followerMap.put("followeruser", new RString("Follower " + j));
				followerMap.put("rate", new Double(j));
				Tuple followerTuple = followerSchema.getTuple(followerMap);
				followersList.add(followerTuple);
			}

			/* Now submit tuple */
			OutputTuple tuple = out.newTuple();
			tuple.setString("username", username);
			tuple.setString("tweet", tweet);
			tuple.setLong("timestamp", timestamp);
			tuple.setBoolean("suspiciousContent", (i % 3 == 0));
			tuple.setTuple("location", locationTuple);
			tuple.setList("retweets", retweetList);
			tuple.setList("followers", followersList);
			out.submit(tuple);
			if (i != 0 && i % 20 == 0)
				out.punctuate(Punctuation.WINDOW_MARKER);
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