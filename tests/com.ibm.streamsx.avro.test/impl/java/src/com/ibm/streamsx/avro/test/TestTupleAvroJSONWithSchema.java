package com.ibm.streamsx.avro.test;

import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.avro.AvroToJSON;
import com.ibm.streamsx.avro.TupleToAvro;
import com.ibm.streamsx.avro.test.Display;

public class TestTupleAvroJSONWithSchema {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String tweetT = "tuple<rstring username, ustring tweet, int64 timestamp, boolean suspiciousContent, "
				+ "tuple<rstring country, float32 lat, float32 lon> location, " + "list<rstring> retweets, "
				+ "list<tuple<rstring followeruser, float64 rate>> followers, int64 nonExistingField>";
		String avroBlobT = "tuple<blob avroMessage>";
		String jsonStringT = "tuple<rstring jsonMessage>";

		OperatorGraph graph = OperatorGraphFactory.newGraph();

		OperatorInvocation<TupleBeacon> tupleBeaconOp = graph.addOperator(TupleBeacon.class);
		OutputPortDeclaration tupleBeaconOut = tupleBeaconOp.addOutput(tweetT);

		// Declare a TupleToAvro operator
		OperatorInvocation<TupleToAvro> tupleToAvroOp = graph.addOperator(TupleToAvro.class);
		tupleToAvroOp.setStringParameter("avroMessageSchemaFile", "data/twitter_complex.avsc");
		tupleToAvroOp.setBooleanParameter("embedAvroSchema", true);
		tupleToAvroOp.setLongParameter("timePerMessage", 60);
		tupleToAvroOp.setBooleanParameter("ignoreParsingError", false);
		tupleToAvroOp.addInput(tupleBeaconOut);
		OutputPortDeclaration tupleToAvroOut = tupleToAvroOp.addOutput(avroBlobT);

		// Use the output of the TupleToAvro to convert to JSON
		OperatorInvocation<AvroToJSON> avroToJSONOp = graph.addOperator(AvroToJSON.class);
		avroToJSONOp.setBooleanParameter("avroSchemaEmbedded", true);
		avroToJSONOp.addInput(tupleToAvroOut);
		OutputPortDeclaration avroToJsonOut = avroToJSONOp.addOutput(jsonStringT);

		// Declare the Display operator to display to stdout
		OperatorInvocation<Display> display = graph.addOperator(Display.class);
		// display.addInput(tupleBeaconOut);
		display.addInput(tupleToAvroOut);
		display.addInput(avroToJsonOut);

		/*
		 * Initialize the framework and declare the operator
		 */
		JavaOperatorTester tester = new JavaOperatorTester();
		JavaTestableGraph executableGraph;
		// Future<JavaTestableGraph> future = null;
		try {
			executableGraph = tester.executable(graph);
			executableGraph.setTraceLevel(TraceLevel.TRACE);
			executableGraph.executeToCompletion();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}