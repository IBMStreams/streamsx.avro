package com.ibm.streamsx.avro.test;

import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.avro.AvroToJSON;
import com.ibm.streamsx.avro.test.Display;

public class TestAvroToJSON {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String tupleType_AvroBlob = "tuple<blob avroBlob>";
		String jsonString = "tuple<rstring jsonString>";

		OperatorGraph graph = OperatorGraphFactory.newGraph();

		// Generate 100 Avro messages
		OperatorInvocation<AvroBeacon> avroBeaconOp = graph.addOperator(AvroBeacon.class);
		OutputPortDeclaration avroBeaconOut = avroBeaconOp.addOutput(tupleType_AvroBlob);

		// Declare a AvroToJSON operator
		OperatorInvocation<AvroToJSON> avroToJsonOp = graph.addOperator(AvroToJSON.class);
		avroToJsonOp.setStringParameter("avroSchemaFile", "data/twitter.avsc");
		avroToJsonOp.addInput(avroBeaconOut);
		OutputPortDeclaration avroToJsonOut = avroToJsonOp.addOutput(jsonString);

		// Declare the Display operator to display to stdout
		OperatorInvocation<Display> display = graph.addOperator(Display.class);
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