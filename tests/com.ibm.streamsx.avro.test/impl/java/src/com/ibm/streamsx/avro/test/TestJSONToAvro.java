package com.ibm.streamsx.avro.test;

import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.avro.AvroToJSON;
import com.ibm.streamsx.avro.JSONToAvro;
import com.ibm.streamsx.avro.test.Display;

public class TestJSONToAvro {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String tupleType_AvroBlob = "tuple<blob avroBlob>";
		String jsonString = "tuple<rstring jsonString>";

		OperatorGraph graph = OperatorGraphFactory.newGraph();

		OperatorInvocation<JSONBeacon> jsonBeaconOp = graph.addOperator(JSONBeacon.class);
		OutputPortDeclaration jsonBeaconOut = jsonBeaconOp.addOutput(jsonString);

		// Declare a JSONToAvro operator
		OperatorInvocation<JSONToAvro> jsonToAvroOp = graph.addOperator(JSONToAvro.class);
		jsonToAvroOp.setStringParameter("avroSchemaFile", "data/twitter.avsc");
		jsonToAvroOp.addInput(jsonBeaconOut);
		OutputPortDeclaration jsonToAvroOut = jsonToAvroOp.addOutput(tupleType_AvroBlob);

		// Use the output of the JSONToAvro to convert back to JSON
		OperatorInvocation<AvroToJSON> avroToJSONOp = graph.addOperator(AvroToJSON.class);
		avroToJSONOp.setStringParameter("avroSchemaFile", "data/twitter.avsc");
		avroToJSONOp.addInput(jsonToAvroOut);
		OutputPortDeclaration avroToJsonOut = avroToJSONOp.addOutput(jsonString);

		// Declare the Display operator to display to stdout
		OperatorInvocation<Display> display = graph.addOperator(Display.class);
		display.addInput(jsonToAvroOut);
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