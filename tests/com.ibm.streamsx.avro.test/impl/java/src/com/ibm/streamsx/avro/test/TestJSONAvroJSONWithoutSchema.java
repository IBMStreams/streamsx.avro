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

public class TestJSONAvroJSONWithoutSchema {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String avroBlobT = "tuple<blob avroMessage, uint64 timeStamp>";
		String jsonStringT = "tuple<rstring jsonMessage, uint64 timeStamp>";

		OperatorGraph graph = OperatorGraphFactory.newGraph();

		OperatorInvocation<JSONBeacon> jsonBeaconOp = graph.addOperator(JSONBeacon.class);
		OutputPortDeclaration jsonBeaconOut = jsonBeaconOp.addOutput(jsonStringT);

		// Declare a JSONToAvro operator
		OperatorInvocation<JSONToAvro> jsonToAvroOp = graph.addOperator(JSONToAvro.class);
		jsonToAvroOp.setStringParameter("avroMessageSchemaFile", "data/twitter.avsc");
		jsonToAvroOp.setBooleanParameter("embedAvroSchema", false);
		jsonToAvroOp.setBooleanParameter("ignoreParsingError", true);
		jsonToAvroOp.addInput(jsonBeaconOut);
		OutputPortDeclaration jsonToAvroOut = jsonToAvroOp.addOutput(avroBlobT);

		// Use the output of the JSONToAvro to convert back to JSON
		OperatorInvocation<AvroToJSON> avroToJSONOp = graph.addOperator(AvroToJSON.class);
		avroToJSONOp.setStringParameter("avroMessageSchemaFile", "data/twitter.avsc");
		avroToJSONOp.setBooleanParameter("avroSchemaEmbedded", false);
		avroToJSONOp.addInput(jsonToAvroOut);
		OutputPortDeclaration avroToJsonOut = avroToJSONOp.addOutput(jsonStringT);

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