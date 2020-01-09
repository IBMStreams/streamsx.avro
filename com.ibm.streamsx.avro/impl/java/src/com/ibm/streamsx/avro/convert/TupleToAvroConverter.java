//
// *******************************************************************************
// * Copyright (C)2018, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//

package com.ibm.streamsx.avro.convert;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.log4j.TraceLevel;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.avro.Messages;

public class TupleToAvroConverter {

	private static Logger LOGGER = Logger.getLogger(TupleToAvroConverter.class.getCanonicalName());

	/*
	 * Check the schema of the input tuple (recursively
	 */
	public static boolean isValidTupleToAvroMapping(String tupleSchemaName, StreamSchema tupleSchema, Schema avroSchema)
			throws Exception {
		boolean validMapping = true;
		LOGGER.log(TraceLevel.TRACE,
				"Checking attributes in tuple schema " + tupleSchemaName + ": " + tupleSchema.getAttributeNames());
		for (String attributeName : tupleSchema.getAttributeNames()) {
			Attribute attribute = tupleSchema.getAttribute(attributeName);
			Field avroField = avroSchema.getField(attributeName);
			if (avroField != null)
				validMapping = validMapping
						& isValidAttributeToAvroMapping(attributeName, attribute.getType(), avroField.schema());
			else
				LOGGER.log(TraceLevel.INFO, "Attribute " + attributeName + " in schema " + tupleSchemaName
						+ " does not have a corresponding field in the Avro schema. It will not be mapped.");
		}
		return validMapping;
	}

	/*
	 * Check that the tuple input attribute has a valid type (recursively)
	 */
	private static boolean isValidAttributeToAvroMapping(String attributeName, Type tupleAttributeType,
			Schema avroSchema) throws Exception {
		boolean validMapping = true;
		MetaType attributeMetaType = tupleAttributeType.getMetaType();
		Schema.Type avroSchemaType = avroSchema.getType();
		LOGGER.log(TraceLevel.TRACE, "Checking attribute: " + attributeName + ", type: " + tupleAttributeType
				+ ", metatype: " + attributeMetaType + ". Avro type is: " + avroSchemaType);
		if (!SupportedTypes.SUPPORTED_STREAMS_TYPES.contains(attributeMetaType)) {		
			LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_TYPE_NOT_SUPPORTED", tupleAttributeType, attributeName, SupportedTypes.SUPPORTED_STREAMS_TYPES));
			validMapping = false;
		} else {
			switch (attributeMetaType) {
			case BOOLEAN:
				if (avroSchemaType != Schema.Type.BOOLEAN) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "boolean attribute " + attributeName, "Boolean type", avroSchemaType));
					validMapping = false;
				}
				break;
			case FLOAT32:
				if (avroSchemaType != Schema.Type.FLOAT) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "float32 attribute " + attributeName, "Float type", avroSchemaType));
					validMapping = false;
				}
				break;
			case FLOAT64:
				if (avroSchemaType != Schema.Type.DOUBLE) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "float64 attribute " + attributeName, "Double type", avroSchemaType));
					validMapping = false;
				}
				break;
			case INT32:
				if (avroSchemaType != Schema.Type.INT) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "int32 attribute " + attributeName, "Integer type", avroSchemaType));
					validMapping = false;
				}
				break;
			case INT64:
				if (avroSchemaType != Schema.Type.LONG) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "int64 attribute " + attributeName, "Long type", avroSchemaType));
					validMapping = false;
				}
				break;
			case RSTRING:
			case USTRING:
				if (avroSchemaType != Schema.Type.STRING) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "rstring or ustring attribute " + attributeName, "String type", avroSchemaType));
					validMapping = false;
				}
				break;
			case TUPLE:
				if (avroSchemaType != Schema.Type.RECORD) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "tuple attribute " + attributeName, "Record type", avroSchemaType));
					validMapping = false;
				} else {
					StreamSchema subStreamSchema = ((TupleType) tupleAttributeType).getTupleSchema();
					Schema subAvroSchema = avroSchema;
					validMapping = isValidTupleToAvroMapping(attributeName, subStreamSchema, subAvroSchema);
				}
				break;
			case LIST:
				if (avroSchemaType != Schema.Type.ARRAY) {
					LOGGER.log(TraceLevel.ERROR, Messages.getString ("AVRO_WRONG_MAPPING", "list<> attribute " + attributeName, "Array type", avroSchemaType));
					validMapping = false;
				} else {
					// Obtain the type of the elements contained in the Streams
					// list
					Type tupleElementType = ((CollectionType) tupleAttributeType).getElementType();
					// Obtain the type of the elements contained in the Avro
					// array
					Schema avroArrayElementType = avroSchema.getElementType();
					validMapping = isValidAttributeToAvroMapping(attributeName, tupleElementType, avroArrayElementType);
				}
				break;
			default:
				LOGGER.log(TraceLevel.WARN,
						"Ignoring attribute " + attributeName + " because of unsupported type " + tupleAttributeType);
			}
		}

		return validMapping;
	}

	/*
	 * Convert the an input schema to an Avro Generic Record
	 */
	public static GenericRecord convertTupleToAvro(Tuple tuple, StreamSchema streamSchema, Schema avroSchema) {
		GenericRecord datum = new GenericData.Record(avroSchema);
		for (String attributeName : streamSchema.getAttributeNames()) {
			Attribute attribute = streamSchema.getAttribute(attributeName);
			Object tupleAttribute = tuple.getObject(attributeName);
			Field avroField = avroSchema.getField(attributeName);
			// If there is an Avro field associated with this attribute, convert
			if (avroField != null)
				datum.put(attributeName,
						convertAttributeToAvro(attributeName, tupleAttribute, attribute.getType(), avroField.schema()));
		}
		return datum;
	}

	/*
	 * Get the Avro object for the appropriate type
	 */
	private static Object convertAttributeToAvro(String attributeName, Object tupleAttribute, Type tupleAttributeType,
			Schema avroSchema) {
		Object returnObject = null;
		MetaType metaType = tupleAttributeType.getMetaType();
		switch (metaType) {
		case BOOLEAN:
			returnObject = (Boolean) tupleAttribute;
			break;
		case FLOAT32:
			returnObject = (Float) tupleAttribute;
			break;
		case FLOAT64:
			returnObject = (Double) tupleAttribute;
			break;
		case INT32:
			returnObject = (Integer) tupleAttribute;
			break;
		case INT64:
			returnObject = (Long) tupleAttribute;
			break;
		case RSTRING:
			returnObject = ((RString) tupleAttribute).getString();
			break;
		case USTRING:
			returnObject = tupleAttribute.toString();
			break;
		case TUPLE:
			Tuple subTuple = (Tuple) tupleAttribute;
			StreamSchema subStreamSchema = subTuple.getStreamSchema();
			GenericRecord subDatum = convertTupleToAvro(subTuple, subStreamSchema, avroSchema);
			// Return the Avro record
			returnObject = subDatum;
			break;
		case LIST:
			@SuppressWarnings("unchecked")
			List<Object> subList = (List<Object>) tupleAttribute;
			// Obtain the type of the elements contained in the Streams list
			Type tupleElementType = ((CollectionType) tupleAttributeType).getElementType();
			// Obtain the type of the elements contained in the Avro array
			Schema avroArrayElementType = avroSchema.getElementType();
			// Now loop through all list elements and populate the associated
			// Avro array elements
			GenericArray<Object> subArray = new GenericData.Array<Object>(subList.size(), avroSchema);
			for (Object arrayElement : subList) {
				Object avroElement = convertAttributeToAvro(attributeName, arrayElement, tupleElementType,
						avroArrayElementType);
				subArray.add(avroElement);
			}
			// Return the Avro array
			returnObject = subArray;
			break;
		default:
			LOGGER.log(TraceLevel.WARN,
					"Ignoring attribute " + attributeName + " because of unsupported type " + metaType);
		}
		return returnObject;
	}

}
