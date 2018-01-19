package com.ibm.streamsx.avro.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ibm.streams.operator.Type.MetaType;

public class SupportedTypes {

	public static final List<MetaType> SUPPORTED_STREAMS_TYPES = new ArrayList<MetaType>(
			Arrays.asList(MetaType.BOOLEAN, MetaType.FLOAT32, MetaType.FLOAT64, MetaType.INT32, MetaType.INT64,
					MetaType.RSTRING, MetaType.USTRING, MetaType.TUPLE, MetaType.LIST));

}
