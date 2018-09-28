#samples path
setVar 'TTRO_streamsxAvroSamplesPath' "$TTRO_inputDir/../../../samples"

#toolkit path
setVar 'TTPR_streamsxAvroToolkit' "$TTRO_inputDir/../../../com.ibm.streamsx.avro"

#Some sample need json toolkit to compile
setVar 'TTPR_streamsxJsonToolkit' "$STREAMS_INSTALL/toolkits/com.ibm.streamsx.json"

setVar 'TT_toolkitPath' "${TTPR_streamsxAvroToolkit}:${TTPR_streamsxJsonToolkit}" #consider more than one tk...
