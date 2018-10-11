#--variantList='jsonInputNoJsonMessage jsonInputTypeWrong tupleInputTypeWrong jsonOutputAttrNotExists tupleOutputAttrNotExists jsonOutputAttrNoBlob tupleOutputAttrNoBlob'

PREPS='copyAndMorphSpl'

STEPS=(
	'splCompile'
	'executeLogAndError output/bin/standalone'
	'checkOutput'
)

case "$TTRO_variantCase" in
	jsonInputNoJsonMessage)
		StreamsOperator='JSONToAvro';  InputStream='GenerateTweet';;
	jsonInputTypeWrong)
		StreamsOperator='JSONToAvro';  InputStream='GenerateTweet';;
	tupleInputTypeWrong)
		StreamsOperator='TupleToAvro'; InputStream='GenerateTweet';;
	jsonOutputAttrNotExists)
		StreamsOperator='JSONToAvro';  InputStream='ConvertTupleToJson';;
	tupleOutputAttrNotExists)
		StreamsOperator='TupleToAvro'; InputStream='GenerateTweet';;
	jsonOutputAttrNoBlob)
		StreamsOperator='JSONToAvro';  InputStream='ConvertTupleToJson';;
	tupleOutputAttrNoBlob)
		StreamsOperator='TupleToAvro'; InputStream='GenerateTweet';;
esac

checkOutput() {
	case "$TTRO_variantCase" in
	jsonInputNoJsonMessage)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*ERROR*java.lang.IllegalArgumentException: No inputJsonMessage attribute `jsonMessage` found in input stream.*';;
	jsonInputTypeWrong)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*ERROR*java.lang.IllegalArgumentException: inputJsonMessage attribute `tweettime` must have a rstring or ustring type.*';;
	tupleInputTypeWrong)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*ERROR*Type UINT64:uint64 of attribute tweettime is not supported.*';;
	*OutputAttrNotExists)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*ERROR*No outputAvroMessage attribute `myAvroMessage` found in output stream.*';;
	*OutputAttrNoBlob)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*ERROR*outputAvroMessage attribute `avroMessage` must have a blob type.*';;
	*)
		printErrorAndExit "Wrong variant $TTRO_variantCase" $errRt;;
	esac
}