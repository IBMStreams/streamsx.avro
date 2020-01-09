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
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3454E*';;
	jsonInputTypeWrong)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3455E*';;
	tupleInputTypeWrong)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3452E*';;
	*OutputAttrNotExists)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3453E*';;
	*OutputAttrNoBlob)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3455E*';;
	*)
		printErrorAndExit "Wrong variant $TTRO_variantCase" $errRt;;
	esac
}
