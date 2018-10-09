#--variantList='embedAvroSchema_false embedAvroSchema_false2 embedAvroSchema_true inputNoAvroMessage inputNoBlob outputAttrNotExists outputAttrNoString'

PREPS=(
	'myExplain'
	'copyAndMorphSpl'
	'setVar TT_mainComposite AvroJSONSample'
)

STEPS=(
	'splCompile'
	'executeAndLog output/bin/standalone'
	'myEval'
)

myExplain() {
	case "$TTRO_variantCase" in
	embedAvroSchema_false)
		echo "No schema embedded in Avro stream and no schema expected in stream -> exeption but no error";;
	embedAvroSchema_false2)
		echo "Schema embedded in Avro stream but no schema expected in stream -> exeption  but no error";;
	embedAvroSchema_true)
		echo "Schema embedded in Avro stream, schema expected and schema file specified -> IllegalArgumentException";;
	inputNoAvroMessage)
		echo "Input is a tuple with no avroMessage attribute";;
	inputNoBlob)
		echo "Input attribute avroMessage is not a blob attribute";;
	outputAttrNotExists)
		echo "Output outputJsonMessage attribute not exists";;
	outputAttrNoString)
		echo "Output outputJsonMessage attribute is no string type";;
	esac
}

myEval() {
	if [[ $TTRO_variantCase == embedAvroSchema_false* ]]; then
		if [[ $TTTT_result -ne 0 ]]; then
			setFailure "Unexpected result $TTTT_result"
			return 0
		fi
	else
		if [[ $TTTT_result -eq 0 ]]; then
			setFailure "Unexpected result 0"
			return 0
		fi
	fi
	printInfo "Correct result $TTTT_result returned from standalone"
	
	case "$TTRO_variantCase" in
	embedAvroSchema_false*)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*NullPointerException*';;
	embedAvroSchema_true)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*java.lang.IllegalArgumentException: Parameter avroMessageSchema cannot be specified if the schema is embedded in the message.*';;
	inputNoAvroMessage)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*avroMessage*not exist*';;
	inputNoBlob)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*Conversion between*Blob*not supported*';;
	outputAttrNotExists)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*java.lang.IllegalArgumentException: No outputJsonMessage attribute `xx` found in output stream*';;
	outputAttrNoString)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*java.lang.IllegalArgumentException: outputJsonMessage attribute `x` must have a rstring or ustring type*';;
	*)
		printErrorAndExit "Wrong variant $TTRO_variantCase" $errRt;;
	esac
}