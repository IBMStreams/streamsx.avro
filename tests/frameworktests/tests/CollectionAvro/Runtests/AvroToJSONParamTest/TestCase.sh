#--variantList='embedAvroSchema_false inputNoAvroMessage inputNoBlob outputAttrNotExists outputAttrNoString'

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
		echo "No schema embedded in Avro stream but schema is expected in AvroToJSON -> exeption but no error";;
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
	if [[ $TTRO_variantCase == embedAvroSchema_false ]]; then
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
	embedAvroSchema_false)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*org.apache.avro.InvalidAvroMagicException: Not an Avro data file*';;
	inputNoAvroMessage)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*avroMessage*not exist*';;
	inputNoBlob)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*Conversion between*Blob*not supported*';;
	outputAttrNotExists)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*CDIST3453E*';;
	outputAttrNoString)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" '' '*CDIST3455E*';;
	*)
		printErrorAndExit "Wrong variant $TTRO_variantCase" $errRt;;
	esac
}
