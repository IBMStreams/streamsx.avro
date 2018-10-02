#--variantList='embedAvroSchema_true embedAvroSchema_false'

PREPS='copyAndMorphSpl'

STEPS=(
	'splCompile'
	'executeLogAndError output/bin/standalone'
	'checkOutput'
)

checkOutput() {
	case "$TTRO_variantCase" in
	embedAvroSchema_true)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" "*ERROR*If Avro schema is embedded in the output, you must specify one of the thresholds when the tuple must be submitted (submitOnPunct, bytesPerMessage, timePerMessage, tuplesPerMessage).*";;
	embedAvroSchema_false)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" "*ERROR*Parameter submitOnPunct can only be set to true if Avro schema is embedded in the output.";;
	*)
		printErrorAndExit "Wron variant $TTRO_variantCase" $errRt;;
	esac
}