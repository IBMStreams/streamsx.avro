#--variantList='JsonEmbedAvroSchema_true JsonEmbedAvroSchema_false TupleEmbedAvroSchema_true TupleEmbedAvroSchema_false'

PREPS='copyAndMorphSpl'

STEPS=(
	'splCompile'
	'executeLogAndError output/bin/standalone'
	'checkOutput'
)

checkOutput() {
	case "$TTRO_variantCase" in
	*EmbedAvroSchema_true)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" "*ERROR*If Avro schema is embedded in the output, you must specify one of the thresholds when the tuple must be submitted (submitOnPunct, bytesPerMessage, timePerMessage, tuplesPerMessage).*";;
	*EmbedAvroSchema_false)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" "*ERROR*Parameter submitOnPunct can only be set to true if Avro schema is embedded in the output.";;
	*)
		printErrorAndExit "Wrong variant $TTRO_variantCase" $errRt;;
	esac
}