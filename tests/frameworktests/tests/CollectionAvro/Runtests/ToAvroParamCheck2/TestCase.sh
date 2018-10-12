#--variantList='JsonEmbedAvroSchema_true \
#--       JsonEmbedAvroSchema_false0 JsonEmbedAvroSchema_false1 JsonEmbedAvroSchema_false2 JsonEmbedAvroSchema_false3 \
#--       TupleEmbedAvroSchema_true \
#--       TupleEmbedAvroSchema_false0 TupleEmbedAvroSchema_false1 TupleEmbedAvroSchema_false2 TupleEmbedAvroSchema_false3'

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
	*EmbedAvroSchema_false*)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" "*ERROR*Parameters submitOnPunct, tuplesPerMessage, bytesPerMessage or timePerMessage can only be set if Avro schema is embedded in the output.";;
	*)
		printErrorAndExit "Wrong variant $TTRO_variantCase" $errRt;;
	esac
}