#--variantList='AvroToJSON JSONToAvro TupleToAvro'

PREPS='copyAndMorphSpl'
STEPS=(
	'splCompileInterceptAndError'
	'myEval'
)

myEval() {
	case "$TTRO_variantCase" in
	AvroToJSON)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" 'ERROR: AvroToJSON operator cannot be used inside a consistent region';;
	JSONToAvro)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" 'ERROR: JSONToAvro operator cannot be used inside a consistent region';;
	TupleToAvro)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" 'ERROR: TupleToAvro operator cannot be used inside a consistent region';;
	*)
		printErrorAndExit "Wrong case variant" $errRt
	esac
}