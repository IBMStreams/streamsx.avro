#--variantList='AvroToJSON JSONToAvro TupleToAvro'

PREPS='copyAndMorphSpl'
STEPS=(
	'splCompileInterceptAndError'
	'myEval'
)

myEval() {
	case "$TTRO_variantCase" in
	AvroToJSON)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3450E*';;
	JSONToAvro)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3450E*';;
	TupleToAvro)
		linewisePatternMatchInterceptAndSuccess "$TT_evaluationFile" "true" '*CDIST3450E*';;
	*)
		printErrorAndExit "Wrong case variant" $errRt
	esac
}
