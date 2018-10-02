#--variantList='embedAvroSchema_false submitOnPunct tuplesPerMessage timePerMessage bytesPerMessage'

PREPS='copyAndMorphSpl'

STEPS=(
	"splCompile"
	'submitJob'
	'checkJobNo'
	'waitForFinAndHealth'
	'cancelJobAndLog'
	'checkOutput'
)

FINS='cancelJobAndLog'

checkOutput() {
	local windowMarkerFile='data/Tuples'
	case "$TTRO_variantCase" in
	timePerMessage|bytesPerMessage)
		windowMarkerFile='data/WindowMarker';;
	esac
	local tuplecount=$(grep 'typ_="t",' data/Tuples | wc -l | cut -f1 -d' ')
	local windowcount=$(grep 'typ_="w",' "$windowMarkerFile" | wc -l | cut -f1 -d' ')
	printInfo "Result contains $tuplecount tuples and $windowcount windowMarker"
	case "$TTRO_variantCase" in
	embedAvroSchema_false|submitOnPunct)
		if [[ ( $tuplecount -ne 100 ) || ( $windowcount -ne 1 ) ]]; then
			setFailure "Wrong counts not 100 and 1"
		fi
		echoExecuteInterceptAndSuccess diff data/Tuples data/TuplesReference;;
	tuplesPerMessage)
		if [[ ( $tuplecount -ne 100 ) || ( $windowcount -ne 10 ) ]]; then
			setFailure "Wrong counts not 100 and 10"
		fi
		echoExecuteInterceptAndSuccess diff data/Tuples data/TuplesReference;;
	*)
		if [[ $tuplecount -ne 100 ]]; then
			setFailure "Wrong tuple counts $tuplecount"
		fi
		echoExecuteInterceptAndSuccess diff data/TuplesOnly data/TuplesReference;;
	esac
}
