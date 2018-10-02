#--variantList='embedAvroSchema_false submitOnPunct tuplesPerMessage timePerMessage'

if [[ $TTRO_variantCase == embedAvroSchema_false ]]; then
	setCategory 'quick'
fi

PREPS='copyAndMorphSpl'

STEPS=(
	'splCompile'
	'submitJob'
	'checkJobNo'
	'waitForFinAndHealth'
	'cancelJobAndLog'
	'checkTuples'
	'checkWindowMarker'
	'checkFinalMarker'
)

FINS='cancelJobAndLog'

checkTuples() {
	case "$TTRO_variantCase" in
	timePerMessage)
		local count=$(wc -l data/Tuples | cut -f1 -d' ')
		if [[ $count -ne 100 ]]; then
			setFailure "Number of received tuples in ne 100. Count is: $count"
		fi;;
	tuplesPerMessage)
		echoExecuteInterceptAndSuccess diff data/Tuples data/TuplesExpected_tuplesPerMessage;;
	*)
		echoExecuteInterceptAndSuccess diff data/Tuples data/TuplesExpected;;
	esac
}

checkWindowMarker() {
	case "$TTRO_variantCase" in
	timePerMessage)
		;;
	tuplesPerMessage)
		echoExecuteInterceptAndSuccess diff data/WindowMarker data/WindowMarkerExpected_tuplesPerMessage;;
	*)
		linewisePatternMatchInterceptAndSuccess data/WindowMarker '' '{seq_=100,typ_="w",jsonMessage=""}';;
	esac
}

checkFinalMarker() {
	case "$TTRO_variantCase" in
	timePerMessage)
		;;
	tuplesPerMessage)
		linewisePatternMatchInterceptAndSuccess data/FinalMarker ''  '{seq_=110,typ_="f",jsonMessage=""}';;
	*)
		linewisePatternMatchInterceptAndSuccess data/FinalMarker ''  '{seq_=101,typ_="f",jsonMessage=""}';;
	esac
}