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
	local tuplecount=$(grep 'typ_="t",' data/Tuples | wc -l | cut -f1 -d' ')
	local windowcount=$(grep 'typ_="w",' data/Tuples | wc -l | cut -f1 -d' ')
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
		#remove seq number and window marker entries
		{
			while read -r; do
				if [[ $REPLY == *,typ_=\"t\",* ]]; then
					local lin="${REPLY#*,typ_=\"t\",}"
					echo "$lin" >> data/TuplesOnly
				fi
			done
		} < data/Tuples
		{
			while read -r; do
				if [[ $REPLY == *,typ_=\"t\",* ]]; then
					local lin="${REPLY#*,typ_=\"t\",}"
					echo "$lin" >> data/TuplesOnlyReference
				fi
			done
		} < data/TuplesReference
		echoExecuteInterceptAndSuccess diff data/TuplesOnly data/TuplesOnlyReference;;
	esac
}
