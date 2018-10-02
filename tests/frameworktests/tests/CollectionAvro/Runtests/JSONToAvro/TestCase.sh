
PREPS='copyOnly'

STEPS=(
	'splCompile'
	'submitJob'
	'checkJobNo'
	'waitForFinAndHealth'
	'cancelJobAndLog'
	'checkTuples'
	'linewisePatternMatchInterceptAndSuccess data/WindowMarker "true" "{seq_=100,typ_=\"w\",avroMessage=}"'
	'linewisePatternMatchInterceptAndSuccess data/FinalMarker ""  "{seq_=101,typ_=\"f\",avroMessage=}"'
)

FINS='cancelJobAndLog'

checkTuples() {
	local i
	for ((i=0; i<100; i++)); do
		linewisePatternMatchInterceptAndSuccess data/Tuples "true" "{seq_=$i,typ_=\"t\",avroMessage*"
	done
	return 0
}