setCategory 'quick'

PREPS='copyOnly'

STEPS=(
	"splCompile"
	'submitJob'
	'checkJobNo'
	'waitForFinAndHealth'
	'cancelJobAndLog'
	'echoExecuteInterceptAndSuccess diff data/Tuples data/TuplesExpected'
	'linewisePatternMatchInterceptAndSuccess data/WindowMarker "" "{seq_=2,typ_=\"w\",jsonMessage=\"\"}"'
	'linewisePatternMatchInterceptAndSuccess data/FinalMarker ""  "{seq_=3,typ_=\"f\",jsonMessage=\"\"}"'
)

FINS='cancelJobAndLog'
