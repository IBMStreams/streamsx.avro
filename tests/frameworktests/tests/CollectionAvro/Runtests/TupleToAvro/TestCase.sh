setCategory 'quick'

PREPS='copyOnly'

STEPS=(
	"splCompile"
	'submitJob'
	'checkJobNo'
	'waitForFinAndHealth'
	'cancelJobAndLog'
	'linewisePatternMatchInterceptAndSuccess data/Tuples "true" "{seq_=0,typ_=\"t\",avroMessage*" "{seq_=2,typ_=\"t\",avroMessage*" "{seq_=4,typ_=\"t\",avroMessage*" "{seq_=6,typ_=\"t\",avroMessage*" "{seq_=8,typ_=\"t\",avroMessage*"'
	'linewisePatternMatchInterceptAndSuccess data/WindowMarker "true" "{seq_=1,typ_=\"w\",avroMessage=}" "{seq_=3,typ_=\"w\",avroMessage=}" "{seq_=5,typ_=\"w\",avroMessage=}" "{seq_=7,typ_=\"w\",avroMessage=}" "{seq_=9,typ_=\"w\",avroMessage=}"'
	'linewisePatternMatchInterceptAndSuccess data/FinalMarker ""  "{seq_=10,typ_=\"f\",avroMessage=}"'
)

FINS='cancelJobAndLog'
