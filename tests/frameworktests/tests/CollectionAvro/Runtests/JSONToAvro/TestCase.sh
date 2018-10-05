#--timeout=900

PREPS='copyOnly'

STEPS=(
	'splCompile'
	'submitJob'
	'checkJobNo'
	'waitForFinAndHealth'
	'cancelJobAndLog'
	'checkTuples'
	'linewisePatternMatchInterceptAndSuccess data/WindowMarker "true" "{seq_=199,typ_=\"w\",avroMessage=}"'
	'linewisePatternMatchInterceptAndSuccess data/FinalMarker ""  "{seq_=200,typ_=\"f\",avroMessage=}"'
	'checkContent'
)

FINS='cancelJobAndLog'

checkTuples() {
	local i j
	for ((i=0; i<100; i++)); do
		j=$((i*2))
		linewisePatternMatchInterceptAndSuccess data/Tuples "true" "{seq_=$j,typ_=\"t\",avroMessage*"
	done
	return 0
}

checkContent() {
	if ! jq -h &> /dev/zero; then
		printError "Json tool jq not available"
		return 0
	fi
	local i
	local reference actval
	for ((i=0; i<100; i++)); do
		java -jar "$TTRO_inputDir/../avro-tools-1.8.2.jar" 'tojson' "data/AvroFile_$i" > "data/CompFile_$i"
		reference="$(jq '.username' "data/JsonFile_$i")"
		actval="$(jq '.username' "data/CompFile_$i")"
		if [[ $actval == $reference ]]; then
			echo "Matching username $i: $actval"
		else
			setFailure "Difference in username file data/CompFile_$i"
			break
		fi
		reference="$(jq '.tweet' "data/JsonFile_$i")"
		actval="$(jq '.tweet' "data/CompFile_$i")"
		if [[ $actval == $reference ]]; then
			echo "Matching tweet $i: $actval"
		else
			setFailure "Difference in tweet file data/CompFile_$i"
			break
		fi
		reference="$(jq '.tweettime' "data/JsonFile_$i")"
		actval="$(jq '.tweettime' "data/CompFile_$i")"
		if [[ $actval == $reference ]]; then
			echo "Matching tweettime $i: $actval"
		else
			setFailure "Difference in tweettime file data/CompFile_$i"
			break
		fi
	done
	return 0
}