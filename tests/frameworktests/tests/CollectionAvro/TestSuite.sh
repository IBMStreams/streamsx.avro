# The common test suite for avro toolkit tests
import "$TTRO_scriptDir/streamsutils.sh"

PREPS=('checkJq')

checkJq() {
	if ! jq --version; then
		printError "Json tool jq not available! Install package jq"
		return 0
	fi
}