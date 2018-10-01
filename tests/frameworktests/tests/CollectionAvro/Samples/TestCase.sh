#--variantList=$(\
#--for x in $TTRO_streamsxAvroSamplesPath/*; \
#--	do if [[ -f $x/Makefile ]]; then \
#--		echo -n "${x#$TTRO_streamsxAvroSamplesPath/} "; \
#--	fi; \
#--	done\
#--)

setCategory 'quick'

PREPS=( 
	'copyAndMorph "$TTRO_streamsxAvroSamplesPath/$TTRO_variantCase" "$TTRO_workDirCase" ""'
	'export STREAMSX_AVRO_TOOLKIT="$TTPR_streamsxAvroToolkit"'
	'export SPL_CMD_ARGS="-j $TTRO_treads"'
)
STEPS=( 'echoExecuteInterceptAndSuccess make' )

