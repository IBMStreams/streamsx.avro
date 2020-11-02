# Copyright (C)2018, International Business Machines Corporation and  
# others. All Rights Reserved.                        
.PHONY: all clean

SPLC_FLAGS = -a
OUTPUT_DIR = ./output
ifeq ($(STREAMS_STUDIO_BUILDING), 1)
  # Building from Streams Studio, use env vars set by studio
  SPLC = $(STREAMS_STUDIO_SC_PATH)
  SPL_PATH = $(STREAMS_STUDIO_SPL_PATH)
  OUTPUT_DIR = $(STREAMS_STUDIO_OUTPUT_DIRECTORY)
else ifdef STREAMS_BUILDSERVICE
    $(info Building using buildservice)
    ifeq ($(STREAMS_INSTALL),)
        $(error error: environment variable STREAMS_INSTALL has to be set)
    endif
    SPLC = $(STREAMS_INSTALL)/bin/sc
    SPL_PATH = ../toolkits:$(STREAMS_INSTALL)/toolkits
else
  # Building outside of Streams Studio...
  ifeq ($(STREAMS_INSTALL),)
    $(error error: environment variable STREAMS_INSTALL has to be set)
  endif
  # Set the STREAMSX_AVRO_TOOLKIT variable if streamsx.avro sample or toolkit location changed in your environment.
  ifeq ($(STREAMSX_AVRO_TOOLKIT),)
    ifneq ($(realpath ../../com.ibm.streamsx.avro),)
      STREAMSX_AVRO_TOOLKIT = ../../com.ibm.streamsx.avro
    endif
    ifeq ($(STREAMSX_AVRO_TOOLKIT),)
      ifneq ($(realpath $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.avro),)
        STREAMSX_AVRO_TOOLKIT = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.avro
      endif
    endif
  endif
  SPLC = $(STREAMS_INSTALL)/bin/sc
  SPL_PATH = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json:$(STREAMSX_AVRO_TOOLKIT)
endif

SPLC_FLAGS += -t $(SPL_PATH)

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = com.ibm.streamsx.avro.sample::ReadAvroFileSample


all:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir $(OUTPUT_DIR) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)
	rm -rf doc

