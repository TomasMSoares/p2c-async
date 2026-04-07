##
# csv-read and parquet-generator
#
# @file
# @version 0.1
COMPILER = g++
INC_DIR = ../include/provided
FLAGS = -Wall -std=c++20 -pthread -mavx2 -I$(INC_DIR) 
DEBUG_FLAGS = -g -fno-omit-frame-pointer -fsanitize=address -O0
RELEASE_FLAGS = -O3

# Arrow/Parquet libraries
ARROW_FLAGS = $(shell pkg-config --cflags arrow parquet)
ARROW_LIBS = $(shell pkg-config --libs arrow parquet)

all: all.out parquet-generator

all.out: all.cpp
ifeq ($(target),debug)
	${COMPILER} $< ${FLAGS} ${DEBUG_FLAGS} -o $@
else
	${COMPILER} $< ${FLAGS} ${RELEASE_FLAGS} -o $@
endif

parquet-generator: parquet-generator.cpp
ifeq ($(target),debug)
	${COMPILER} $< ${FLAGS} ${DEBUG_FLAGS} ${ARROW_FLAGS} ${ARROW_LIBS} -o $@
else
	${COMPILER} $< ${FLAGS} ${RELEASE_FLAGS} ${ARROW_FLAGS} ${ARROW_LIBS} -o $@
endif

clean:
	rm -f *.out parquet-generator

.PHONY: clean all

# end
