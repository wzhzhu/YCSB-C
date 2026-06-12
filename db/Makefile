CC=g++
ROCKSDB_HOME ?= /users/wzhzhu/rocksdb
ENABLE_REDIS ?= 0
CFLAGS=-std=c++20 -c -O2 -DNDEBUG -g -Wall -fgnu-tm
INCLUDES=-I../ -I$(ROCKSDB_HOME) -I$(ROCKSDB_HOME)/include
SOURCES=$(filter-out redis_db.cc,$(wildcard *.cc))
ifeq ($(ENABLE_REDIS),1)
SOURCES += redis_db.cc
endif
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

