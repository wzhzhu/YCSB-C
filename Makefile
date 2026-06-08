CC=g++
ROCKSDB_HOME ?= /home/gpu/wzhzhu/rocksdb
ENABLE_REDIS ?= 0
CFLAGS=-std=c++20 -g -Wall -pthread -I./ -I$(ROCKSDB_HOME) -I$(ROCKSDB_HOME)/include -DYCSBC_ENABLE_REDIS=$(ENABLE_REDIS)
LDFLAGS= -L$(ROCKSDB_HOME)/build -Wl,-rpath,$(ROCKSDB_HOME)/build -lpthread -ltbb -lrocksdb
ifeq ($(ENABLE_REDIS),1)
LDFLAGS += -lhiredis
endif
SUBDIRS=core db
SUBSRCS=$(wildcard core/*.cc) $(filter-out db/redis_db.cc,$(wildcard db/*.cc))
ifeq ($(ENABLE_REDIS),1)
SUBSRCS += db/redis_db.cc
endif
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

