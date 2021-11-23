CC=g++
CFLAGS=-std=c++11 -g -Wall -pthread -I./
LDFLAGS= -lpthread -ltbb -lhiredis
SUBDIRS=core
SUBSRCS=$(wildcard core/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=synthetic

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

