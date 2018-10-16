SOURCES := $(shell find . -name '*.go' -not -name '*_test.go')
SOURCES_NOV := $(shell find . -path './vendor*' -prune -o -name '*.go' -print)

all: bin/ingen

fmt: $(SOURCES_NOV)
	@goimports -w $^

ingen: bin/ingen

bin/ingen: $(SOURCES)
	go build -o bin/ingen ./cmd/ingen

clean: $(SUBDIRS)
	rm -rf bin

.PHONY: all clean ingen $(SUBDIRS) fmt
