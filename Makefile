SOURCES := $(shell find . -name '*.go' -not -name '*_test.go')

all: Gopkg.lock bin/ingen

bin/ingen: $(SOURCES)
	go build -i -o bin/ingen ./cmd/ingen

Gopkg.lock: Gopkg.toml
	dep ensure -v

update:
	dep ensure -v -update

clean: $(SUBDIRS)
	rm -rf bin

.PHONY: all clean $(SUBDIRS) update
