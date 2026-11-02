# demesne -- one binary, no dependencies, so this is the whole build.
#
# The constraint is inherited and deliberate: stdlib only means no module cache and no
# network at build time, which matters on a box where the interesting failure mode is a
# machine that has swapped itself to death and cannot fetch anything.

BIN    := demesne
PREFIX ?= $(HOME)/bin
GO     ?= go

.PHONY: all build vet test install uninstall clean

all: build vet test

build:
	$(GO) build -o $(BIN) .

vet:
	$(GO) vet ./...

# -race, because the slot lifecycle is three kinds of concurrency at once: a supervisor
# per resident engine, an idle timer per on-demand one, and a proxy per request.
test:
	$(GO) test -race ./...

install: build
	install -Dm755 $(BIN) $(PREFIX)/$(BIN)
	# `demense` is the common misspelling. It is a symlink, not a second name for
	# anything: one binary, one behaviour, two spellings.
	ln -sfn $(BIN) $(PREFIX)/demense

uninstall:
	rm -f $(PREFIX)/$(BIN) $(PREFIX)/demense

clean:
	rm -f $(BIN) demense
