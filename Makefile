UNAME := $(shell uname)

ifeq ($(UNAME), Linux)
LIBSHOUT_DEPS = libshout/src/.libs/libshout.a
GO_BUILD_OPTS = -ldflags "-linkmode external -extldflags -static"
endif
ifeq ($(UNAME), Darwin)
LIBSHOUT_DEPS = libshout/src/.libs/libshout.dylib
GO_BUILD_OPTS =
endif

default: build test

libshout/src/.libs/libshout.dylib:
	cd libshout && ./configure && make

build: $(LIBSHOUT_DEPS)
	go build $(GO_BUILD_OPTS) ./cmd/iceshift

test:
	go test -v ./...

# For OS X
run: build
	DYLD_LIBRARY_PATH=./libshout/src/.libs ./iceshift
