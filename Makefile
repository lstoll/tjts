.PHONY: libshout test run

UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
LIBSHOUT_DEPS = libshout/src/.libs/libshout.a
GO_BUILD_OPTS = -ldflags "-linkmode external -extldflags -static"
endif
ifeq ($(UNAME), Darwin)
LIBSHOUT_DEPS = libshout/src/.libs/libshout.dylib
GO_BUILD_OPTS =
endif

default: iceshift test

libshout/src/.libs/libshout.dylib: libshout

libshout/src/.libs/libshout.a: libshout

libshout:
	cd libshout && ./configure --disable-speex && make

iceshift: $(LIBSHOUT_DEPS)
	go build $(GO_BUILD_OPTS) ./cmd/iceshift

test:
	go test -v ./...

# For OS X
run: iceshift
	DYLD_LIBRARY_PATH=./libshout/src/.libs ./iceshift
