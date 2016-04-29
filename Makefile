.PHONY: libshout test run

UNAME := $(shell uname)

ifeq ($(UNAME), Linux)
#LIBSHOUT_DEPS = libshout/src/.libs/libshout.a
#GO_BUILD_OPTS = -ldflags "-linkmode external -extldflags -static"
LIBSHOUT_DEPS = libshout/src/.libs/libshout.o
GO_BUILD_OPTS =
LIB_ADD LD_LIBRARY_PATH=./libshout/src/.libs
endif

ifeq ($(UNAME), Darwin)
LIBSHOUT_DEPS = libshout/src/.libs/libshout.dylib
GO_BUILD_OPTS =
LIB_ADD = DYLD_LIBRARY_PATH=./libshout/src/.libs
endif

GOFILES := $(shell find . -name "*.go")

default: iceshift test

libshout/src/.libs/libshout.dylib:
	make libshout

libshout/src/.libs/libshout.o:
	make libshout

#libshout/src/.libs/libshout.a: libshout

libshout:
	cd libshout && ./configure --disable-speex --disable-static && make

iceshift: $(LIBSHOUT_DEPS) $(GOFILES)
	go build $(GO_BUILD_OPTS) ./cmd/iceshift

test:
	$(LIB_ADD) go test -v ./...

# For OS X
run: iceshift
	$(LIB_ADD) ./iceshift
