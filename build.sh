#!/bin/bash

set -eof

mkdir -p build/env
test -f build/golang.tar.gz || wget -O build/golang.tar.gz https://golang.org/dl/go1.15.linux-amd64.tar.gz
test -d build/go || tar xf build/golang.tar.gz -C build

VERSION=$(cat VERSION)

for distribution in bionic xenial trusty stretch buster focal; do
    if [ ! -z "$1" ] && [ "$1" != "$distribution" ]; then
        continue
    fi
    
    # we use build/env to provide an empty context for docker
    docker build -t rrd2whisper_build:$distribution -f dockerfiles/Dockerfile.$distribution build/env

    docker run --rm -t -i -v $(pwd):/build/ -v $(pwd)/build/go:/usr/local/go rrd2whisper_build:$distribution /usr/local/go/bin/go build -ldflags "-X main.Version=$VERSION"
    mv rrd2whisper build/rrd2whisper_${VERSION}_${distribution}_amd64
done
