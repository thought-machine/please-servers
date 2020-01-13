FROM golang:1.13.6-alpine

# gcc is needed until https://github.com/thought-machine/please/issues/797 is fixed
RUN apk update && apk add musl-dev bash git curl libc6-compat python3 patch gcc
