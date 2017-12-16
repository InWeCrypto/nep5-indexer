FROM golang:1.9.2-stretch

LABEL maintainer="support@inwecrypto.com"

COPY . /go/src/github.com/inwecrypto/nep5-indexer

RUN go install github.com/inwecrypto/nep5-indexer/cmd/nep5-indexer && rm -rf /go/src

VOLUME ["/etc/inwecrypto/indexer/nep5"]

WORKDIR /etc/inwecrypto/indexer/nep5

CMD ["/go/bin/nep5-indexer","--conf","/etc/inwecrypto/indexer/nep5/nep5.json"]