FROM golang:1.23 AS builder

WORKDIR /app
COPY . .

RUN make staticbuild

FROM alpine
COPY --from=builder /app/sstloader /app/sstloader

ENTRYPOINT ["/app/sstloader"]
