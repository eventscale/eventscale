FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /eventscale cmd/main.go 

FROM alpine:latest
RUN apk add --update ca-certificates && mkdir -p /eventscale/conf

COPY docker/config.yaml /eventscale/conf/config.yaml
COPY docker/nats.conf /eventscale/conf/nats.conf

COPY --from=builder /eventscale /bin/eventscale

EXPOSE 4222

ENTRYPOINT ["/bin/eventscale"]