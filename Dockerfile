# ---- Build stage ----
FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/nt-demo .

# ---- Runtime stage ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 loader
COPY --from=build /out/nt-demo /usr/local/bin/nt-demo
USER loader
EXPOSE 8080 8081
ENTRYPOINT ["nt-demo"]