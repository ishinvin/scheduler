FROM golang:1.23-alpine AS build

# Recreate the repo layout so "replace .." in _examples/go.mod resolves.
WORKDIR /src/scheduler
COPY go.mod go.sum *.go ./
COPY jobstore/ ./jobstore/

WORKDIR /src/scheduler/_examples
COPY _examples/go.mod _examples/go.sum ./
RUN go mod download
COPY _examples/ ./

RUN go build -o /app ./multi-instance/

FROM alpine:3.20
COPY --from=build /app /app
CMD ["/app"]
