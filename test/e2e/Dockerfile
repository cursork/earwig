FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /earwig .

FROM alpine:3.21
RUN apk add --no-cache bash
COPY --from=build /earwig /usr/local/bin/earwig
COPY test/e2e/dsl.sh  /test/dsl.sh
COPY test/e2e/test.sh /test/test.sh
RUN chmod +x /test/dsl.sh /test/test.sh
CMD ["/test/test.sh"]
