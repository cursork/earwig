FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /earwig .
RUN go build -o /earwig-gen ./test/e2e/gen

FROM alpine:3.21
RUN apk add --no-cache bash sqlite tmux
COPY --from=build /earwig /usr/local/bin/earwig
COPY --from=build /earwig-gen /usr/local/bin/earwig-gen
COPY test/e2e/dsl.sh      /test/dsl.sh
COPY test/e2e/test.sh     /test/test.sh
COPY test/e2e/tui_test.sh /test/tui_test.sh
COPY test/e2e/run-all.sh  /test/run-all.sh
RUN chmod +x /test/*.sh
CMD ["/test/run-all.sh"]
