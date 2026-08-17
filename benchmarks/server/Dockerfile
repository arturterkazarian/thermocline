FROM golang:1.23-alpine AS build
WORKDIR /src
COPY main.go .
RUN go mod init benchsrv && go build -o /benchsrv main.go

FROM alpine:3.20
COPY --from=build /benchsrv /benchsrv
EXPOSE 8077
ENTRYPOINT ["/benchsrv"]
