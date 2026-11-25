FROM golang:1.26-alpine AS build
WORKDIR /src
# dependências primeiro (cache)
COPY meowcaller/go.mod meowcaller/go.sum ./meowcaller/
COPY app/go.mod app/go.sum ./app/
WORKDIR /src/app
RUN go mod download
# código
WORKDIR /src
COPY meowcaller/ ./meowcaller/
COPY app/ ./app/
WORKDIR /src/app
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata
ENV TZ=America/Sao_Paulo DATA_DIR=/data ADDR=:8090
COPY --from=build /out/server /server
VOLUME /data
EXPOSE 8090
ENTRYPOINT ["/server"]
