FROM golang:1.22.3
WORKDIR "/opt/main_go"

COPY ["/main_go/go.mod", "/opt/main_go/go.mod"]
COPY ["/main_go/go.sum", "/opt/main_go/go.sum"]

RUN go mod download

COPY ["/main_go/main.go", "/opt/main_go/main.go"]

RUN go build -o main

ENTRYPOINT ["/opt/main_go/main"]
