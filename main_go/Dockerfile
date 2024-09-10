FROM golang:1.22.3
WORKDIR "/opt/main_go"

COPY ["/main_go/go.mod", "/opt/main_go/go.mod"]
COPY ["/main_go/go.sum", "/opt/main_go/go.sum"]

RUN go mod download

COPY ["/main_go/main.go", "/opt/main_go/main.go"]
COPY ["/main_go/json_types.go", "/opt/main_go/json_types.go"]
COPY ["/main_go/utils.go", "/opt/main_go/utils.go"]

RUN go build .

ENTRYPOINT ["/opt/main_go/allezon_analytics"]
