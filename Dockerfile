FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /raftkv ./cmd/raftkv

FROM alpine
COPY --from=builder /raftkv /raftkv

ENV WALPATH="/store"
ENV PORT=50051
EXPOSE 50051

CMD ["/raftkv"]
