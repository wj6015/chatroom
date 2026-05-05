FROM golang:1.22-alpine AS builder

WORKDIR /app

RUN apk add --no-cache gcc musl-dev sqlite-dev

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o chatroom main.go

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates sqlite-libs

COPY --from=builder /app/chatroom /app/chatroom
COPY invite_codes.txt /app/invite_codes.txt

ENV PORT=8080
ENV DATABASE_PATH=/data/chat.db

EXPOSE 8080

CMD ["/app/chatroom"]
