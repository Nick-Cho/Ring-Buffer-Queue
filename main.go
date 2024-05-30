package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Message struct {
	data []byte
}

type Server struct {
	consumer_offsets map[string]int
	buffer           []Message

	ln net.Listener
}

func NewServer() *Server {
	return &Server{
		consumer_offsets: make(map[string]int),
		buffer:           make([]Message, 0),
	}
}

func (s *Server) Start() error {
	return nil
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", ":9092")

	if err != nil {
		return err
	}

	s.ln = ln
	for {
		conn, err := ln.Accept()
		if err != nil {
			if err == io.EOF {
				return err
			}
			slog.Error("Error accepting connection:", "err", err)
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	fmt.Println("New connection: ", conn.RemoteAddr())
}

func consumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"myTopic", "fooTopic"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
func main() {
	server := NewServer()
	server.Listen()
}
