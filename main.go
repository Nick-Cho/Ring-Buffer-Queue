package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
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

func main() {
	server := NewServer()
	server.Listen()
}
