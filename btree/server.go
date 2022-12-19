package btree

import (
	"fmt"
	"log"
	"net"
)

// Simple tcp server, which implements the following protocol:
// - first byte [uint8] in a stream is intepreted as a protocol version
// - then server recieves commands in form of messages from a client
// - messages are separated by '$' (may be escaped, '\$' is decoded into '$' and does not terminate a message)
// - message contains:
//   - 1 byte: command type [uint8] (0 - get, 1 - put)
//   - variable size: key [byte array]
//
// put-message also conatins:
//   - 1 byte separator ',' (may be escaped as '\,')
//   - variable size: value [byte array]
//
// messages of both types are terminated with '$'
type Server struct {
	Port    string
	Workers uint16

	tasks chan *net.Conn
}

type worker struct {
	wid    uint16
	logger log.Logger
	tasks  chan *net.Conn
}

func MakeServer() *Server {
	workers := uint16(16)
	tasks := make(chan *net.Conn, workers)
	server := Server{Port: "8080", Workers: workers, tasks: tasks}
	return &server
}

func makeWorker(wid uint16, tasks chan *net.Conn) *worker {
	logger := log.New(log.Default().Writer(), fmt.Sprintf("worker [%d]: ", wid), 0)
	w := worker{wid: wid, logger: *logger, tasks: tasks}
	return &w
}

func (s *Server) Serve() error {
	ln, err := net.Listen("tcp", ":"+s.Port)
	if err != nil {
		log.Fatalf("failed to listen on port [%s] with error [%v]\n", s.Port, err)
	}
	for i := uint16(0); i < s.Workers; i++ {
		w := makeWorker(i, s.tasks)
		go w.doWork()
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("failed to accept connection with error [%v]\n", err)
		}
		s.tasks <- &conn
	}
}

func (w *worker) handleConnection(conn *net.Conn) {
	defer (*conn).Close()
	var (
		buffer  = make([]byte, 0)
		chunk   = make([]byte, 64)
		msg     = nil
		decoder = makeDecoder()
	)

	for msg == nil {
		read, err := (*conn).Read(chunk)
		if err != nil {
			w.logger.Printf("failed to read with error [%v]", err)
			break
		}
		msg := decoder.consume(chunk)
	}

	w.logger.Printf("received data [%s]", buffer)
}

func (w *worker) doWork() {
	w.logger.Printf("started\n")
	for {
		conn := <-w.tasks
		if conn == nil {
			w.logger.Printf("finished\n")
			break
		}
		w.handleConnection(conn)
	}
}
