package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/vladem/btree/btree"
)

type ServerConfig struct {
	Port       string
	Workers    uint16
	TelnetMode bool
}

// Simple tcp server, which implements the following protocol:
// - first byte [uint8] in a stream is intepreted as a protocol version
// - then server recieves commands in form of messages from a client
// - messages are separated by '$' (may be escaped, '\$' is decoded into '$' and does not terminate a message)
// - message contains:
//   - 1 byte: command type [uint8] ('g' - get, 'p' - put)
//   - variable size: key [byte array]
//
// put-message also conatins:
//   - 1 byte separator ',' (may be escaped as '\,')
//   - variable size: value [byte array]
//
// get produces a response:
// - 1 byte: success/fail [uint8] ('s' - success, 'f' - fail)
// - variable size: value [byte array]
//
// messages of all types are terminated with '$'
type Server struct {
	cfg      ServerConfig
	bTree    btree.IBTree
	listener *net.Listener
}

type worker struct {
	wid    uint16
	logger log.Logger
	server *Server
}

func MakeServer(cfg ServerConfig, bTree btree.IBTree) (*Server, error) {
	ln, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port [%s] with error [%v]", cfg.Port, err)
	}
	server := Server{cfg: cfg, bTree: bTree, listener: &ln}
	return &server, nil
}

func makeWorker(wid uint16, server *Server) *worker {
	logger := log.New(log.Default().Writer(), fmt.Sprintf("worker [%d]: ", wid), 0)
	w := worker{wid: wid, logger: *logger, server: server}
	return &w
}

func (s *Server) Serve(cancel chan struct{}) error {
	for i := uint16(0); i < s.cfg.Workers; i++ {
		w := makeWorker(i, s)
		go w.doWork()
	}
	<-cancel
	err := (*s.listener).Close()
	if err != nil {
		return fmt.Errorf("failed to close listener on port [%s] with error [%v]", s.cfg.Port, err)
	}
	return nil
}

func (w *worker) handleConnection(conn *net.Conn) {
	defer (*conn).Close()
	var (
		version = make([]byte, 1)
		chunk   = make([]byte, 64)
		decoder = makeDecoder(w.server.cfg.TelnetMode)
	)
	read, err := (*conn).Read(version)
	if err != nil || read != 1 {
		w.logger.Printf("failed to read version with error [%v], read [%d] bytes", err, read)
		return
	} else {
		w.logger.Printf("protocol version is %v", version[0])
	}
	for {
		read, err := (*conn).Read(chunk)
		if err != nil && !errors.Is(err, io.EOF) {
			w.logger.Printf("failed to read with error [%v]", err)
		}
		if err != nil {
			break
		}
		decoder.consume(chunk[:read])
		for decoder.hasNext() {
			next := decoder.next()
			w.logger.Printf("received msg, type: %d, data: %v", next.commandType, next.payloads)
			if next.commandType == commandTypeGet {
				getM, err := next.ToGetMessage()
				if err != nil {
					w.logger.Printf("invalid msg, type: %d, data: %v", next.commandType, next.payloads)
					continue
				}
				val := w.server.bTree.Get(getM.key)
				result := []byte{'f'}
				if val != nil {
					result[0] = 's'
					result = append(result, val...)
				}
				result = append(result, '$')
				(*conn).Write(result)
			} else if next.commandType == commandTypePut {
				putM, err := next.ToPutMessage()
				if err != nil {
					w.logger.Printf("invalid msg, type: %d, data: %v", next.commandType, next.payloads)
					continue
				}
				w.server.bTree.Put(putM.key, putM.value)
			} else {
				w.logger.Printf("invalid msg, type: %d, data: %v", next.commandType, next.payloads)
			}
		}
	}
	w.logger.Printf("connection [%s/%s] will be closed", (*conn).LocalAddr().String(), (*conn).RemoteAddr().String())
}

func (w *worker) doWork() {
	w.logger.Printf("started\n")
	for {
		conn, err := (*w.server.listener).Accept()
		if err != nil {
			log.Printf("failed to accept connection with error [%v]\n", err)
			break
		}
		w.handleConnection(&conn)
	}
}
