package btree_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vladem/btree/btree"
)

func createServer(t *testing.T, port string) chan struct{} {
	cfg := btree.ServerConfig{
		Port:       port,
		Workers:    2,
		TelnetMode: true,
	}
	server, err := btree.MakeServer(cfg, btree.MakeDummyBTreeT())
	if err != nil {
		t.Fatalf("failed to create server with error [%v]\n", err)
	}
	cancel := make(chan struct{})
	go server.Serve(cancel)
	return cancel
}

func writeAndCheck(t *testing.T, conn *net.Conn, data []byte) {
	n, err := (*conn).Write(data)
	if err != nil {
		t.Fatalf("failed to write with error [%v]\n", err)
	}
	assert.Equal(t, n, len(data))
}

func TestGetExisting(t *testing.T) {
	port := "8080"
	cancel := createServer(t, port)
	conn, err := net.Dial("tcp", "localhost:"+port)
	if err != nil {
		t.Fatalf("failed to connect to the server with error [%v]\n", err)
	}
	writeAndCheck(t, &conn, []byte{1 /* version */, 'p' /* put */, 'a' /* key */, ',', 'b' /* value */, '$'})
	writeAndCheck(t, &conn, []byte{'g' /* get */, 'a' /* key */, '$'})
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read with error [%v]\n", err)
	}
	assert.Equal(t, n, 3)
	assert.Equal(t, []byte{'s', 'b', '$'}, buf[0:3])
	cancel <- struct{}{}
}
