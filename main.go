package main

import (
	"log"

	"github.com/vladem/btree/btree"
)

func main() {
	cfg := btree.ServerConfig{
		Port:       "8080",
		Workers:    2,
		TelnetMode: true,
	}
	server, err := btree.MakeServer(cfg, btree.MakeDummyBTreeT())
	if err != nil {
		log.Fatalf("failed to create server with error [%v]\n", err)
	}
	err = server.Serve(make(chan struct{}))
	if err != nil {
		log.Printf("serve finished with error [%v]\n", err)
	}
	log.Printf("finished\n")
}
