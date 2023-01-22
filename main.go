package main

import (
	"flag"
	"log"

	"github.com/vladem/btree/btree"
	"github.com/vladem/btree/server"
	"github.com/vladem/btree/storage"
)

func main() {
	path := flag.String("path", "./db", "path to a file to persist data")
	flag.Parse()

	maxKeysCount := uint32(11)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: *path, MaxCellsCount: maxKeysCount}
	strg, err := storage.MakeNodeStorage(config)
	if err != nil {
		log.Fatalf("failed to create storage with error [%v]\n", err)
	}
	defer strg.Close()
	tree := btree.MakePagedBTree(strg, maxKeysCount)
	if tree == nil {
		log.Fatalf("failed to create a tree\n")
	}
	cfg := server.ServerConfig{
		Port:       "8080",
		Workers:    2,
		TelnetMode: true,
	}
	server, err := server.MakeServer(cfg, tree)
	if err != nil {
		log.Fatalf("failed to create server with error [%v]\n", err)
	}
	err = server.Serve(make(chan struct{}))
	if err != nil {
		log.Printf("serve finished with error [%v]\n", err)
	}
	log.Printf("finished\n")
}
