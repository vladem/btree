package main

import (
	"log"

	"github.com/vladem/btree/btree"
)

func main() {
	server := btree.MakeServer()
	err := server.Serve()
	if err != nil {
		log.Printf("serve finished with error [%v]\n", err)
	}
	log.Printf("finished\n")
}
