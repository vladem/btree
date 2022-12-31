# B-tree

## Description

Implementation of btree inspired by [the book](https://www.databass.dev/) and an interface for reading/writing data to it.

## Commands

```bash
go test -v ./...       # run all tests in the project
go run main.go         # build and run server
telnet localhost 8080  # open a tcp connection to the server
> 1paaa,bbb$           # [telnet prompt] put value 'bbb' with key 'aaa' 
> gaaa$                # [telnet prompt] get value of the key 'aaa' 
< sbbb$                # [telnet prompt] see data you've just entered (first symbol marks success/failure of the operation)
```

## Structure

Repo consists of several packages: 
1. server (tcp server and a wire protocl implementations)
1. btree (btree's interface and traversal implementation)
1. storage (implementation of slotted pages used to store btree nodes and )
1. util

Dependency chain:
server -> btree -> storage
               `-> util

## Diagrams

1. [file layout](https://drive.google.com/file/d/1wmpuofQr0EiAAsHpGlJimK-cK2M-64g0/view)

## FAQ

### What is btree?

In a nutshell, this is a datastructure used to store data on disk. Seems that, postgres implements it to store and retrive data associated with a primary key. 

### What is the main idea behind it?

The same as for binary search tree, lookup is performed by triversing the tree. Although, there is an important distinction: nodes of a btree generally have a greater number of ancestors and this number is variable for each node.   

### What do multiple ancestors give us?

This design is better suited for disk as data access require less separate disk io-operations. That means that more data can be read within one disk access, and this data will be immediately useful for tree triversal. 

### Ok, got it about reads, but what about insertions and deletions?

<TBD>: rebalancing algorithm and comparison to classical binary tree rotation technique.

## Resources

1. [postgres page layout](https://www.postgresql.org/docs/current/storage-page-layout.html)

