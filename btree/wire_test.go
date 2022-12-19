package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateMessageSimpleGet(t *testing.T) {
	buf := []byte{0, 'k', 'e', 'y'}
	msg := createMessage(buf)
	if msg == nil {
		t.Fatal("msg is nil")
	}
	if msg.commandType != 0 {
		t.Fatalf("wrong commandType: %v", msg.commandType)
	}
	if len(msg.payloads) != 1 {
		t.Fatalf("wrong msg.payloads: %v", msg.payloads)
	}
	assert.Equal(t, []byte{'k', 'e', 'y'}, msg.payloads[0])
}

func TestCreateMessageSimplePut(t *testing.T) {
	buf := []byte{1, 'k', 'e', 'y', ',', 'v', 'a', 'l', 'u', 'e'}
	msg := createMessage(buf)
	if msg == nil {
		t.Fatal("msg is nil")
	}
	if msg.commandType != 1 {
		t.Fatalf("wrong commandType: %v", msg.commandType)
	}
	if len(msg.payloads) != 2 {
		t.Fatalf("wrong msg.payloads: %v", msg.payloads)
	}
	assert.Equal(t, []byte{'k', 'e', 'y'}, msg.payloads[0])
	assert.Equal(t, []byte{'v', 'a', 'l', 'u', 'e'}, msg.payloads[1])
}

func TestCreateMessageEscapedGet(t *testing.T) {
	buf := []byte{0, 'k', '\\', '\\', 'e', '\\', ',', 'y', '\\', '\\', '\\', '\\'}
	msg := createMessage(buf)
	if msg == nil {
		t.Fatal("msg is nil")
	}
	if msg.commandType != 0 {
		t.Fatalf("wrong commandType: %v", msg.commandType)
	}
	if len(msg.payloads) != 1 {
		t.Fatalf("wrong msg.payloads: %v", msg.payloads)
	}
	assert.Equal(t, []byte{'k', '\\', 'e', ',', 'y', '\\', '\\'}, msg.payloads[0])
}

func TestCreateMessageEscapedPut(t *testing.T) {
	buf := []byte{1, 'k', '\\', '\\', 'e', '\\', ',', 'y', '\\', '\\', '\\', '\\', ',', '\\', '\\', 'v', 'a', 'l'}
	msg := createMessage(buf)
	if msg == nil {
		t.Fatal("msg is nil")
	}
	if msg.commandType != 1 {
		t.Fatalf("wrong commandType: %v", msg.commandType)
	}
	if len(msg.payloads) != 2 {
		t.Fatalf("wrong msg.payloads: %v", msg.payloads)
	}
	assert.Equal(t, []byte{'k', '\\', 'e', ',', 'y', '\\', '\\'}, msg.payloads[0])
	assert.Equal(t, []byte{'\\', 'v', 'a', 'l'}, msg.payloads[1])
}
