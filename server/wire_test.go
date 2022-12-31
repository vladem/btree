package server

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

func TestConsumeSimple(t *testing.T) {
	var (
		decoder = makeDecoder(false)
		buf     = []byte{0, 'k', 'e', 'y', '$'}
	)
	decoder.consume(buf)
	assert.True(t, decoder.hasNext())
	next := decoder.next()
	assert.Equal(t, []byte{'k', 'e', 'y'}, next.payloads[0])
}

func TestConsumeManyInOneChunk(t *testing.T) {
	var (
		decoder = makeDecoder(false)
		buf     = []byte{0, 'k', 'e', 'y', '$', 1, 'k', 'e', 'y', ',', 'v', 'a', 'l', '$'}
	)
	decoder.consume(buf)
	assert.True(t, decoder.hasNext())
	next := decoder.next()
	expected1 := message{commandType: 0}
	expected1.payloads = append(expected1.payloads, []byte{'k', 'e', 'y'})
	assert.Equal(t, expected1, *next)

	assert.True(t, decoder.hasNext())
	next = decoder.next()
	expected2 := message{commandType: 1}
	expected2.payloads = append(expected2.payloads, []byte{'k', 'e', 'y'})
	expected2.payloads = append(expected2.payloads, []byte{'v', 'a', 'l'})
	assert.Equal(t, expected2, *next)
}

func TestConsumeOneInManyChunks(t *testing.T) {
	var (
		decoder = makeDecoder(false)
		buf1    = []byte{1, 'k', 'e', 'y', ',', 'v'}
		buf2    = []byte{'a', 'l', '$'}
	)
	decoder.consume(buf1)
	assert.False(t, decoder.hasNext())
	decoder.consume(buf2)
	assert.True(t, decoder.hasNext())
	next := decoder.next()
	expected := message{commandType: 1}
	expected.payloads = append(expected.payloads, []byte{'k', 'e', 'y'})
	expected.payloads = append(expected.payloads, []byte{'v', 'a', 'l'})
	assert.Equal(t, expected, *next)
}
