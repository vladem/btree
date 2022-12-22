package btree

import (
	"container/list"
	"fmt"
	"log"
)

type message struct {
	commandType uint8
	payloads    [][]byte
}

var (
	commandTypeGet = uint8('g')
	commandTypePut = uint8('p')
)

type getMessage struct {
	key []byte
}

type putMessage struct {
	key   []byte
	value []byte
}

type streamDecoder struct {
	buffer     []byte
	messages   *list.List
	telnetMode bool
}

func makeDecoder(telnetMode bool) *streamDecoder {
	return &streamDecoder{messages: list.New(), telnetMode: telnetMode}
}

func createMessage(buffer []byte) *message {
	message := &message{commandType: buffer[0]}
	buffer = buffer[1:]
	for len(buffer) > 0 {
		var (
			ch      byte
			readIdx = 0
			escaped = false
			part    = make([]byte, 0)
		)
		for readIdx, ch = range buffer {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
				continue
			} else if ch == ',' {
				break
			}
			part = append(part, ch)
		}
		message.payloads = append(message.payloads, part)
		buffer = buffer[readIdx+1:]
	}
	return message
}

func (s *streamDecoder) isFilteredOut(ch byte) bool {
	if !s.telnetMode {
		return false
	}
	return ch == 10 || ch == 13
}

func (s *streamDecoder) consume(chunk []byte) {
	var (
		i       int
		ch      byte
		eof     = false
		escaped = false
	)
	for i, ch = range chunk {
		if s.isFilteredOut(ch) {
			continue
		}
		if escaped {
			escaped = false
		} else if ch == '\\' {
			escaped = true
		} else if ch == '$' {
			eof = true
			break
		}
		s.buffer = append(s.buffer, ch)
	}
	if eof {
		msg := createMessage(s.buffer)
		s.messages.PushBack(msg)
		s.buffer = nil
		s.consume(chunk[i+1:])
	}
}

func (s *streamDecoder) hasNext() bool {
	return s.messages.Len() > 0
}

func (s *streamDecoder) next() *message {
	if !s.hasNext() {
		return nil
	}
	qElem := s.messages.Front()
	msg, ok := qElem.Value.(*message)
	if !ok {
		log.Fatalf("expected message ptr, got: %v", qElem.Value)
	}
	s.messages.Remove(qElem)
	return msg
}

func (m *message) ToGetMessage() (*getMessage, error) {
	if m.commandType != commandTypeGet {
		return nil, fmt.Errorf("unexpected commandType for message: %v", m)
	}
	if len(m.payloads) != 1 {
		return nil, fmt.Errorf("unexpected payloads for message: %v", m)
	}
	return &getMessage{key: m.payloads[0]}, nil
}

func (m *message) ToPutMessage() (*putMessage, error) {
	if m.commandType != commandTypePut {
		return nil, fmt.Errorf("unexpected commandType for message: %v", m)
	}
	if len(m.payloads) != 2 {
		return nil, fmt.Errorf("unexpected payloads for message: %v", m)
	}
	return &putMessage{key: m.payloads[0], value: m.payloads[1]}, nil
}
