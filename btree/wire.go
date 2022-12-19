package btree

type message struct {
	commandType uint8
	payloads    [][]byte
}

type getMessage struct {
	key []byte
}

type putMessage struct {
	key   []byte
	value []byte
}

type streamDecoder struct {
	buffer []byte
}

func makeDecoder() *streamDecoder {
	return &streamDecoder{}
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

func (s *streamDecoder) consume(chunk []byte) *message {
	eof := false
	escaped := false
	for i := 0; i < len(chunk); i++ {
		if escaped {
			escaped = false
		} else if chunk[i] == '\\' {
			escaped = true
		} else if chunk[i] == '$' {
			eof = true
			break
		}
		s.buffer = append(s.buffer, chunk[i])
	}
	if eof {
		message := createMessage(s.buffer)
		s.buffer = nil // todo: support multiple messages in a stream
		return message
	}
	return nil
}

// func (m *message) ToGetMessage() (*getMessage, error) {

// }

// func (m *message) ToPutMessage() (*putMessage, error) {

// }
