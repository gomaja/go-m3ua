package messages

import (
	"fmt"

	"github.com/gomaja/go-m3ua/messages/params"
)

func requireParameter(param *params.Param, name string) error {
	if param == nil {
		return fmt.Errorf("%w: %s", ErrMissingParameter, name)
	}
	return nil
}

func parseTypedHeader(b []byte, expectedClass, expectedType uint8) (*Header, error) {
	header, err := ParseHeader(b)
	if err != nil {
		return nil, err
	}
	if header.Class != expectedClass || header.Type != expectedType {
		return nil, fmt.Errorf(
			"%w: got class %d type %d, want class %d type %d",
			ErrUnexpectedMessageType,
			header.Class,
			header.Type,
			expectedClass,
			expectedType,
		)
	}
	return header, nil
}

func marshalOtherParams(payload []byte, offset int, others []*params.Param) error {
	othersLength := paramsMarshalLen(others)
	if offset < 0 || offset > len(payload) || othersLength > len(payload)-offset {
		return ErrTooShortToMarshalBinary
	}
	if offset+othersLength != len(payload) {
		return fmt.Errorf(
			"%w: parameters occupy %d of %d payload octets",
			ErrInvalidMessageLength,
			offset+othersLength,
			len(payload),
		)
	}

	for _, param := range others {
		if param == nil {
			continue
		}
		if err := param.MarshalTo(payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	return nil
}

func setParamLengths(parameters []*params.Param) {
	for _, param := range parameters {
		if param != nil {
			param.SetLength()
		}
	}
}

func paramsMarshalLen(parameters []*params.Param) int {
	length := 0
	for _, param := range parameters {
		if param != nil {
			length += param.MarshalLen()
		}
	}
	return length
}
