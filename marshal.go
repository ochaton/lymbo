package lymbo

import "encoding/json"

type PayloadMarshaler interface {
	MarshalPayload() ([]byte, error)
}

type PayloadUnmarshaler interface {
	UnmarshalPayload([]byte) error
}

type ErrorMarshaler interface {
	MarshalError() ([]byte, error)
}

type ErrorUnmarshaler interface {
	UnmarshalError([]byte) error
}

type marshaller func() ([]byte, error)

type marshalFunc struct {
	arg  any
	call func(v any) ([]byte, error)
}

func (m *marshalFunc) MarshalError() ([]byte, error) {
	return m.call(m.arg)
}

func (m *marshalFunc) MarshalPayload() ([]byte, error) {
	return m.call(m.arg)
}

func (m *marshaller) MarshalError() ([]byte, error) {
	return (*m)()
}

func (m *marshaller) MarshalPayload() ([]byte, error) {
	return (*m)()
}

func toDefaultPayloadMarshaller(payload any) PayloadMarshaler {
	if r, ok := payload.(PayloadMarshaler); ok {
		return r
	}
	if r, ok := payload.(json.Marshaler); ok {
		var m marshaller = r.MarshalJSON
		return &m
	}
	return &marshalFunc{arg: payload, call: json.Marshal}
}

func toDefaultErrorMarshaller(reason any) ErrorMarshaler {
	if r, ok := reason.(ErrorMarshaler); ok {
		return r
	}
	if r, ok := reason.(json.Marshaler); ok {
		var m marshaller = r.MarshalJSON
		return &m
	}
	return &marshalFunc{arg: reason, call: json.Marshal}
}
