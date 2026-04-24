package producer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var (
	eventTypeIndexes = map[EventType]int32{
		EventTypeViewStarted:  0,
		EventTypeViewPaused:   1,
		EventTypeViewResumed:  2,
		EventTypeViewFinished: 3,
		EventTypeLiked:        4,
		EventTypeSearched:     5,
	}
	deviceTypeIndexes = map[DeviceType]int32{
		DeviceTypeMobile:  0,
		DeviceTypeDesktop: 1,
		DeviceTypeTV:      2,
		DeviceTypeTablet:  3,
	}
)

func encodeConfluentMovieEvent(schemaID int, event MovieEvent) ([]byte, error) {
	var payload bytes.Buffer
	payload.Grow(128)
	payload.WriteByte(0)

	var schemaIDBytes [4]byte
	binary.BigEndian.PutUint32(schemaIDBytes[:], uint32(schemaID))
	payload.Write(schemaIDBytes[:])

	if err := writeString(&payload, event.EventID); err != nil {
		return nil, err
	}
	if err := writeString(&payload, event.UserID); err != nil {
		return nil, err
	}
	if err := writeString(&payload, event.MovieID); err != nil {
		return nil, err
	}
	if err := writeEnum(&payload, eventTypeIndexes[event.EventType]); err != nil {
		return nil, err
	}
	if err := writeLong(&payload, int64(event.Timestamp)); err != nil {
		return nil, err
	}
	if err := writeEnum(&payload, deviceTypeIndexes[event.DeviceType]); err != nil {
		return nil, err
	}
	if err := writeString(&payload, event.SessionID); err != nil {
		return nil, err
	}
	if err := writeInt(&payload, int32(event.ProgressSeconds)); err != nil {
		return nil, err
	}

	return payload.Bytes(), nil
}

func writeString(w io.Writer, value string) error {
	if err := writeLong(w, int64(len(value))); err != nil {
		return err
	}
	_, err := io.WriteString(w, value)
	return err
}

func writeEnum(w io.Writer, index int32) error {
	return writeInt(w, index)
}

func writeInt(w io.Writer, value int32) error {
	return writeLong(w, int64(value))
}

func writeLong(w io.Writer, value int64) error {
	var buf [10]byte
	u := uint64(value<<1) ^ uint64(value>>63)
	n := binary.PutUvarint(buf[:], u)
	if n <= 0 {
		return fmt.Errorf("failed to encode avro integer")
	}
	_, err := w.Write(buf[:n])
	return err
}
