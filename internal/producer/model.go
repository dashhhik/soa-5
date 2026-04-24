package producer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventTypeViewStarted  EventType = "VIEW_STARTED"
	EventTypeViewPaused   EventType = "VIEW_PAUSED"
	EventTypeViewResumed  EventType = "VIEW_RESUMED"
	EventTypeViewFinished EventType = "VIEW_FINISHED"
	EventTypeLiked        EventType = "LIKED"
	EventTypeSearched     EventType = "SEARCHED"
)

type DeviceType string

const (
	DeviceTypeMobile  DeviceType = "MOBILE"
	DeviceTypeDesktop DeviceType = "DESKTOP"
	DeviceTypeTV      DeviceType = "TV"
	DeviceTypeTablet  DeviceType = "TABLET"
)

type TimestampMillis int64

func (t *TimestampMillis) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return errors.New("timestamp is required")
	}
	if data[0] == '"' {
		var raw string
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		parsed, err := parseTimestamp(raw)
		if err != nil {
			return err
		}
		*t = TimestampMillis(parsed.UnixMilli())
		return nil
	}
	var millis int64
	if err := json.Unmarshal(data, &millis); err != nil {
		return err
	}
	*t = TimestampMillis(millis)
	return nil
}

func (t TimestampMillis) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(t), 10)), nil
}

func (t TimestampMillis) Time() time.Time {
	return time.UnixMilli(int64(t)).UTC()
}

type MovieEvent struct {
	EventID         string          `json:"event_id"`
	UserID          string          `json:"user_id"`
	MovieID         string          `json:"movie_id"`
	EventType       EventType       `json:"event_type"`
	Timestamp       TimestampMillis `json:"timestamp"`
	DeviceType      DeviceType      `json:"device_type"`
	SessionID       string          `json:"session_id"`
	ProgressSeconds int             `json:"progress_seconds"`
}

type GeneratorStartRequest struct {
	IntervalMS  *int   `json:"interval_ms,omitempty"`
	BatchSize   *int   `json:"batch_size,omitempty"`
	MaxSessions *int   `json:"max_sessions,omitempty"`
	Seed        *int64 `json:"seed,omitempty"`
}

func (e MovieEvent) Validate() error {
	if e.EventID == "" {
		return errors.New("event_id is required")
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("event_id must be a valid UUID: %w", err)
	}
	if strings.TrimSpace(e.UserID) == "" {
		return errors.New("user_id is required")
	}
	if strings.TrimSpace(e.MovieID) == "" {
		return errors.New("movie_id is required")
	}
	if !validEventType(e.EventType) {
		return fmt.Errorf("event_type must be one of %v", supportedEventTypes())
	}
	if int64(e.Timestamp) <= 0 {
		return errors.New("timestamp must be a positive unix millis value")
	}
	if !validDeviceType(e.DeviceType) {
		return fmt.Errorf("device_type must be one of %v", supportedDeviceTypes())
	}
	if strings.TrimSpace(e.SessionID) == "" {
		return errors.New("session_id is required")
	}
	if e.ProgressSeconds < 0 {
		return errors.New("progress_seconds must be non-negative")
	}
	return nil
}

func parseTimestamp(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z07:00",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp format: %q", value)
}

func validEventType(v EventType) bool {
	switch v {
	case EventTypeViewStarted, EventTypeViewPaused, EventTypeViewResumed, EventTypeViewFinished, EventTypeLiked, EventTypeSearched:
		return true
	default:
		return false
	}
}

func validDeviceType(v DeviceType) bool {
	switch v {
	case DeviceTypeMobile, DeviceTypeDesktop, DeviceTypeTV, DeviceTypeTablet:
		return true
	default:
		return false
	}
}

func supportedEventTypes() []string {
	return []string{
		string(EventTypeViewStarted),
		string(EventTypeViewPaused),
		string(EventTypeViewResumed),
		string(EventTypeViewFinished),
		string(EventTypeLiked),
		string(EventTypeSearched),
	}
}

func supportedDeviceTypes() []string {
	return []string{
		string(DeviceTypeMobile),
		string(DeviceTypeDesktop),
		string(DeviceTypeTV),
		string(DeviceTypeTablet),
	}
}
