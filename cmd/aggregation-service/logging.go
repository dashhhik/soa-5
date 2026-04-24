package main

import (
	"encoding/json"
	"io"
	"log"
)

type jsonLogger struct {
	out *log.Logger
}

func newJSONLogger(w io.Writer) *jsonLogger {
	return &jsonLogger{out: log.New(w, "", 0)}
}

func (l *jsonLogger) Info(msg string, fields map[string]any) {
	l.log("info", msg, fields)
}

func (l *jsonLogger) Error(msg string, fields map[string]any) {
	l.log("error", msg, fields)
}

func (l *jsonLogger) log(level, msg string, fields map[string]any) {
	record := map[string]any{
		"level": level,
		"msg":   msg,
	}
	for k, v := range fields {
		record[k] = v
	}

	blob, err := json.Marshal(record)
	if err != nil {
		l.out.Printf(`{"level":"error","msg":"log marshal failed","error":%q}`, err.Error())
		return
	}
	l.out.Print(string(blob))
}
