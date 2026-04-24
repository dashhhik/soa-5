package producer

import _ "embed"

//go:embed schema.avsc
var embeddedMovieEventSchema string

func movieEventSchema() string {
	return embeddedMovieEventSchema
}
