package format

import (
	"encoding/json"
	"fmt"
	"io"
)

// RenderJSON writes the value as formatted JSON to w.
func RenderJSON(w io.Writer, v any, pretty bool) error {
	var data []byte
	var err error
	if pretty {
		data, err = json.MarshalIndent(v, "", "  ")
	} else {
		data, err = json.Marshal(v)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}

// ErrorResponse represents a structured JSON error.
type ErrorResponse struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
}

// RenderJSONError writes a structured JSON error response.
func RenderJSONError(w io.Writer, err error) {
	resp := ErrorResponse{
		Error:   true,
		Message: err.Error(),
	}
	_ = RenderJSON(w, resp, true)
}
