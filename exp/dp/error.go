package dp

import (
	"github.com/bitly/go-simplejson"
	"log"
	"net/http"
	"strings"
)

type Error struct {
	StatusCode int // HTTP status code (200, 403, ...)
	Status     string
	Code       string // Dynamodb error code ("MalformedQueryString", ...)
	Message    string // The human-oriented error message
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

func buildError(r *http.Response, jsonBody []byte) error {

	ddbError := Error{
		StatusCode: r.StatusCode,
		Status:     r.Status,
	}
	// TODO return error if Unmarshal fails?

	json, err := simplejson.NewJson(jsonBody)
	if err != nil {
		log.Printf("Failed to parse body as JSON")
		return err
	}
	ddbError.Message = json.Get("message").MustString()

	// Of the form: com.amazon.coral.validate#ValidationException
	// We only want the last part
	codeStr := json.Get("__type").MustString()
	hashIndex := strings.Index(codeStr, "#")
	if hashIndex > 0 {
		codeStr = codeStr[hashIndex+1:]
	}
	ddbError.Code = codeStr

	return &ddbError
}
