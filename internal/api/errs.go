package api

import (
	"fmt"

	"github.com/catdevman/sfn-local/internal/api/gen"
)

func modeled(name, message string) error {
	err, ok := gen.NewModeledError(name, message)
	if !ok {
		return fmt.Errorf("%s: %s", name, message)
	}
	return err
}

// wireError is an HTTP error that is not a Smithy shape. The default profile
// still uses a com.amazonaws.sfn# prefix. The compat profile may use a jar prefix.
type wireError struct {
	name    string
	shape   string
	message string
	status  int
}

func (e *wireError) Error() string {
	if e == nil || e.message == "" {
		return e.name
	}
	return e.name + ": " + e.message
}

func (e *wireError) ErrorName() string { return e.name }

func (e *wireError) ShapeID() string { return e.shape }

func (e *wireError) StatusCode() int { return e.status }

func (s *Service) unsupported(message string) error {
	shape := shapePrefix + "UnsupportedOperation"
	if s != nil && s.Profile == ProfileSFNLocal {
		shape = jarUnsupported
	}
	return &wireError{name: "UnsupportedOperation", shape: shape, message: message, status: 500}
}
