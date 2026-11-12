package api

import "github.com/catdevman/sfn-local/internal/api/gen"

// Service is the operation handler. Phase G embeds Unimplemented and does
// not override a method. Deferred operations return from the HTTP layer
// before Call reaches this value.
type Service struct {
	gen.Unimplemented
}
