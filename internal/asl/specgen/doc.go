// Package specgen is the ASL field matrix generated from aws-spec/asl/spec.html.
// Regenerate with go generate. Do not edit matrix.go or matrix.json by hand.
package specgen

//go:generate go run github.com/catdevman/sfn-local/cmd/genasl -html ../../../aws-spec/asl/spec.html -out .
