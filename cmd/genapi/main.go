// Command genapi reads a Smithy 2.0 JSON model and writes the AWS JSON 1.0
// Go surface under internal/api/gen.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	smithy := flag.String("smithy", "", "path to the Smithy JSON model")
	out := flag.String("out", "", "directory for generated Go and surface.gen.json")
	flag.Parse()
	if *smithy == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: genapi -smithy model.json -out dir")
		os.Exit(2)
	}
	if err := run(*smithy, *out); err != nil {
		fmt.Fprintf(os.Stderr, "genapi: %v\n", err)
		os.Exit(1)
	}
}

func run(smithyPath, outDir string) error {
	m, err := loadModel(smithyPath)
	if err != nil {
		return err
	}
	return emit(m, outDir, filepath.Base(smithyPath))
}
