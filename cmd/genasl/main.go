// Command genasl reads the Amazon States Language spec HTML and writes the
// field matrix under internal/asl/specgen. The HTML file is not modified.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	htmlPath := flag.String("html", "", "path to aws-spec/asl/spec.html")
	out := flag.String("out", "", "directory for matrix.go and matrix.json")
	flag.Parse()
	if *htmlPath == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: genasl -html spec.html -out dir")
		os.Exit(2)
	}
	if err := run(*htmlPath, *out); err != nil {
		fmt.Fprintf(os.Stderr, "genasl: %v\n", err)
		os.Exit(1)
	}
}

func run(htmlPath, outDir string) error {
	data, err := os.ReadFile(htmlPath)
	if err != nil {
		return err
	}
	doc, err := parseSpec(string(data))
	if err != nil {
		return err
	}
	return emit(doc, outDir, filepath.Base(htmlPath))
}
