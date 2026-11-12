package api

import _ "embed"

//go:embed surface.toml
var surfaceTOML string

// LoadSurface reads the embedded surface.toml.
func LoadSurface() (*Surface, error) {
	return ParseSurface(surfaceTOML)
}
