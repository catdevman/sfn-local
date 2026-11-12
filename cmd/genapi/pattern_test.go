package main

import (
	"regexp"
	"testing"
)

func TestLowerLeadingLookahead(t *testing.T) {
	p, err := lowerPattern(`^(?=.*[a-zA-Z_\-\.])[a-zA-Z0-9_\-\.]+$`)
	if err != nil {
		t.Fatal(err)
	}
	if p.extra == "" {
		t.Fatal("lookahead was not lowered")
	}
	main := regexp.MustCompile(p.main)
	extra := regexp.MustCompile(p.extra)
	if !main.MatchString("rebuild-check") || !extra.MatchString("rebuild-check") {
		t.Fatal("rebuild-check")
	}
	if !main.MatchString("123") || extra.MatchString("123") {
		t.Fatal("digits should fail the lookahead only")
	}
	if main.MatchString("a b") {
		t.Fatal("space matched the character class")
	}
	if _, err := lowerPattern(`(?=foo)`); err == nil {
		t.Fatal("unsupported lookahead compiled")
	}
}
