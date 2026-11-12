package main

import (
	"fmt"
	"regexp"
	"strings"
)

// patternProgram is a Smithy @pattern lowered to RE2.
// extra is set when the source uses a leading positive lookahead, which RE2
// does not compile. Both expressions must match.
type patternProgram struct {
	main  string
	extra string
}

func lowerPattern(pattern string) (patternProgram, error) {
	if _, err := regexp.Compile(pattern); err == nil {
		return patternProgram{main: pattern}, nil
	}
	main, extra, ok := lowerLeadingLookahead(pattern)
	if !ok {
		return patternProgram{}, fmt.Errorf("pattern %q is not valid RE2 and has no supported lookahead lowering", pattern)
	}
	if _, err := regexp.Compile(main); err != nil {
		return patternProgram{}, fmt.Errorf("lowered pattern %q: %w", pattern, err)
	}
	if _, err := regexp.Compile(extra); err != nil {
		return patternProgram{}, fmt.Errorf("lowered lookahead %q: %w", pattern, err)
	}
	return patternProgram{main: main, extra: extra}, nil
}

// lowerLeadingLookahead rewrites ^(?=.*X)REST into main ^REST and extra X.
func lowerLeadingLookahead(pattern string) (main, extra string, ok bool) {
	const prefix = "^(?="
	if !strings.HasPrefix(pattern, prefix) {
		return "", "", false
	}
	body := pattern[len(prefix):]
	end, found := findLookaheadEnd(body)
	if !found {
		return "", "", false
	}
	inside := body[:end]
	rest := body[end+1:]
	if !strings.HasPrefix(inside, ".*") || len(inside) == 2 {
		return "", "", false
	}
	return "^" + rest, inside[2:], true
}

func findLookaheadEnd(s string) (int, bool) {
	class := false
	esc := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if esc {
			esc = false
			continue
		}
		if c == '\\' {
			esc = true
			continue
		}
		if class {
			if c == ']' {
				class = false
			}
			continue
		}
		switch c {
		case '[':
			class = true
		case ')':
			return i, true
		}
	}
	return 0, false
}
