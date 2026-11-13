package api

import (
	"fmt"
	"regexp"
	"strings"
)

// Invalid-name and ARN patterns from docs/spec.md. Smithy Name is only a
// length, so these stay handwritten. A full match of invalidNamePattern
// rejects the name.
var (
	invalidNamePattern = regexp.MustCompile(
		`.*[\s<>"#%{}\^|~\[\]` + "`" + `$&,;:/?*\x00-\x1f\x7f-\x9f]+.*`,
	)
	roleARNPattern = regexp.MustCompile(
		`^arn:[\w-]+:iam::\d{12}:role((/)|(/[!-\x7f]+/))[\w+=,.@-]+$`,
	)
	stateMachineARNPattern = regexp.MustCompile(
		`^arn:aws:states:[a-z0-9-]+:[0-9]{12}:stateMachine:.+$`,
	)
	activityARNPattern = regexp.MustCompile(
		`^arn:aws:states:[a-z0-9-]+:[0-9]{12}:activity:.+$`,
	)
	executionARNPattern = regexp.MustCompile(
		`^arn:aws:states:[a-z0-9-]+:[0-9]{12}:execution:.*?:.*$`,
	)
	// The jar checks this placeholder text before a real express ARN.
	expressPlaceholderPattern = regexp.MustCompile(
		`^arn:aws:states:[a-z0-9-]+:[0-9]{12}:express:stateMachineName:.*$`,
	)
	expressARNPattern = regexp.MustCompile(
		`^arn:aws:states:[a-z0-9-]+:[0-9]{12}:express:.+:.+$`,
	)
	versionQualifier   = regexp.MustCompile(`^[1-9][0-9]*$`)
	aliasQualifier     = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	aliasQualifierMark = regexp.MustCompile(`[A-Za-z_.-]`)
)

const stateMachineMarker = ":stateMachine:"

const expressPlaceholderMessage = "Resource type not valid in this context: express"

// NameRejected reports a full match of the jar invalid-name pattern.
func NameRejected(name string) bool {
	return invalidNamePattern.MatchString(name)
}

// RoleARNValid reports a match of the path-capable IAM role pattern.
func RoleARNValid(arn string) bool {
	return roleARNPattern.MatchString(arn)
}

// StateMachineARN builds an ARN with the process region and account.
func StateMachineARN(region, account, name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", region, account, name)
}

// ActivityARN builds an activity ARN.
func ActivityARN(region, account, name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:activity:%s", region, account, name)
}

// ExecutionARN builds a standard or express execution ARN.
func ExecutionARN(region, account, machineName, execName string, express bool) string {
	kind := "execution"
	if express {
		kind = "express"
	}
	return fmt.Sprintf("arn:aws:states:%s:%s:%s:%s:%s", region, account, kind, machineName, execName)
}

// ValidStateMachineARN reports whether arn has the state-machine shape.
// Lookup is still an exact map key. A version or alias suffix still matches.
func ValidStateMachineARN(arn string) bool {
	return stateMachineARNPattern.MatchString(arn)
}

// splitQualifiedARN splits a state-machine ARN into the unqualified ARN and
// the suffix after the machine name. Machine names cannot contain ':'.
// ok is false when the shape is not a state-machine ARN or the suffix is empty.
func splitQualifiedARN(arn string) (base, qual string, ok bool) {
	if !ValidStateMachineARN(arn) {
		return "", "", false
	}
	i := strings.Index(arn, stateMachineMarker)
	if i < 0 {
		return "", "", false
	}
	rest := arn[i+len(stateMachineMarker):]
	j := strings.IndexByte(rest, ':')
	if j < 0 {
		return arn, "", true
	}
	if j == 0 || j == len(rest)-1 || strings.Contains(rest[j+1:], ":") {
		return "", "", false
	}
	return arn[:i+len(stateMachineMarker)+j], rest[j+1:], true
}

// qualifierKind classifies the suffix from splitQualifiedARN.
// machine, version, alias, or invalid.
func qualifierKind(qual string) string {
	switch {
	case qual == "":
		return "machine"
	case versionQualifier.MatchString(qual):
		return "version"
	case len(qual) <= 80 && aliasQualifier.MatchString(qual) && aliasQualifierMark.MatchString(qual):
		return "alias"
	default:
		return "invalid"
	}
}

// ValidActivityARN reports whether arn has the activity shape.
func ValidActivityARN(arn string) bool {
	return activityARNPattern.MatchString(arn)
}

// SplitMockSuffix strips one # test-case suffix. More than one # is an error.
func SplitMockSuffix(arn string) (base, test string, err error) {
	n := strings.Count(arn, "#")
	if n == 0 {
		return arn, "", nil
	}
	if n > 1 {
		return "", "", fmt.Errorf("more than one '#'")
	}
	i := strings.IndexByte(arn, '#')
	return arn[:i], arn[i+1:], nil
}

// ExecutionARNKind classifies an ARN for DescribeExecution, history, stop,
// and DescribeStateMachineForExecution.
//
//	placeholder: the literal express:stateMachineName: token
//	standard:    a standard execution ARN shape
//	express:     a real express ARN
//	invalid:     anything else
func ExecutionARNKind(arn string) string {
	switch {
	case expressPlaceholderPattern.MatchString(arn):
		return "placeholder"
	case executionARNPattern.MatchString(arn):
		return "standard"
	case expressARNPattern.MatchString(arn):
		return "express"
	default:
		return "invalid"
	}
}
