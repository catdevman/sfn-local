package api

import "github.com/catdevman/sfn-local/internal/api/gen"

// Profile selects the error __type strings written on the wire.
// smithy is the default and emits the public Smithy shape id.
// sfn-local selects the Step Functions Local profile. The jar __type map
// stays empty until a later phase has fixtures. The jar never shipped 14
// operations. This profile returns HTTP 500 UnsupportedOperation for each
// of them before decode, except TestState, which this server implements.
type Profile string

const (
	ProfileSmithy   Profile = "smithy"
	ProfileSFNLocal Profile = "sfn-local"

	shapePrefix = "com.amazonaws.sfn#"

	// Jar prefixes from docs/spec.md. The default profile does not emit them.
	jarModelPrefix = "com.amazonaws.swf.service.v2.model#"
	jarCoralPrefix = "com.amazon.coral.validate#"
	jarModelDot    = "com.amazonaws.services.stepfunctions.model."

	jarUnsupported = jarModelPrefix + "UnsupportedOperation"
	jarMissingHdr  = jarModelPrefix + "MissingRequiredHeader"
	jarInternal    = jarModelPrefix + "InternalFailure"

	// UnknownOperationType is the __type for a target the model does not list.
	UnknownOperationType = "UnknownOperationException"
)

// jarErrorTypes maps a Smithy error name to a jar __type.
// It is empty on purpose. Add a row only after a jar fixture captures the string.
var jarErrorTypes = map[string]string{}

// jarAbsent is the Step Functions Local 2.0.0 operation set that this process
// still refuses before decode. TestState is implemented, so it is not in the set.
// The other names stay deferred. The default profile returns their deferred error.
var jarAbsent = map[string]bool{
	"CreateStateMachineAlias":        true,
	"DeleteStateMachineAlias":        true,
	"DeleteStateMachineVersion":      true,
	"DescribeMapRun":                 true,
	"DescribeStateMachineAlias":      true,
	"ListMapRuns":                    true,
	"ListStateMachineAliases":        true,
	"ListStateMachineVersions":       true,
	"PublishStateMachineVersion":     true,
	"RedriveExecution":               true,
	"UpdateMapRun":                   true,
	"UpdateStateMachineAlias":        true,
	"ValidateStateMachineDefinition": true,
}

// ParseProfile accepts smithy and sfn-local.
func ParseProfile(v string) (Profile, bool) {
	switch Profile(v) {
	case ProfileSmithy, ProfileSFNLocal:
		return Profile(v), true
	default:
		return "", false
	}
}

// TypeFor returns the __type for a modeled error.
func (p Profile) TypeFor(err gen.APIError) string {
	if p == ProfileSFNLocal {
		if t, ok := jarErrorTypes[err.ErrorName()]; ok && t != "" {
			return t
		}
	}
	return err.ShapeID()
}

// MissingHeaderType is the __type when X-Amz-Target is missing or has no dot.
func (p Profile) MissingHeaderType() string {
	if p == ProfileSFNLocal {
		return jarMissingHdr
	}
	return shapePrefix + "MissingRequiredParameter"
}

// InternalFailureType is the __type for a plain Go error.
func (p Profile) InternalFailureType() string {
	if p == ProfileSFNLocal {
		return jarInternal
	}
	return shapePrefix + "InternalFailure"
}

// JarAbsent reports whether the jar returns UnsupportedOperation for this operation.
func JarAbsent(name string) bool {
	return jarAbsent[name]
}
