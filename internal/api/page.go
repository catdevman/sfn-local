package api

import (
	"regexp"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

// logGroupARNPattern is the CloudWatch log-group ARN accepted as a logging destination.
var logGroupARNPattern = regexp.MustCompile(`^arn:[A-Za-z0-9-]+:logs:[A-Za-z0-9-]*:[0-9]{12}:log-group:.+`)

func sp(s string) *string { return &s }

func ts(t time.Time) *gen.Timestamp {
	v := gen.NewTimestamp(float64(t.UnixMilli()) / 1000)
	return &v
}

// pageBounds slices a sorted collection. An omitted maxResults is 100.
// nextToken is the decimal index the client sends back.
func pageBounds(n int, max *int32, token *string) (start, end int, next *string, err error) {
	limit := 100
	if max != nil {
		limit = int(*max)
	}
	if token != nil {
		if len(*token) > 1024 || !decimalToken(*token) {
			return 0, 0, nil, modeled("InvalidToken", "Invalid Token: '"+*token+"'")
		}
		v, convErr := strconv.Atoi(*token)
		if convErr != nil || v < 0 || v >= n {
			return 0, 0, nil, modeled("InvalidToken", "Invalid Token: '"+*token+"'")
		}
		start = v
	}
	end = start + limit
	if end > n {
		end = n
	}
	if end < n {
		text := strconv.Itoa(end)
		next = &text
	}
	return start, end, next, nil
}

func decimalToken(s string) bool {
	if s == "0" {
		return true
	}
	if s == "" || s[0] < '1' || s[0] > '9' {
		return false
	}
	for _, c := range s[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func tagCharset(s string) bool {
	for _, r := range s {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), unicode.IsSpace(r):
		case r < utf8.RuneSelf && containsByte("_.:/=+-@", byte(r)):
		default:
			return false
		}
	}
	return true
}

func containsByte(s string, b byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return true
		}
	}
	return false
}

func tagsFrom(list *gen.TagList) ([]store.Tag, error) {
	if list == nil {
		return nil, nil
	}
	out := make([]store.Tag, 0, len(*list))
	seen := map[string]int{}
	for _, tag := range *list {
		if tag.Key == nil || *tag.Key == "" {
			return nil, modeled("ValidationException", "tag key is required")
		}
		value := ""
		if tag.Value != nil {
			value = *tag.Value
		}
		if !tagCharset(*tag.Key) || !tagCharset(value) {
			return nil, modeled("ValidationException", "tag key and value may contain letters, digits, whitespace, and _.:/=+-@")
		}
		if i, ok := seen[*tag.Key]; ok {
			out[i].Value = value
			continue
		}
		seen[*tag.Key] = len(out)
		out = append(out, store.Tag{Key: *tag.Key, Value: value})
	}
	if len(out) > 50 {
		return nil, modeled("TooManyTags", "Too many tags provided.")
	}
	return out, nil
}

func toGenTags(tags []store.Tag) *gen.TagList {
	list := make(gen.TagList, 0, len(tags))
	for _, tag := range tags {
		key, value := tag.Key, tag.Value
		list = append(list, gen.Tag{Key: &key, Value: &value})
	}
	return &list
}

// normalizeLogging stores a logging configuration. enforce is true for an
// EXPRESS create and for every update that includes logging. OFF with no
// destinations is valid. Any other level needs exactly one destination.
// More than one destination fails even when the level is OFF. Nothing is written.
func normalizeLogging(in *gen.LoggingConfiguration, enforce bool) (*store.Logging, error) {
	if in == nil {
		return nil, nil
	}
	level := gen.LogLevelOFF
	if in.Level != nil {
		if !in.Level.Valid() {
			return nil, modeled("InvalidLoggingConfiguration", "Invalid Logging Configuration: '"+string(*in.Level)+"'")
		}
		level = *in.Level
	}
	if enforce {
		if err := checkLogDestinations(in.Destinations, level); err != nil {
			return nil, err
		}
	}
	out := &store.Logging{Level: string(level)}
	if in.IncludeExecutionData != nil {
		v := *in.IncludeExecutionData
		out.IncludeExecutionData = &v
	}
	if in.Destinations != nil {
		for _, dest := range *in.Destinations {
			row := store.Destination{}
			if dest.CloudWatchLogsLogGroup != nil && dest.CloudWatchLogsLogGroup.LogGroupArn != nil {
				row.LogGroupArn = *dest.CloudWatchLogsLogGroup.LogGroupArn
			}
			out.Destinations = append(out.Destinations, row)
		}
	}
	return out, nil
}

func checkLogDestinations(list *gen.LogDestinationList, level gen.LogLevel) error {
	n := 0
	if list != nil {
		n = len(*list)
	}
	if n > 1 || (n == 0 && level != gen.LogLevelOFF) {
		return modeled("InvalidLoggingConfiguration", "Invalid Logging Configuration: 'Must specify exactly one Log Destination'")
	}
	if n != 1 {
		return nil
	}
	dest := (*list)[0]
	arn := ""
	if dest.CloudWatchLogsLogGroup != nil && dest.CloudWatchLogsLogGroup.LogGroupArn != nil {
		arn = *dest.CloudWatchLogsLogGroup.LogGroupArn
	}
	if !logGroupARNPattern.MatchString(arn) {
		return modeled("InvalidLoggingConfiguration", "Invalid Logging Configuration: 'Must set valid CloudWatch Log Group ARN'")
	}
	return nil
}

func toGenLogging(in *store.Logging) *gen.LoggingConfiguration {
	if in == nil {
		return nil
	}
	level := gen.LogLevel(in.Level)
	out := &gen.LoggingConfiguration{Level: &level}
	if in.IncludeExecutionData != nil {
		v := *in.IncludeExecutionData
		out.IncludeExecutionData = &v
	}
	if in.Destinations != nil {
		list := make(gen.LogDestinationList, 0, len(in.Destinations))
		for _, dest := range in.Destinations {
			arn := dest.LogGroupArn
			list = append(list, gen.LogDestination{
				CloudWatchLogsLogGroup: &gen.CloudWatchLogsLogGroup{LogGroupArn: &arn},
			})
		}
		out.Destinations = &list
	}
	return out
}
