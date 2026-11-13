package api

import (
	"context"
	"regexp"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

var mapRunARNPattern = regexp.MustCompile(`^arn:aws:states:[a-z0-9-]+:[0-9]{12}:mapRun:.+$`)

func (s *Service) mapRun(arn string) (store.MapRun, error) {
	if !mapRunARNPattern.MatchString(arn) {
		return store.MapRun{}, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	run, ok := s.Store.MapRun(arn)
	if !ok {
		return store.MapRun{}, modeled("ResourceNotFound", "Resource not found: '"+arn+"'")
	}
	return run, nil
}

func (s *Service) DescribeMapRun(_ context.Context, in *gen.DescribeMapRunInput) (*gen.DescribeMapRunOutput, error) {
	run, err := s.mapRun(valueOf(in.MapRunArn))
	if err != nil {
		return nil, err
	}
	status := gen.MapRunStatus(run.Status)
	maxc := run.MaxConcurrency
	pct := 0.0
	if run.TolerancePct != nil {
		pct = float64(*run.TolerancePct)
	}
	var count int64
	if run.ToleranceCount != nil {
		count = *run.ToleranceCount
	}
	items := counts(run.Items)
	execs := counts(run.Executions)
	zero := int32(0)
	out := &gen.DescribeMapRunOutput{
		MapRunArn:                  sp(run.ARN),
		ExecutionArn:               sp(run.ExecutionARN),
		Status:                     &status,
		StartDate:                  ts(run.Started),
		MaxConcurrency:             &maxc,
		ToleratedFailurePercentage: &pct,
		ToleratedFailureCount:      &count,
		ItemCounts:                 (*gen.MapRunItemCounts)(&items),
		ExecutionCounts:            &execs,
		RedriveCount:               &zero,
	}
	if run.Stopped != nil {
		out.StopDate = ts(*run.Stopped)
	}
	return out, nil
}

// counts fills every count, including the redrive counts this server keeps at 0.
func counts(c store.MapRunCounts) gen.MapRunExecutionCounts {
	i := func(n int64) *int64 { return &n }
	return gen.MapRunExecutionCounts{
		Pending: i(c.Pending), Running: i(c.Running), Succeeded: i(c.Succeeded),
		Failed: i(c.Failed), TimedOut: i(c.TimedOut), Aborted: i(c.Aborted),
		Total: i(c.Total), ResultsWritten: i(c.ResultsWritten),
		FailuresNotRedrivable: i(0), PendingRedrive: i(0),
	}
}

func (s *Service) ListMapRuns(_ context.Context, in *gen.ListMapRunsInput) (*gen.ListMapRunsOutput, error) {
	parent, err := s.standardExecution(valueOf(in.ExecutionArn))
	if err != nil {
		return nil, err
	}
	runs := s.Store.MapRuns(parent.ARN)
	start, end, next, err := pageBounds(len(runs), in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}
	list := make(gen.MapRunList, 0, end-start)
	for _, run := range runs[start:end] {
		item := gen.MapRunListItem{
			ExecutionArn:    sp(run.ExecutionARN),
			MapRunArn:       sp(run.ARN),
			StateMachineArn: sp(parent.StateMachineARN),
			StartDate:       ts(run.Started),
		}
		if run.Stopped != nil {
			item.StopDate = ts(*run.Stopped)
		}
		list = append(list, item)
	}
	return &gen.ListMapRunsOutput{MapRuns: &list, NextToken: next}, nil
}

// UpdateMapRun changes a RUNNING Map Run. The runner reads MaxConcurrency
// before each child starts and the tolerances after each child finishes.
func (s *Service) UpdateMapRun(_ context.Context, in *gen.UpdateMapRunInput) (*gen.UpdateMapRunOutput, error) {
	run, err := s.mapRun(valueOf(in.MapRunArn))
	if err != nil {
		return nil, err
	}
	if in.MaxConcurrency == nil && in.ToleratedFailurePercentage == nil && in.ToleratedFailureCount == nil {
		return nil, modeled("ValidationException", "At least one of maxConcurrency, toleratedFailurePercentage, or toleratedFailureCount is required")
	}
	if run.Status != store.MapRunRunning {
		return nil, modeled("ValidationException", "Map Run is not running: '"+run.ARN+"'")
	}
	s.Store.UpdateMapRun(run.ARN, func(m *store.MapRun) {
		if in.MaxConcurrency != nil {
			m.MaxConcurrency = *in.MaxConcurrency
		}
		if in.ToleratedFailurePercentage != nil {
			p := float32(*in.ToleratedFailurePercentage)
			m.TolerancePct = &p
		}
		if in.ToleratedFailureCount != nil {
			c := *in.ToleratedFailureCount
			m.ToleranceCount = &c
		}
	})
	return &gen.UpdateMapRunOutput{}, nil
}
