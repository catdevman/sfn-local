package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

const maxTaskOutput = 262144

// GetActivityTask waits up to ActivityPoll for a scheduled activity token.
// The wait does not hold the store lock. An empty poll returns {}.
func (s *Service) GetActivityTask(ctx context.Context, in *gen.GetActivityTaskInput) (*gen.GetActivityTaskOutput, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("no store")
	}
	if in == nil {
		in = &gen.GetActivityTaskInput{}
	}
	arn := valueOf(in.ActivityArn)
	if _, err := s.activityByARN(arn); err != nil {
		return nil, err
	}
	worker := valueOf(in.WorkerName)
	if ctx == nil {
		ctx = context.Background()
	}
	ch := make(chan *store.TaskToken, 1)
	tok, waiting := s.Store.PollActivity(arn, worker, ch)
	if !waiting {
		return activityOut(tok), nil
	}
	timer := time.NewTimer(s.activityPoll())
	defer timer.Stop()
	select {
	case tok = <-ch:
		return activityOut(tok), nil
	case <-ctx.Done():
		s.Store.CancelPoller(arn, ch)
		select {
		case tok = <-ch:
			return activityOut(tok), nil
		default:
			return nil, ctx.Err()
		}
	case <-timer.C:
		s.Store.CancelPoller(arn, ch)
		select {
		case tok = <-ch:
			return activityOut(tok), nil
		default:
			return &gen.GetActivityTaskOutput{}, nil
		}
	}
}

func activityOut(tok *store.TaskToken) *gen.GetActivityTaskOutput {
	if tok == nil {
		return &gen.GetActivityTaskOutput{}
	}
	out := &gen.GetActivityTaskOutput{TaskToken: sp(tok.ID)}
	if tok.Input != "" {
		out.Input = sp(tok.Input)
	}
	return out
}

// SendTaskSuccess completes a started token. Output must be JSON of at most 256 KiB.
func (s *Service) SendTaskSuccess(_ context.Context, in *gen.SendTaskSuccessInput) (*gen.SendTaskSuccessOutput, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("no store")
	}
	if in == nil {
		in = &gen.SendTaskSuccessInput{}
	}
	text, err := compactTaskOutput(valueOf(in.Output))
	if err != nil {
		return nil, err
	}
	err = s.Store.FinishToken(valueOf(in.TaskToken), store.TokenDone{Status: store.TokenSucceeded, Output: text})
	if err != nil {
		return nil, tokenErr(err)
	}
	return &gen.SendTaskSuccessOutput{}, nil
}

// SendTaskFailure fails a started token. Error and cause are omitted when absent.
func (s *Service) SendTaskFailure(_ context.Context, in *gen.SendTaskFailureInput) (*gen.SendTaskFailureOutput, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("no store")
	}
	if in == nil {
		in = &gen.SendTaskFailureInput{}
	}
	err := s.Store.FinishToken(valueOf(in.TaskToken), store.TokenDone{
		Status: store.TokenFailed,
		Error:  valueOf(in.Error),
		Cause:  valueOf(in.Cause),
	})
	if err != nil {
		return nil, tokenErr(err)
	}
	return &gen.SendTaskFailureOutput{}, nil
}

// SendTaskHeartbeat resets the heartbeat timer of a started token.
func (s *Service) SendTaskHeartbeat(_ context.Context, in *gen.SendTaskHeartbeatInput) (*gen.SendTaskHeartbeatOutput, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("no store")
	}
	if in == nil {
		in = &gen.SendTaskHeartbeatInput{}
	}
	if err := s.Store.HeartbeatToken(valueOf(in.TaskToken)); err != nil {
		return nil, tokenErr(err)
	}
	return &gen.SendTaskHeartbeatOutput{}, nil
}

func compactTaskOutput(text string) (string, error) {
	if len(text) > maxTaskOutput {
		return "", modeled("InvalidOutput", "Invalid Output: 'output exceeded the maximum size of 262144 bytes'")
	}
	dec := json.NewDecoder(strings.NewReader(text))
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", modeled("InvalidOutput", "Invalid Output: '"+err.Error()+"'")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return "", modeled("InvalidOutput", "Invalid Output: 'trailing tokens'")
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(text)); err != nil {
		return "", modeled("InvalidOutput", "Invalid Output: '"+err.Error()+"'")
	}
	if buf.Len() > maxTaskOutput {
		return "", modeled("InvalidOutput", "Invalid Output: 'output exceeded the maximum size of 262144 bytes'")
	}
	return buf.String(), nil
}

func tokenErr(err error) error {
	var te *store.TokenError
	if !errors.As(err, &te) || te == nil {
		return err
	}
	switch te.Kind {
	case "TaskTimedOut":
		return modeled("TaskTimedOut", "Task Timed Out: '"+te.Resource+"'")
	case "TaskDoesNotExist":
		return modeled("TaskDoesNotExist", "Task Does Not Exist: '"+te.ID+"'")
	default:
		return modeled("InvalidToken", "Invalid Token: '"+te.ID+"'")
	}
}
