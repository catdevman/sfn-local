package store

import "sync"

// Token statuses. scheduled is queued for GetActivityTask. started is in a
// worker's hands or in a callback wait. The terminal statuses are succeeded,
// failed, timed_out, and aborted.
const (
	TokenScheduled = "scheduled"
	TokenStarted   = "started"
	TokenSucceeded = "succeeded"
	TokenFailed    = "failed"
	TokenTimedOut  = "timed_out"
	TokenAborted   = "aborted"

	TokenActivity = "activity"
	TokenCallback = "callback"
)

// TaskToken is one activity or callback token. The runner parks on done.
// started is closed when a worker receives an activity token, or immediately
// for a callback. hb receives SendTaskHeartbeat.
type TaskToken struct {
	ID          string
	Kind        string
	ActivityARN string
	// Resource is the jar's TaskTimedOut argument: the activity ARN, or a
	// callback resource with an empty region and account filled in.
	Resource     string
	ExecutionARN string
	Input        string
	Status       string
	Worker       string

	started   chan struct{}
	done      chan TokenDone
	hb        chan struct{}
	startOnce sync.Once
}

// TokenDone is the terminal result delivered to the parked runner.
type TokenDone struct {
	Status string
	Output string
	Error  string
	Cause  string
}

// TokenError is a SendTask* rejection. Kind is the Smithy error name.
// Resource is set for TaskTimedOut and is the string inside the jar message.
type TokenError struct {
	Kind     string
	ID       string
	Resource string
}

func (e *TokenError) Error() string {
	if e == nil {
		return ""
	}
	return e.Kind
}

type activityWaiter struct {
	ch     chan *TaskToken
	worker string
}

// NewTaskToken builds a token the runner has not registered yet.
func NewTaskToken(id, kind, execARN, input string) *TaskToken {
	return &TaskToken{
		ID:           id,
		Kind:         kind,
		ExecutionARN: execARN,
		Input:        input,
		started:      make(chan struct{}),
		done:         make(chan TokenDone, 1),
		hb:           make(chan struct{}, 1),
	}
}

func (t *TaskToken) signalStart() {
	if t == nil {
		return
	}
	t.startOnce.Do(func() { close(t.started) })
}

// Started is closed when the token becomes started.
func (t *TaskToken) Started() <-chan struct{} {
	if t == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return t.started
}

// Done receives one terminal result.
func (t *TaskToken) Done() <-chan TokenDone {
	if t == nil {
		return nil
	}
	return t.done
}

// Heartbeats receives SendTaskHeartbeat pings.
func (t *TaskToken) Heartbeats() <-chan struct{} {
	if t == nil {
		return nil
	}
	return t.hb
}

func (s *Store) trackTokenLocked(tok *TaskToken) {
	s.tokens[tok.ID] = tok
	per := s.tokensByExec[tok.ExecutionARN]
	if per == nil {
		per = map[string]*TaskToken{}
		s.tokensByExec[tok.ExecutionARN] = per
	}
	per[tok.ID] = tok
}

// KeepExpired records a token that timed out before a worker could take it.
func (s *Store) KeepExpired(tok *TaskToken) {
	if s == nil || tok == nil {
		return
	}
	s.mu.Lock()
	tok.Status = TokenTimedOut
	s.trackTokenLocked(tok)
	s.mu.Unlock()
}

// EnqueueActivity queues a scheduled activity token, or hands it to a waiting
// GetActivityTask. The token is started when a waiter takes it.
func (s *Store) EnqueueActivity(tok *TaskToken) {
	if s == nil || tok == nil {
		return
	}
	s.mu.Lock()
	tok.Status = TokenScheduled
	s.trackTokenLocked(tok)
	if w, ok := s.popWaiterLocked(tok.ActivityARN); ok {
		tok.Status = TokenStarted
		tok.Worker = w.worker
		s.mu.Unlock()
		tok.signalStart()
		w.ch <- tok
		return
	}
	s.activityQ[tok.ActivityARN] = append(s.activityQ[tok.ActivityARN], tok)
	s.mu.Unlock()
}

// RegisterCallback records a callback token that is already started.
func (s *Store) RegisterCallback(tok *TaskToken) {
	if s == nil || tok == nil {
		return
	}
	s.mu.Lock()
	tok.Status = TokenStarted
	s.trackTokenLocked(tok)
	s.mu.Unlock()
	tok.signalStart()
}

// PollActivity returns a queued token or registers ch until CancelPoller.
// waiting is true when the caller must receive on ch.
func (s *Store) PollActivity(arn, worker string, ch chan *TaskToken) (*TaskToken, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	if tok := s.dequeueActivityLocked(arn, worker); tok != nil {
		s.mu.Unlock()
		tok.signalStart()
		return tok, false
	}
	s.activityWait[arn] = append(s.activityWait[arn], activityWaiter{ch: ch, worker: worker})
	s.mu.Unlock()
	return nil, true
}

// CancelPoller drops a GetActivityTask waiter. A token already handed to ch
// stays there.
func (s *Store) CancelPoller(arn string, ch chan *TaskToken) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	waiters := s.activityWait[arn]
	kept := waiters[:0]
	for _, w := range waiters {
		if w.ch != ch {
			kept = append(kept, w)
		}
	}
	if len(kept) == 0 {
		delete(s.activityWait, arn)
		return
	}
	s.activityWait[arn] = kept
}

// FinishToken completes a started token. A timed-out token returns
// TaskTimedOut. Any other non-started token returns TaskDoesNotExist.
func (s *Store) FinishToken(id string, done TokenDone) error {
	if s == nil {
		return &TokenError{Kind: "InvalidToken", ID: id}
	}
	s.mu.Lock()
	tok := s.tokens[id]
	if tok == nil {
		s.mu.Unlock()
		return &TokenError{Kind: "InvalidToken", ID: id}
	}
	switch tok.Status {
	case TokenTimedOut:
		resource := tok.Resource
		s.mu.Unlock()
		return &TokenError{Kind: "TaskTimedOut", ID: id, Resource: resource}
	case TokenStarted:
		tok.Status = done.Status
		ch := tok.done
		s.mu.Unlock()
		select {
		case ch <- done:
		default:
		}
		return nil
	default:
		s.mu.Unlock()
		return &TokenError{Kind: "TaskDoesNotExist", ID: id}
	}
}

// HeartbeatToken records a heartbeat for a started token.
func (s *Store) HeartbeatToken(id string) error {
	if s == nil {
		return &TokenError{Kind: "InvalidToken", ID: id}
	}
	s.mu.Lock()
	tok := s.tokens[id]
	if tok == nil {
		s.mu.Unlock()
		return &TokenError{Kind: "InvalidToken", ID: id}
	}
	switch tok.Status {
	case TokenTimedOut:
		resource := tok.Resource
		s.mu.Unlock()
		return &TokenError{Kind: "TaskTimedOut", ID: id, Resource: resource}
	case TokenStarted:
		ch := tok.hb
		s.mu.Unlock()
		select {
		case ch <- struct{}{}:
		default:
		}
		return nil
	default:
		s.mu.Unlock()
		return &TokenError{Kind: "TaskDoesNotExist", ID: id}
	}
}

// ExpireToken marks a scheduled or started token timed out and wakes the runner.
func (s *Store) ExpireToken(id string) {
	s.settleToken(id, TokenTimedOut, TokenDone{Status: TokenTimedOut})
}

// AbortToken marks a scheduled or started token aborted and wakes the runner.
func (s *Store) AbortToken(id string) {
	s.settleToken(id, TokenAborted, TokenDone{Status: TokenAborted})
}

func (s *Store) settleToken(id, status string, done TokenDone) {
	if s == nil {
		return
	}
	s.mu.Lock()
	tok := s.tokens[id]
	if tok == nil || terminalToken(tok.Status) {
		s.mu.Unlock()
		return
	}
	tok.Status = status
	s.removeQueuedLocked(tok)
	ch := tok.done
	s.mu.Unlock()
	select {
	case ch <- done:
	default:
	}
}

func terminalToken(status string) bool {
	switch status {
	case TokenSucceeded, TokenFailed, TokenTimedOut, TokenAborted:
		return true
	default:
		return false
	}
}

func (s *Store) popWaiterLocked(arn string) (activityWaiter, bool) {
	waiters := s.activityWait[arn]
	if len(waiters) == 0 {
		return activityWaiter{}, false
	}
	w := waiters[0]
	waiters = waiters[1:]
	if len(waiters) == 0 {
		delete(s.activityWait, arn)
	} else {
		s.activityWait[arn] = waiters
	}
	return w, true
}

func (s *Store) dequeueActivityLocked(arn, worker string) *TaskToken {
	q := s.activityQ[arn]
	if len(q) == 0 {
		return nil
	}
	tok := q[0]
	q = q[1:]
	if len(q) == 0 {
		delete(s.activityQ, arn)
	} else {
		s.activityQ[arn] = q
	}
	tok.Status = TokenStarted
	tok.Worker = worker
	return tok
}

func (s *Store) removeQueuedLocked(tok *TaskToken) {
	if tok == nil || tok.ActivityARN == "" {
		return
	}
	q := s.activityQ[tok.ActivityARN]
	kept := q[:0]
	for _, item := range q {
		if item != tok {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		delete(s.activityQ, tok.ActivityARN)
		return
	}
	s.activityQ[tok.ActivityARN] = kept
}

func (s *Store) dropTokensLocked(execARN string) {
	per := s.tokensByExec[execARN]
	for id, tok := range per {
		if tok != nil && !terminalToken(tok.Status) {
			tok.Status = TokenAborted
			s.removeQueuedLocked(tok)
			select {
			case tok.done <- TokenDone{Status: TokenAborted}:
			default:
			}
		}
		delete(s.tokens, id)
	}
	delete(s.tokensByExec, execARN)
}
