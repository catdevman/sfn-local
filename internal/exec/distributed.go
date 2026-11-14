package exec

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

const (
	// maxBatchBytes is the default and largest MaxInputBytesPerBatch.
	maxBatchBytes = 262144
	// thresholdCause is the Map Run failure cause. AWS's wording is not checked.
	thresholdCause = "The specified tolerated failure threshold was exceeded"
)

// distPlan is one Map Run worth of child inputs and its runtime settings.
type distPlan struct {
	inputs []string
	// sizes is how many items each child input holds. It is 1 without a batcher.
	sizes          []int64
	maxConcurrency int64
	tolerancePct   *float64
	toleranceCount *int64
	// writeBucket is set when ResultWriter exports to S3.
	writeBucket string
	writePrefix string
	// length is MapStateStarted's length: the item count, or 0 when an
	// ItemReader supplies the items, as AWS reports it.
	length int
}

// runDistributed runs a Distributed Map. Each attempt, a retry included, is a
// new Map Run. Each item, or each batch, is a child execution in the store.
// The parent records MapRunStarted, or MapRunRedriven when a parent redrive
// resumes the failed run, and then MapRunSucceeded, MapRunFailed, or
// MapRunAborted.
func (r *Runner) runDistributed(ctx context.Context, arn string, st *asl.State, input string, effVal, ctxObj any) (string, string, bool, bool) {
	retries := 0
	for {
		if r.halt(ctx, arn) {
			return "", "", false, true
		}
		if r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return "", "", false, true
		}
		setRetryCount(ctxObj, retries)
		var res branchResult
		if run, ok := r.redrivableRun(ctx, arn, st); ok {
			res = r.redriveMapRun(ctx, arn, st, run)
		} else if plan, err := r.planDistributed(ctx, arn, st, effVal, ctxObj, retries); err != nil {
			res = failureFrom(err)
		} else {
			res = r.mapRun(ctx, arn, st, plan)
		}
		switch res.outcome {
		case outcomeOK:
			return r.completeTask(ctx, arn, st, input, res.output, ctxObj)
		case outcomeHalt:
			return "", "", false, true
		case outcomeStop:
			if stopRequested(ctx) {
				r.halt(ctx, arn)
			}
			return "", "", false, true
		}
		next, output, terminal, done, again := r.retryOrFinish(ctx, arn, st, input, ctxObj, "", res, &retries)
		if again {
			continue
		}
		return next, output, terminal, done
	}
}

// planDistributed reads the items, applies ItemSelector to each, batches
// them, and resolves the numeric settings that may come from a path or an
// expression.
func (r *Runner) planDistributed(ctx context.Context, arn string, st *asl.State, root, ctxObj any, retries int) (distPlan, error) {
	d := st.Dist
	vars := entryVars(ctx)
	var items []any
	var err error
	var plan distPlan
	if d.Writer != nil && d.Writer.Resource != "" {
		if plan.writeBucket, plan.writePrefix, err = r.writerTarget(ctx, st, root, ctxObj, vars); err != nil {
			return distPlan{}, err
		}
	}
	if d.Reader != nil {
		items, err = r.readItems(ctx, st, root, ctxObj, vars)
	} else if st.JSONata {
		obj, _ := ctxObj.(map[string]any)
		items, err = r.evalItems(ctx, st, root, obj, vars)
	} else {
		items, err = mapItems(st, root, ctxObj, vars)
	}
	if err != nil {
		return distPlan{}, err
	}
	if d.Reader == nil {
		plan.length = len(items)
	}
	num := func(fixed *float64, path, expr, label string, min, max float64) (*float64, error) {
		return r.distNumber(ctx, st, fixed, path, expr, root, ctxObj, vars, label, min, max)
	}
	var maxc *float64
	if st.MaxConcurrency != nil {
		f := float64(*st.MaxConcurrency)
		maxc = &f
	}
	if maxc, err = num(maxc, d.MaxConcurrencyPath, st.MaxConcurrencyExpr, "MaxConcurrency", 0, math.MaxInt32); err != nil {
		return distPlan{}, err
	}
	if maxc != nil {
		plan.maxConcurrency = int64(*maxc)
	}
	if plan.tolerancePct, err = num(d.TolerancePct, d.TolerancePctPath, d.TolerancePctExpr, "ToleratedFailurePercentage", 0, 100); err != nil {
		return distPlan{}, err
	}
	count, err := num(intToFloat(d.ToleranceCount), d.ToleranceCountPath, d.ToleranceCountExpr, "ToleratedFailureCount", 0, math.MaxInt64)
	if err != nil {
		return distPlan{}, err
	}
	if count != nil {
		n := int64(*count)
		plan.toleranceCount = &n
	}
	inputs := make([]string, len(items))
	for i, elem := range items {
		if inputs[i], err = r.iterationInput(ctx, arn, st, root, i, elem, retries); err != nil {
			return distPlan{}, err
		}
	}
	if d.Batcher == nil {
		plan.inputs = inputs
		plan.sizes = make([]int64, len(inputs))
		for i := range plan.sizes {
			plan.sizes[i] = 1
		}
		return plan, nil
	}
	b := d.Batcher
	maxItems, err := num(intToFloat(b.MaxItems), b.MaxItemsPath, b.MaxItemsExpr, "MaxItemsPerBatch", 1, math.MaxInt64)
	if err != nil {
		return distPlan{}, err
	}
	maxBytes, err := num(intToFloat(b.MaxBytes), b.MaxBytesPath, b.MaxBytesExpr, "MaxInputBytesPerBatch", 1, maxBatchBytes)
	if err != nil {
		return distPlan{}, err
	}
	batchInput := ""
	if b.HasBatchInput {
		if batchInput, err = r.batchInput(ctx, st, root, ctxObj, vars); err != nil {
			return distPlan{}, err
		}
	}
	limitItems := int64(math.MaxInt64)
	if maxItems != nil {
		limitItems = int64(*maxItems)
	}
	limitBytes := int64(maxBatchBytes)
	if maxBytes != nil {
		limitBytes = int64(*maxBytes)
	}
	plan.inputs, plan.sizes, err = batch(inputs, batchInput, limitItems, limitBytes)
	return plan, err
}

func intToFloat(n *int64) *float64 {
	if n == nil {
		return nil
	}
	f := float64(*n)
	return &f
}

// distNumber resolves a setting from its fixed value, its JSONPath reference
// path, or its JSONata expression. nil means none was set.
func (r *Runner) distNumber(ctx context.Context, st *asl.State, fixed *float64, path, expr string, root, ctxObj any, vars map[string]any, label string, min, max float64) (*float64, error) {
	var raw any
	switch {
	case expr != "":
		val, err := r.evalExpr(ctx, expr, root, r.bindings(root, ctxObj, vars))
		if err != nil {
			return nil, err
		}
		raw = val
	case path != "":
		val, found, err := jsonpath.GetBound(path, root, ctxObj, vars)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, missingPath(path)
		}
		raw = val
	default:
		return fixed, nil
	}
	f, ok := asFloat(raw)
	if !ok || math.IsNaN(f) || f < min || f > max || (label != "ToleratedFailurePercentage" && f != math.Floor(f)) {
		cause := label + " must be a number in " + strconv.FormatFloat(min, 'f', -1, 64) + ".." + strconv.FormatFloat(max, 'f', -1, 64)
		if st.JSONata {
			return nil, &queryError{Cause: cause}
		}
		return nil, jsonpath.Runtime(cause)
	}
	return &f, nil
}

func (r *Runner) batchInput(ctx context.Context, st *asl.State, root, ctxObj any, vars map[string]any) (string, error) {
	raw := st.Dist.Batcher.BatchInput
	if st.JSONata {
		val, err := r.evalRaw(ctx, raw, root, r.bindings(root, ctxObj, vars))
		if err != nil {
			return "", err
		}
		return compactValue(val)
	}
	_, text, err := evalParameters(raw, root, ctxObj, vars)
	return text, err
}

// batch groups item inputs into {"BatchInput":...,"Items":[...]} documents of
// at most maxItems items and maxBytes bytes.
func batch(items []string, batchInput string, maxItems, maxBytes int64) ([]string, []int64, error) {
	head := `{"Items":[`
	if batchInput != "" {
		head = `{"BatchInput":` + batchInput + `,"Items":[`
	}
	const tail = `]}`
	var out []string
	var sizes []int64
	var cur []string
	size := int64(len(head) + len(tail))
	flush := func() {
		if len(cur) > 0 {
			out = append(out, head+strings.Join(cur, ",")+tail)
			sizes = append(sizes, int64(len(cur)))
		}
		cur = nil
		size = int64(len(head) + len(tail))
	}
	for i, item := range items {
		add := int64(len(item))
		if len(cur) > 0 {
			add++
		}
		if len(cur) > 0 && (int64(len(cur)) >= maxItems || size+add > maxBytes) {
			flush()
			add = int64(len(item))
		}
		if size+add > maxBytes {
			return nil, nil, jsonpath.Runtime("item " + strconv.Itoa(i) + " does not fit in MaxInputBytesPerBatch")
		}
		cur = append(cur, item)
		size += add
	}
	flush()
	return out, sizes, nil
}

// mapRun starts one Map Run and waits for it.
func (r *Runner) mapRun(ctx context.Context, arn string, st *asl.State, plan distPlan) branchResult {
	parent, ok := r.Store.Execution(arn)
	if !ok {
		return branchResult{outcome: outcomeHalt}
	}
	d := st.Dist
	prog, err := asl.ValidateType(d.Definition, d.ExecutionType)
	if err != nil {
		return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: err.Error()}
	}
	label := d.Label
	if label == "" {
		label = newTaskID()
	}
	prefix, name := machinePrefix(parent.Machine())
	n := len(plan.inputs)
	var totalItems int64
	for _, s := range plan.sizes {
		totalItems += s
	}
	run := store.MapRun{
		ARN:             prefix + "mapRun:" + name + "/" + label + ":" + newTaskID(),
		ExecutionARN:    arn,
		StateMachineARN: prefix + "stateMachine:" + name + "/" + label,
		Label:           label,
		Status:          store.MapRunRunning,
		Started:         r.now(),
		MaxConcurrency:  int32(plan.maxConcurrency),
		ToleranceCount:  plan.toleranceCount,
		Items:           store.MapRunCounts{Pending: totalItems, Total: totalItems},
		Executions:      store.MapRunCounts{Pending: int64(n), Total: int64(n)},
		State:           st.Name,
		Inputs:          plan.inputs,
		Sizes:           plan.sizes,
		Children:        make([]string, n),
		WriteBucket:     plan.writeBucket,
		WritePrefix:     plan.writePrefix,
	}
	if plan.tolerancePct != nil {
		p := float32(*plan.tolerancePct)
		run.TolerancePct = &p
	}
	started, ok := r.emitID(ctx, arn, store.HistoryEvent{Type: "MapStateStarted", Length: int32(plan.length), HasLength: true})
	if !ok {
		return branchResult{outcome: outcomeHalt}
	}
	run.StartedEventID = started
	r.Store.PutMapRun(run)
	runEvent, ok := r.emitID(ctx, arn, store.HistoryEvent{Type: "MapRunStarted", MapRunARN: run.ARN})
	if !ok {
		r.closeMapRun(run.ARN, store.MapRunAborted)
		return branchResult{outcome: outcomeHalt}
	}
	r.Store.UpdateMapRun(run.ARN, func(m *store.MapRun) { m.RunEventID = runEvent })
	todo := make([]int, n)
	for i := range todo {
		todo[i] = i
	}
	return r.driveMapRun(ctx, parent, st, prog, run.ARN, todo)
}

// redrivableRun is the Map Run a parent redrive should resume for st. It is
// the run started by the state's latest entry before the redrive, when that
// run FAILED or was ABORTED. Only the state the redrive resumed in place, or
// the first entry of a nested one after the redrive, resumes it. A retry or
// a later entry starts a new run.
func (r *Runner) redrivableRun(ctx context.Context, arn string, st *asl.State) (store.MapRun, bool) {
	resumed := false
	if m, _ := ctx.Value(resumeKey{}).(*resumeMark); m != nil && m.inPlace && m.name == st.Name && !m.mapUsed {
		m.mapUsed = true
		resumed = true
	}
	ex, ok := r.Store.Execution(arn)
	if !ok || ex.RedriveCount == 0 {
		return store.MapRun{}, false
	}
	h := ex.History
	redriven := -1
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].Type == "ExecutionRedriven" {
			redriven = i
			break
		}
	}
	if redriven < 0 {
		return store.MapRun{}, false
	}
	ownRun := func(ev store.HistoryEvent) (store.MapRun, bool) {
		if ev.Type != "MapRunStarted" && ev.Type != "MapRunRedriven" {
			return store.MapRun{}, false
		}
		run, ok := r.Store.MapRun(ev.MapRunARN)
		return run, ok && run.State == st.Name
	}
	entries := 0
	for _, ev := range h[redriven+1:] {
		if ev.Type == "MapStateEntered" && ev.Name == st.Name {
			entries++
		}
		if _, mine := ownRun(ev); mine {
			return store.MapRun{}, false
		}
	}
	if !resumed && entries != 1 {
		return store.MapRun{}, false
	}
	for i := redriven - 1; i >= 0; i-- {
		if h[i].Type == "MapStateEntered" && h[i].Name == st.Name {
			return store.MapRun{}, false
		}
		if run, mine := ownRun(h[i]); mine {
			return run, run.Status == store.MapRunFailed || run.Status == store.MapRunAborted
		}
	}
	return store.MapRun{}, false
}

// redriveMapRun resumes a FAILED or ABORTED Map Run with the same items.
// Succeeded children are kept. The others, and items that never started,
// run again. The run's failure counts start over.
func (r *Runner) redriveMapRun(ctx context.Context, arn string, st *asl.State, run store.MapRun) branchResult {
	parent, ok := r.Store.Execution(arn)
	if !ok {
		return branchResult{outcome: outcomeHalt}
	}
	prog, err := asl.ValidateType(st.Dist.Definition, st.Dist.ExecutionType)
	if err != nil {
		return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: err.Error()}
	}
	var todo []int
	succeeded := map[int]bool{}
	for i, child := range run.Children {
		if child != "" {
			if ex, ok := r.Store.Execution(child); ok && ex.Status == store.ExecSucceeded {
				succeeded[i] = true
				continue
			}
		}
		todo = append(todo, i)
	}
	when := r.now()
	run, _ = r.Store.UpdateMapRun(run.ARN, func(m *store.MapRun) {
		m.Status = store.MapRunRunning
		m.Stopped = nil
		m.RedriveCount++
		m.RedriveDate = &when
		items := store.MapRunCounts{Total: m.Items.Total}
		execs := store.MapRunCounts{Total: m.Executions.Total}
		for i, child := range m.Children {
			switch {
			case succeeded[i]:
				items.Succeeded += m.Sizes[i]
				execs.Succeeded++
			case child == "":
				items.Pending += m.Sizes[i]
				execs.Pending++
			default:
				items.PendingRedrive += m.Sizes[i]
				execs.PendingRedrive++
			}
		}
		m.Items, m.Executions = items, execs
	})
	// MapRunRedriven follows the run's MapStateStarted, as on AWS.
	runEvent, ok := r.emitID(ctx, arn, store.HistoryEvent{Type: "MapRunRedriven", MapRunARN: run.ARN, RedriveCount: run.RedriveCount, HasRedrive: true,
		Previous: run.StartedEventID, HasPrevious: true})
	if !ok {
		r.closeMapRun(run.ARN, store.MapRunAborted)
		return branchResult{outcome: outcomeHalt}
	}
	r.Store.UpdateMapRun(run.ARN, func(m *store.MapRun) { m.RunEventID = runEvent })
	return r.driveMapRun(ctx, parent, st, prog, run.ARN, todo)
}

// driveMapRun runs the children for the todo indexes and waits for them.
func (r *Runner) driveMapRun(ctx context.Context, parent store.Execution, st *asl.State, prog *asl.Program, runARN string, todo []int) branchResult {
	arn := parent.ARN
	d := st.Dist
	var mu sync.Mutex
	running := 0
	active := map[string]bool{}
	var failure *branchResult
	aborting := false
	wake := make(chan struct{}, 1)
	signal := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	var wg sync.WaitGroup
dispatch:
	for _, i := range todo {
		var run store.MapRun
		for {
			run, _ = r.Store.MapRun(runARN)
			mu.Lock()
			stop := failure != nil
			free := run.MaxConcurrency <= 0 || running < int(run.MaxConcurrency)
			mu.Unlock()
			if stop || ctx.Err() != nil || stopRequested(ctx) || !r.Store.Running(arn) {
				break dispatch
			}
			if free {
				break
			}
			select {
			case <-wake:
			case <-ctx.Done():
			case <-time.After(syncPoll):
			}
		}
		child, handle, redriven, err := r.launchChild(parent, run, d, prog, i)
		if err != nil {
			mu.Lock()
			if failure == nil {
				failure = &branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: err.Error()}
			}
			mu.Unlock()
			break
		}
		size := run.Sizes[i]
		mu.Lock()
		running++
		active[child.ARN] = true
		mu.Unlock()
		r.Store.UpdateMapRun(runARN, func(m *store.MapRun) {
			m.Children[i] = child.ARN
			if redriven {
				m.Items.PendingRedrive -= size
				m.Executions.PendingRedrive--
			} else {
				m.Items.Pending -= size
				m.Executions.Pending--
			}
			m.Items.Running += size
			m.Executions.Running++
		})
		wg.Add(1)
		r.Start(handle, child.ARN)
		go func(childARN string, done <-chan struct{}) {
			defer wg.Done()
			<-done
			final, _ := r.Store.Execution(childARN)
			updated, _ := r.Store.UpdateMapRun(runARN, func(m *store.MapRun) {
				m.Items.Running -= size
				m.Executions.Running--
				switch final.Status {
				case store.ExecSucceeded:
					m.Items.Succeeded += size
					m.Executions.Succeeded++
				case store.ExecTimedOut:
					m.Items.TimedOut += size
					m.Executions.TimedOut++
				case store.ExecAborted:
					m.Items.Aborted += size
					m.Executions.Aborted++
				default:
					m.Items.Failed += size
					m.Executions.Failed++
				}
			})
			mu.Lock()
			running--
			delete(active, childARN)
			if failure == nil && !aborting && thresholdExceeded(updated) {
				failure = &branchResult{outcome: outcomeFail, errName: "States.ExceedToleratedFailureThreshold", cause: thresholdCause}
			}
			mu.Unlock()
			signal()
		}(child.ARN, handle.Done)
	}

	// Wait for the children, stopping them early if the run failed or the
	// parent stopped.
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	for {
		mu.Lock()
		stopNow := (failure != nil || ctx.Err() != nil || stopRequested(ctx) || !r.Store.Running(arn)) && !aborting
		var open []string
		if stopNow {
			aborting = true
			for a := range active {
				open = append(open, a)
			}
		}
		mu.Unlock()
		for _, a := range open {
			r.abortChild(a, runARN)
		}
		select {
		case <-finished:
		case <-wake:
			continue
		case <-ctx.Done():
			continue
		case <-time.After(syncPoll):
			continue
		}
		break
	}

	if !r.Store.Running(arn) || ctx.Err() != nil || stopRequested(ctx) {
		r.closeMapRun(runARN, store.MapRunAborted)
		if r.Store.Running(arn) {
			r.emit(ctx, arn, store.HistoryEvent{Type: "MapRunAborted"})
		}
		if stopRequested(ctx) {
			return branchResult{outcome: outcomeStop}
		}
		return branchResult{outcome: outcomeHalt}
	}
	run, _ := r.Store.MapRun(runARN)
	children := make([]store.Execution, len(run.Children))
	for i, child := range run.Children {
		if child != "" {
			children[i], _ = r.Store.Execution(child)
		}
	}
	if failure != nil {
		if run.WriteBucket != "" {
			// The failed and pending records are still exported. A write
			// error does not replace the threshold error.
			_, _ = r.writeResults(ctx, run, children, d.Writer)
		}
		r.closeMapRun(runARN, store.MapRunFailed)
		if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapRunFailed", Error: failure.errName, HasError: true, Cause: failure.cause, HasCause: failure.cause != ""}) {
			return branchResult{outcome: outcomeHalt}
		}
		if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapStateFailed"}) {
			return branchResult{outcome: outcomeHalt}
		}
		return *failure
	}
	r.closeMapRun(runARN, store.MapRunSucceeded)
	if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapRunSucceeded"}) {
		return branchResult{outcome: outcomeHalt}
	}
	if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapStateSucceeded"}) {
		return branchResult{outcome: outcomeHalt}
	}
	// MapStateExited follows the run's start event, as on AWS.
	r.Store.HintPrevious(arn, run.RunEventID, "MapStateExited")
	if run.WriteBucket != "" {
		out, err := r.writeResults(ctx, run, children, d.Writer)
		if err != nil {
			return failureFrom(err)
		}
		return branchResult{outcome: outcomeOK, output: out}
	}
	out, err := distOutput(children, d.Writer)
	if err != nil {
		return failureFrom(err)
	}
	return branchResult{outcome: outcomeOK, output: out}
}

// launchChild starts the child for input i. An unsuccessful STANDARD child
// is redriven in place. An EXPRESS child, or one with no state to redrive,
// is replaced by a new execution with the same input. redriven reports that
// the input had a child before.
func (r *Runner) launchChild(parent store.Execution, run store.MapRun, d *asl.DistMap, prog *asl.Program, i int) (store.Execution, *store.RunHandle, bool, error) {
	prev := run.Children[i]
	if prev != "" {
		now := r.now()
		ex, handle, err := r.Store.BeginRedrive(prev, now, "", now.Add(childLimit(prog, false)))
		if err == nil && handle != nil {
			return ex, handle, true, nil
		}
	}
	ex, handle, err := r.startChild(parent, run, d, prog, run.Inputs[i], run.Sizes[i])
	return ex, handle, prev != "", err
}

// childLimit is a child's execution timeout: the type's limit or the
// definition's TimeoutSeconds, whichever is shorter.
func childLimit(prog *asl.Program, express bool) time.Duration {
	limit := 365 * 24 * time.Hour
	if express {
		limit = 300 * time.Second
	}
	if machine := time.Duration(prog.TimeoutSeconds) * time.Second; machine < limit {
		limit = machine
	}
	return limit
}

// thresholdExceeded applies the tolerated failure settings. Failed and timed
// out items count. Without either setting, one failed item fails the run.
func thresholdExceeded(m store.MapRun) bool {
	failed := m.Items.Failed + m.Items.TimedOut
	if failed == 0 {
		return false
	}
	if m.TolerancePct == nil && m.ToleranceCount == nil {
		return true
	}
	if m.TolerancePct != nil && float64(failed)*100 > float64(*m.TolerancePct)*float64(m.Items.Total) {
		return true
	}
	return m.ToleranceCount != nil && failed > *m.ToleranceCount
}

// closeMapRun sets a final status. A run that succeeds or fails counts every
// item as written, pending ones included, the way AWS reports resultsWritten
// with or without a ResultWriter.
func (r *Runner) closeMapRun(arn, status string) {
	when := r.now()
	r.Store.UpdateMapRun(arn, func(m *store.MapRun) {
		if m.Status == store.MapRunRunning {
			m.Status = status
			m.Stopped = &when
			if status != store.MapRunAborted {
				m.Items.ResultsWritten = m.Items.Total
				m.Executions.ResultsWritten = m.Executions.Total
			}
		}
	})
}

// abortChild stops a running child of a failed or stopped Map Run, with the
// cause AWS gives. The event points at 0, as on AWS.
func (r *Runner) abortChild(arn, runARN string) {
	when := r.now()
	cause := "The map run execution [" + runARN + "] which was managing this resource was aborted"
	ev := store.HistoryEvent{Type: "ExecutionAborted", Time: when, Cause: cause, HasCause: true, HasPrevious: true}
	r.Store.Complete(arn, store.ExecAborted, when, ev, "", false, "", cause, false, true)
	r.Store.Cancel(arn)
}

// machinePrefix splits arn:aws:states:<region>:<account>:stateMachine:<name>
// into "arn:aws:states:<region>:<account>:" and the name.
func machinePrefix(machineARN string) (string, string) {
	const mark = "stateMachine:"
	i := strings.LastIndex(machineARN, ":"+mark)
	if i < 0 {
		return machineARN + ":", ""
	}
	return machineARN[:i+1], machineARN[i+1+len(mark):]
}

// startChild records a RUNNING child execution and its ExecutionStarted event.
func (r *Runner) startChild(parent store.Execution, run store.MapRun, d *asl.DistMap, prog *asl.Program, input string, items int64) (store.Execution, *store.RunHandle, error) {
	name := newTaskID()
	express := d.ExecutionType == store.TypeExpress
	kind := ":execution:"
	if express {
		kind = ":express:"
	}
	arn := strings.Replace(run.StateMachineARN, ":stateMachine:", kind, 1) + ":" + name
	now := r.now()
	ex := store.Execution{
		ARN:              arn,
		Name:             name,
		StateMachineARN:  run.StateMachineARN,
		Definition:       d.Definition,
		RoleArn:          parent.RoleArn,
		Input:            input,
		Status:           store.ExecRunning,
		Type:             d.ExecutionType,
		Express:          express,
		Started:          now,
		Deadline:         now.Add(childLimit(prog, express)),
		MockTest:         parent.MockTest,
		MapRunARN:        run.ARN,
		Label:            run.Label,
		ParentMachineARN: parent.Machine(),
		ItemCount:        int32(items),
	}
	stored, handle, existed := r.Store.PutExecution(ex)
	if existed {
		return store.Execution{}, nil, errors.New("child execution already exists: " + arn)
	}
	ev := store.HistoryEvent{Type: "ExecutionStarted", Time: now, Input: input, HasInput: true}
	if parent.RoleArn != "" {
		ev.RoleArn = parent.RoleArn
		ev.HasRole = true
	}
	if err := r.Store.Append(stored.ARN, ev); err != nil {
		r.Store.Drop(stored.ARN)
		handle.Finish()
		return store.Execution{}, nil, err
	}
	return stored, handle, nil
}

// distOutput builds the state result from the children in item order. A
// succeeded child contributes its output, flattened for FLATTEN. A failed
// child that was tolerated, and every child under NONE, contributes its
// execution record.
func distOutput(children []store.Execution, w *asl.ResultWriter) (string, error) {
	transform := "COMPACT"
	if w != nil && w.Transformation != "" {
		transform = w.Transformation
	}
	out := make([]any, 0, len(children))
	for _, ex := range children {
		if ex.Status != store.ExecSucceeded || transform == "NONE" {
			out = append(out, childRecord(ex))
			continue
		}
		val, err := parseJSON(ex.Output)
		if err != nil {
			return "", jsonpath.Runtime("child execution output is not JSON")
		}
		if arr, ok := val.([]any); ok && transform == "FLATTEN" {
			out = append(out, arr...)
			continue
		}
		out = append(out, val)
	}
	text, err := compactValue(out)
	if err != nil {
		return "", err
	}
	if len(text) > maxPayload {
		return "", jsonpath.DataLimit("The Map Run result exceeded the maximum size of 262144 bytes.")
	}
	return text, nil
}

// childRecord is the execution record ResultWriter's NONE transformation
// writes for one child.
func childRecord(ex store.Execution) map[string]any {
	rec := map[string]any{
		"ExecutionArn":    ex.ARN,
		"Input":           ex.Input,
		"InputDetails":    map[string]any{"Included": true},
		"Name":            ex.Name,
		"RedriveCount":    ex.RedriveCount,
		"StartDate":       isoInstant(ex.Started),
		"StateMachineArn": ex.StateMachineARN,
		"Status":          ex.Status,
	}
	status, reason := store.RedriveStatus(ex)
	rec["RedriveStatus"] = status
	if reason != "" {
		rec["RedriveStatusReason"] = reason
	}
	if ex.Stopped != nil {
		rec["StopDate"] = isoInstant(*ex.Stopped)
	}
	// AWS writes OutputDetails on every record, a failed one included.
	rec["OutputDetails"] = map[string]any{"Included": true}
	if ex.HasOutput {
		rec["Output"] = ex.Output
	}
	if ex.HasError {
		rec["Error"] = ex.Error
	}
	if ex.HasCause {
		rec["Cause"] = ex.Cause
	}
	return rec
}

// writerTarget evaluates ResultWriter Parameters (Arguments for JSONata)
// to a bucket and an optional prefix.
func (r *Runner) writerTarget(ctx context.Context, st *asl.State, root, ctxObj any, vars map[string]any) (string, string, error) {
	var val any
	var err error
	if st.JSONata {
		val, err = r.evalRaw(ctx, st.Dist.Writer.Parameters, root, r.bindings(root, ctxObj, vars))
	} else {
		val, _, err = evalParameters(st.Dist.Writer.Parameters, root, ctxObj, vars)
	}
	if err != nil {
		return "", "", err
	}
	obj, _ := val.(map[string]any)
	bucket, _ := obj["Bucket"].(string)
	prefix, _ := obj["Prefix"].(string)
	if bucket == "" {
		return "", "", &jsonpath.EvalError{Name: "States.ResultWriterFailed", Cause: "ResultWriter needs a Bucket"}
	}
	for key := range obj {
		if key != "Bucket" && key != "Prefix" {
			return "", "", &jsonpath.EvalError{Name: "States.ResultWriterFailed", Cause: "ResultWriter does not take " + key}
		}
	}
	return bucket, strings.Trim(prefix, "/"), nil
}

// writeResults exports a Map Run to <prefix>/<run id>/: SUCCEEDED_0, FAILED_0
// (failed, timed out, and aborted children), and PENDING_0 (items that never
// started), each written only when it has entries, then manifest.json. The
// state result points at the manifest.
func (r *Runner) writeResults(ctx context.Context, run store.MapRun, children []store.Execution, w *asl.ResultWriter) (string, error) {
	transform := "NONE"
	if w.Transformation != "" {
		transform = w.Transformation
	}
	var succeeded, failed, pending []any
	for i, ex := range children {
		switch {
		case ex.ARN == "":
			pending = append(pending, map[string]any{"Input": run.Inputs[i], "Status": "PENDING"})
			continue
		case ex.Status != store.ExecSucceeded:
			failed = append(failed, childRecord(ex))
		case transform == "NONE":
			succeeded = append(succeeded, childRecord(ex))
		default:
			val, err := parseJSON(ex.Output)
			if err != nil {
				return "", &jsonpath.EvalError{Name: "States.ResultWriterFailed", Cause: "child execution output is not JSON"}
			}
			if arr, ok := val.([]any); ok && transform == "FLATTEN" {
				succeeded = append(succeeded, arr...)
			} else {
				succeeded = append(succeeded, val)
			}
		}
	}
	runID := run.ARN[strings.LastIndex(run.ARN, ":")+1:]
	dir := runID
	if run.WritePrefix != "" {
		dir = run.WritePrefix + "/" + runID
	}
	ext := ".json"
	if w.OutputType == "JSONL" {
		ext = ".jsonl"
	}
	files := map[string][]any{}
	for _, group := range []struct {
		name    string
		entries []any
	}{{"SUCCEEDED", succeeded}, {"FAILED", failed}, {"PENDING", pending}} {
		files[group.name] = []any{}
		if len(group.entries) == 0 {
			continue
		}
		body, err := encodeResults(group.entries, w.OutputType == "JSONL")
		if err != nil {
			return "", err
		}
		key := dir + "/" + group.name + "_0" + ext
		if err := r.SDK.S3Write(ctx, run.WriteBucket, key, body); err != nil {
			return "", writerFailed(err)
		}
		files[group.name] = []any{map[string]any{"Key": key, "Size": len(body)}}
	}
	manifest := map[string]any{"DestinationBucket": run.WriteBucket, "MapRunArn": run.ARN, "ResultFiles": files}
	body, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	manifestKey := dir + "/manifest.json"
	if err := r.SDK.S3Write(ctx, run.WriteBucket, manifestKey, body); err != nil {
		return "", writerFailed(err)
	}
	return compactValue(map[string]any{
		"MapRunArn":           run.ARN,
		"ResultWriterDetails": map[string]any{"Bucket": run.WriteBucket, "Key": manifestKey},
	})
}

func encodeResults(entries []any, lines bool) ([]byte, error) {
	if !lines {
		text, err := compactValue(entries)
		return []byte(text), err
	}
	var b strings.Builder
	for _, e := range entries {
		text, err := compactValue(e)
		if err != nil {
			return nil, err
		}
		b.WriteString(text)
		b.WriteByte('\n')
	}
	return []byte(b.String()), nil
}

func writerFailed(err error) error {
	cause := err.Error()
	var se *sdk.ServiceError
	if errors.As(err, &se) {
		cause = se.Name + ": " + se.Cause
	}
	return &jsonpath.EvalError{Name: "States.ResultWriterFailed", Cause: cause}
}
