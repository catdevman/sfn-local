package exec

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
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
}

// runDistributed runs a Distributed Map. Each attempt, a retry included, is a
// new Map Run. Each item, or each batch, is a child execution in the store.
// The parent records MapRunStarted and then MapRunSucceeded, MapRunFailed,
// or MapRunAborted.
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
		plan, err := r.planDistributed(ctx, arn, st, effVal, ctxObj, retries)
		if err != nil {
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
	if len(d.Reader) > 0 {
		return distPlan{}, &jsonpath.EvalError{Name: "States.ItemReaderFailed", Cause: "ItemReader is not supported by this local server yet"}
	}
	if d.Writer != nil && d.Writer.Resource != "" {
		return distPlan{}, &jsonpath.EvalError{Name: "States.ResultWriterFailed", Cause: "ResultWriter export to S3 is not supported by this local server yet"}
	}
	vars := entryVars(ctx)
	var items []any
	var err error
	if st.JSONata {
		obj, _ := ctxObj.(map[string]any)
		items, err = r.evalItems(ctx, st, root, obj, vars)
	} else {
		items, err = mapItems(st, root, ctxObj, vars)
	}
	if err != nil {
		return distPlan{}, err
	}
	var plan distPlan
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
	machine := parent.Machine()
	prefix, name := machinePrefix(machine)
	now := r.now()
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
		Started:         now,
		MaxConcurrency:  int32(plan.maxConcurrency),
		ToleranceCount:  plan.toleranceCount,
		Items:           store.MapRunCounts{Pending: totalItems, Total: totalItems},
		Executions:      store.MapRunCounts{Pending: int64(n), Total: int64(n)},
	}
	if plan.tolerancePct != nil {
		p := float32(*plan.tolerancePct)
		run.TolerancePct = &p
	}
	r.Store.PutMapRun(run)
	if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapRunStarted", MapRunARN: run.ARN}) {
		r.closeMapRun(run.ARN, store.MapRunAborted)
		return branchResult{outcome: outcomeHalt}
	}

	children := make([]store.Execution, n)
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
	for i := 0; i < n; i++ {
		for {
			cur, _ := r.Store.MapRun(run.ARN)
			mu.Lock()
			stop := failure != nil
			free := cur.MaxConcurrency <= 0 || running < int(cur.MaxConcurrency)
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
		child, handle, err := r.startChild(parent, run, d, prog, plan.inputs[i])
		if err != nil {
			mu.Lock()
			if failure == nil {
				failure = &branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: err.Error()}
			}
			mu.Unlock()
			break
		}
		size := plan.sizes[i]
		mu.Lock()
		running++
		active[child.ARN] = true
		mu.Unlock()
		r.Store.UpdateMapRun(run.ARN, func(m *store.MapRun) {
			m.Items.Pending -= size
			m.Items.Running += size
			m.Executions.Pending--
			m.Executions.Running++
		})
		wg.Add(1)
		r.Start(handle, child.ARN)
		go func(i int, childARN string, done <-chan struct{}) {
			defer wg.Done()
			<-done
			final, _ := r.Store.Execution(childARN)
			updated, _ := r.Store.UpdateMapRun(run.ARN, func(m *store.MapRun) {
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
			children[i] = final
			if failure == nil && !aborting && thresholdExceeded(updated) {
				failure = &branchResult{outcome: outcomeFail, errName: "States.ExceedToleratedFailureThreshold", cause: thresholdCause}
			}
			mu.Unlock()
			signal()
		}(i, child.ARN, handle.Done)
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
			r.abortChild(a)
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
		r.closeMapRun(run.ARN, store.MapRunAborted)
		if r.Store.Running(arn) {
			r.emit(ctx, arn, store.HistoryEvent{Type: "MapRunAborted"})
		}
		if stopRequested(ctx) {
			return branchResult{outcome: outcomeStop}
		}
		return branchResult{outcome: outcomeHalt}
	}
	if failure != nil {
		r.closeMapRun(run.ARN, store.MapRunFailed)
		if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapRunFailed", Error: failure.errName, HasError: true, Cause: failure.cause, HasCause: failure.cause != ""}) {
			return branchResult{outcome: outcomeHalt}
		}
		return *failure
	}
	r.closeMapRun(run.ARN, store.MapRunSucceeded)
	if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapRunSucceeded"}) {
		return branchResult{outcome: outcomeHalt}
	}
	out, err := distOutput(children, d.Writer)
	if err != nil {
		return failureFrom(err)
	}
	return branchResult{outcome: outcomeOK, output: out}
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

func (r *Runner) closeMapRun(arn, status string) {
	when := r.now()
	r.Store.UpdateMapRun(arn, func(m *store.MapRun) {
		if m.Status == store.MapRunRunning {
			m.Status = status
			m.Stopped = &when
		}
	})
}

// abortChild stops a running child the way StopExecution does.
func (r *Runner) abortChild(arn string) {
	when := r.now()
	r.Store.Complete(arn, store.ExecAborted, when, store.HistoryEvent{Type: "ExecutionAborted", Time: when}, "", false, "", "", false, false)
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
func (r *Runner) startChild(parent store.Execution, run store.MapRun, d *asl.DistMap, prog *asl.Program, input string) (store.Execution, *store.RunHandle, error) {
	name := newTaskID()
	express := d.ExecutionType == store.TypeExpress
	kind := ":execution:"
	if express {
		kind = ":express:"
	}
	arn := strings.Replace(run.StateMachineARN, ":stateMachine:", kind, 1) + ":" + name
	now := r.now()
	limit := 365 * 24 * time.Hour
	if express {
		limit = 300 * time.Second
	}
	if machine := time.Duration(prog.TimeoutSeconds) * time.Second; machine < limit {
		limit = machine
	}
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
		Deadline:         now.Add(limit),
		MockTest:         parent.MockTest,
		MapRunARN:        run.ARN,
		Label:            run.Label,
		ParentMachineARN: parent.Machine(),
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
		"RedriveCount":    0,
		"StartDate":       isoInstant(ex.Started),
		"StateMachineArn": ex.StateMachineARN,
		"Status":          ex.Status,
	}
	if ex.Status == store.ExecSucceeded || ex.Express {
		rec["RedriveStatus"] = "NOT_REDRIVABLE"
		rec["RedriveStatusReason"] = "Execution is " + ex.Status + " and cannot be redriven"
	} else {
		rec["RedriveStatus"] = "REDRIVABLE"
	}
	if ex.Stopped != nil {
		rec["StopDate"] = isoInstant(*ex.Stopped)
	}
	if ex.HasOutput {
		rec["Output"] = ex.Output
		rec["OutputDetails"] = map[string]any{"Included": true}
	}
	if ex.HasError {
		rec["Error"] = ex.Error
	}
	if ex.HasCause {
		rec["Cause"] = ex.Cause
	}
	return rec
}
