package workerpool

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// collect запускає пул і збирає всі результати за JobID.
func collect(t *testing.T, jobs []Job, numWorkers int, timeout time.Duration) map[string]Result {
	t.Helper()

	in := make(chan Job, len(jobs))
	for _, j := range jobs {
		in <- j
	}
	close(in)

	got := make(map[string]Result, len(jobs))
	for r := range RunPool(in, numWorkers, timeout) {
		got[r.JobID] = r
	}
	return got
}

// TestRunPool_TimeoutErrorIsDeadlineExceeded перевіряє, що повільне
// завдання завершується саме помилкою context.DeadlineExceeded, а не
// будь-якою іншою, і що Size при цьому нульовий.
func TestRunPool_TimeoutErrorIsDeadlineExceeded(t *testing.T) {
	withTimeout(t, 3*time.Second, func() {
		got := collect(t, []Job{{ID: "slow", Fetch: slowFetch(time.Second)}}, 1, 50*time.Millisecond)

		r, ok := got["slow"]
		if !ok {
			t.Fatal("результат для job \"slow\" не отримано")
		}
		if !errors.Is(r.Err, context.DeadlineExceeded) {
			t.Errorf("Err = %v, want context.DeadlineExceeded", r.Err)
		}
		if r.Size != 0 {
			t.Errorf("Size = %d, want 0 при тайм-ауті", r.Size)
		}
	})
}

// TestRunPool_FetchGetsDeadline перевіряє, що Fetch отримує контекст
// із дедлайном, який відповідає переданому timeout.
func TestRunPool_FetchGetsDeadline(t *testing.T) {
	withTimeout(t, 3*time.Second, func() {
		const timeout = 500 * time.Millisecond

		var remaining time.Duration
		var hasDeadline bool
		fetch := func(ctx context.Context) (int, error) {
			var deadline time.Time
			deadline, hasDeadline = ctx.Deadline()
			remaining = time.Until(deadline)
			return 1, nil
		}

		collect(t, []Job{{ID: "job", Fetch: fetch}}, 1, timeout)

		if !hasDeadline {
			t.Fatal("контекст, переданий у Fetch, не має дедлайну")
		}
		if remaining <= 0 || remaining > timeout {
			t.Errorf("до дедлайну лишалось %v, очікувалось у межах (0, %v]", remaining, timeout)
		}
	})
}

// TestRunPool_TimeoutIsPerJob перевіряє, що тайм-аут рахується для
// кожного завдання окремо, а не один на весь пул: 3 повільні завдання
// на одному воркері мають кожне отримати свої повні 100 мс.
func TestRunPool_TimeoutIsPerJob(t *testing.T) {
	withTimeout(t, 3*time.Second, func() {
		const timeout = 100 * time.Millisecond

		var jobs []Job
		for i := range 3 {
			jobs = append(jobs, Job{ID: fmt.Sprintf("slow-%d", i), Fetch: slowFetch(time.Second)})
		}

		start := time.Now()
		got := collect(t, jobs, 1, timeout)
		elapsed := time.Since(start)

		if len(got) != len(jobs) {
			t.Fatalf("отримано %d результатів, очікувалось %d", len(got), len(jobs))
		}
		for id, r := range got {
			if !errors.Is(r.Err, context.DeadlineExceeded) {
				t.Errorf("%s: Err = %v, want context.DeadlineExceeded", id, r.Err)
			}
		}
		// Один воркер виконує завдання послідовно: 3 × 100 мс.
		if elapsed < 3*timeout {
			t.Errorf("пул тривав %v — менше за 3 × %v, тайм-аут схоже спільний для всіх завдань", elapsed, timeout)
		}
	})
}

// TestRunPool_MixedFastAndSlow перевіряє, що тайм-аут повільного
// завдання не впливає на швидкі: вони мають завершитися успішно.
func TestRunPool_MixedFastAndSlow(t *testing.T) {
	withTimeout(t, 3*time.Second, func() {
		jobs := []Job{
			{ID: "fast-1", Fetch: slowFetch(5 * time.Millisecond)},
			{ID: "slow", Fetch: slowFetch(time.Second)},
			{ID: "fast-2", Fetch: slowFetch(5 * time.Millisecond)},
		}

		got := collect(t, jobs, 3, 100*time.Millisecond)

		for _, id := range []string{"fast-1", "fast-2"} {
			if r := got[id]; r.Err != nil || r.Size != 1234 {
				t.Errorf("%s: got {Size: %d, Err: %v}, want {Size: 1234, Err: nil}", id, r.Size, r.Err)
			}
		}
		if r := got["slow"]; !errors.Is(r.Err, context.DeadlineExceeded) {
			t.Errorf("slow: Err = %v, want context.DeadlineExceeded", r.Err)
		}
	})
}

// TestRunPool_ContextCancelledAfterFetch перевіряє, що воркер викликає
// cancel() одразу після Fetch, а не тримає таймер до кінця тайм-ауту.
// Тайм-аут навмисно великий: якщо cancel() забули, ctx.Err() буде nil.
func TestRunPool_ContextCancelledAfterFetch(t *testing.T) {
	withTimeout(t, 3*time.Second, func() {
		var captured context.Context
		fetch := func(ctx context.Context) (int, error) {
			captured = ctx
			return 1, nil
		}

		collect(t, []Job{{ID: "job", Fetch: fetch}}, 1, time.Minute)

		if captured == nil {
			t.Fatal("Fetch не викликано")
		}
		if !errors.Is(captured.Err(), context.Canceled) {
			t.Errorf("ctx.Err() після завершення = %v, want context.Canceled — cancel() не викликано", captured.Err())
		}
	})
}
