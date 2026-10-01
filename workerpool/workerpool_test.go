package workerpool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// withTimeout захищає тест від вічного зависання, якщо реалізація
// RunPool має дедлок чи забуває закрити канал результатів.
func withTimeout(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("тест не завершився за відведений час — імовірний дедлок або незакритий канал результатів")
	}
}

// slowFetch — гарно поведений сімулятор Fetch: сам перевіряє
// ctx.Done() і повертається достроково, якщо контекст скасовано чи
// його дедлайн вичерпано. Саме такої кооперативної поведінки
// вимагає ідіоматичний тайм-аут у Go.
func slowFetch(d time.Duration) func(ctx context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		select {
		case <-time.After(d):
			return 1234, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func TestRunPool_FastJobsSucceed(t *testing.T) {
	withTimeout(t, 3*time.Second, func() {
		jobs := make(chan Job, 5)
		for i := 0; i < 5; i++ {
			jobs <- Job{ID: fmt.Sprintf("job-%d", i), Fetch: slowFetch(10 * time.Millisecond)}
		}
		close(jobs)

		results := RunPool(jobs, 3, time.Second)

		count := 0
		for r := range results {
			if r.Err != nil {
				t.Errorf("%s: неочікувана помилка: %v", r.JobID, r.Err)
			}
			if r.Size != 1234 {
				t.Errorf("%s: Size = %d, want 1234", r.JobID, r.Size)
			}
			count++
		}
		if count != 5 {
			t.Errorf("отримано %d результатів, очікувалось 5", count)
		}
	})
}

// TestRunPool_TimeoutAbortsSlowJob перевіряє поведінковий контракт:
// пул має ПОВЕРНУТИСЯ приблизно за час тайм-ауту, а не чекати повний
// час виконання повільного завдання. Це і є справжня перевірка
// select+timeout, а не просто перевірка поля помилки.
func TestRunPool_TimeoutAbortsSlowJob(t *testing.T) {
	withTimeout(t, 3*time.Second, func() {
		jobs := make(chan Job, 1)
		jobs <- Job{ID: "slow", Fetch: slowFetch(2 * time.Second)}
		close(jobs)

		start := time.Now()
		results := RunPool(jobs, 1, 200*time.Millisecond)

		var got Result
		for r := range results {
			got = r
		}
		elapsed := time.Since(start)

		if got.Err == nil {
			t.Error("очікувалась помилка тайм-ауту, отримано nil")
		}
		if elapsed > time.Second {
			t.Errorf("пул тривав %v — мав завершитися близько тайм-ауту (200мс), а не чекати всі 2с завдання", elapsed)
		}
	})
}

// TestRunPool_RespectsWorkerCount перевіряє, що одночасно активних
// воркерів ніколи не більше numWorkers, і що з більшою кількістю
// завдань, ніж воркерів, робота все одно виконується вся.
func TestRunPool_RespectsWorkerCount(t *testing.T) {
	withTimeout(t, 5*time.Second, func() {
		const numWorkers = 3
		const numJobs = 9

		var mu sync.Mutex
		current, max := 0, 0
		track := func(ctx context.Context) (int, error) {
			mu.Lock()
			current++
			if current > max {
				max = current
			}
			mu.Unlock()

			defer func() {
				mu.Lock()
				current--
				mu.Unlock()
			}()

			select {
			case <-time.After(100 * time.Millisecond):
				return 1, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		jobs := make(chan Job, numJobs)
		for i := 0; i < numJobs; i++ {
			jobs <- Job{ID: fmt.Sprintf("job-%d", i), Fetch: track}
		}
		close(jobs)

		results := RunPool(jobs, numWorkers, time.Second)
		count := 0
		for range results {
			count++
		}

		if count != numJobs {
			t.Errorf("отримано %d результатів, очікувалось %d", count, numJobs)
		}
		if max > numWorkers {
			t.Errorf("максимум одночасних воркерів = %d, має бути <= %d", max, numWorkers)
		}
		if max < 2 {
			t.Errorf("максимум одночасних воркерів = %d — здається, завдання виконуються послідовно, а не конкурентно", max)
		}
	})
}
