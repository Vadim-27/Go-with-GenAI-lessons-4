package pipeline

import (
	"testing"
	"time"
)

// withTimeout запускає fn в окремій горутині й провалює тест, якщо
// fn не завершилась за d. Це захищає CI від зависання назавжди,
// якщо реалізація студента має дедлок чи забуває закрити канал —
// тест просто провалиться з чітким повідомленням замість того, щоб
// тримати весь запуск CI вічно.
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
		t.Fatal("тест не завершився за відведений час — імовірний дедлок або незакритий канал")
	}
}

// TestGenerate_CountAndRange перевіряє Generator (Завдання 1.1):
// рівно n значень, кожне в діапазоні [1,100], канал закривається.
func TestGenerate_CountAndRange(t *testing.T) {
	withTimeout(t, 2*time.Second, func() {
		const n = 10
		ch := Generate(n)

		count := 0
		for v := range ch {
			if v < 1 || v > 100 {
				t.Errorf("значення %d поза діапазоном [1,100]", v)
			}
			count++
		}
		if count != n {
			t.Errorf("отримано %d значень, очікувалось %d", count, n)
		}
	})
}

// TestGenerate_ZeroCount перевіряє крайовий випадок: n == 0 має
// одразу дати закритий порожній канал, без паніки й без зависання.
func TestGenerate_ZeroCount(t *testing.T) {
	withTimeout(t, time.Second, func() {
		ch := Generate(0)
		count := 0
		for range ch {
			count++
		}
		if count != 0 {
			t.Errorf("отримано %d значень, очікувалось 0", count)
		}
	})
}

// TestFilter_KeepsOnlyEven перевіряє Filter (Завдання 1.1): лише
// парні значення проходять, у тому самому порядку, канал
// закривається після вичерпання входу.
func TestFilter_KeepsOnlyEven(t *testing.T) {
	withTimeout(t, 2*time.Second, func() {
		in := make(chan int)
		go func() {
			defer close(in)
			for _, v := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
				in <- v
			}
		}()

		out := Filter(in)

		want := []int{2, 4, 6, 8, 10}
		got := make([]int, 0, len(want))
		for v := range out {
			got = append(got, v)
		}

		if len(got) != len(want) {
			t.Fatalf("отримано %v, очікувалось %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("позиція %d: отримано %d, очікувалось %d", i, got[i], want[i])
			}
		}
	})
}

// TestFilter_EmptyInput перевіряє, що закритий одразу порожній
// вхідний канал дає закритий порожній вихідний канал.
func TestFilter_EmptyInput(t *testing.T) {
	withTimeout(t, time.Second, func() {
		in := make(chan int)
		close(in)

		out := Filter(in)
		count := 0
		for range out {
			count++
		}
		if count != 0 {
			t.Errorf("отримано %d значень, очікувалось 0", count)
		}
	})
}

// TestPipeline_EndToEnd з'єднує Generate і Filter разом (саме так,
// як це робить cmd/pipeline/main.go) і перевіряє, що результат
// містить лише парні числа з діапазону [1,100].
func TestPipeline_EndToEnd(t *testing.T) {
	withTimeout(t, 2*time.Second, func() {
		out := Filter(Generate(10))
		for v := range out {
			if v%2 != 0 {
				t.Errorf("непарне значення %d пройшло крізь Filter", v)
			}
			if v < 1 || v > 100 {
				t.Errorf("значення %d поза діапазоном [1,100]", v)
			}
		}
	})
}
