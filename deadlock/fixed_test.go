package deadlock

import (
	"testing"
	"time"
)

// TestRun_DoesNotDeadlock — ключовий тест Частини 2. Викликає Run() в
// окремій горутині: якщо Run() досі має дедлок (заглушка), ця
// горутина зависає НАЗАВЖДИ, але сам тест не чекає вічно — select з
// time.After ловить це за 2 секунди і провалює тест з чітким
// повідомленням, а не підвішує весь прогін CI.
func TestRun_DoesNotDeadlock(t *testing.T) {
	done := make(chan int, 1)

	go func() {
		done <- Run()
	}()

	select {
	case got := <-done:
		if got != 42 {
			t.Errorf("Run() = %d, want 42", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() не завершилась за 2с — дедлок досі не виправлено")
	}
}

// TestRun_MultipleCallsAreIndependent перевіряє, що Run() можна
// викликати кілька разів поспіль без побічних ефектів (наприклад,
// без випадково спільної змінної між викликами).
func TestRun_MultipleCallsAreIndependent(t *testing.T) {
	for i := 0; i < 3; i++ {
		done := make(chan int, 1)
		go func() { done <- Run() }()

		select {
		case got := <-done:
			if got != 42 {
				t.Errorf("виклик %d: Run() = %d, want 42", i, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("виклик %d: Run() не завершилась за 2с", i)
		}
	}
}
