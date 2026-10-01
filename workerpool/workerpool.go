// Package workerpool реалізує Частину 3 домашньої роботи: пул
// воркерів з обмеженим часом на кожне завдання, що симулює
// конкурентне отримання розміру URL.
package workerpool

import (
	"context"
	"time"
)

// Job — одна одиниця роботи для пулу. Fetch отримує контекст із
// дедлайном і має сам його поважати (кооперативне скасування) —
// саме так уникають витоку горутин при тайм-ауті.
type Job struct {
	ID    string
	Fetch func(ctx context.Context) (int, error)
}

// Result — результат виконання одного Job.
type Result struct {
	JobID string
	Size  int
	Err   error
}

// RunPool запускає рівно numWorkers горутин-воркерів, які беруть
// завдання з jobs і надсилають Result у повернутий канал. Кожному
// Job надається не більше timeout часу — якщо job.Fetch не встигає,
// Result.Err міститиме помилку тайм-ауту (context.DeadlineExceeded).
//
// TODO (Завдання 3): реалізуйте цю функцію.
//   - запустіть рівно numWorkers горутин (використайте sync.WaitGroup,
//     щоб знати, коли всі вони завершили);
//   - кожен воркер у циклі `for job := range jobs` для кожного job:
//   - створює ctx, cancel := context.WithTimeout(context.Background(), timeout)
//     і викликає job.Fetch(ctx);
//   - обов'язково викликає cancel() (defer), щоб не тримати таймер;
//   - надсилає Result{JobID: job.ID, Size: size, Err: err} у results;
//   - у окремій горутині: після wg.Wait() закрийте results.
//
// Це і є той самий select + time.After / context.WithTimeout
// патерн проти витоку горутин, який ми проходили на занятті —
// різниця лише в тому, що тут скасування кооперативне: Fetch сам
// перевіряє ctx.Done() (дивіться приклад slowFetch у тестах).
func RunPool(jobs <-chan Job, numWorkers int, timeout time.Duration) <-chan Result {
	results := make(chan Result)
	// TODO: ваш код тут
	close(results)
	return results
}
