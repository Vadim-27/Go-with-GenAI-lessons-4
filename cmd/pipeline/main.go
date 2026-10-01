// Команда pipeline з'єднує Generate і Filter та виводить результат —
// саме той "головний горутин", що читає з фінального каналу і
// друкує результати (Завдання 1.1).
//
// Запустіть з детектором гонок, як вимагає завдання:
//
//	go run -race ./cmd/pipeline
package main

import (
	"fmt"

	"example.com/lesson04/pipeline"
)

func main() {
	numbers := pipeline.Generate(10)
	evens := pipeline.Filter(numbers)

	for v := range evens {
		fmt.Println(v)
	}
}
