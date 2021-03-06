package main

import (
	"fmt"
	"time"

	"github.com/iotopo/batch"
)

func main() {
	writer := batch.NewBatchWriter(10, 1 * time.Second, func (items []int)  {
		fmt.Println("need to write:", len(items))
	})

	for i := 0; i < 100; i++ {
		writer.AppendData(i)
		time.Sleep(100 * time.Millisecond)
	}
}