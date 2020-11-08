package main

import (
	"fmt"
	"time"
	"math/rand"
	"sort"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	var scoreMap = make(map[string]int, 20)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("stu%01d", i)
		value := rand.Intn(10)
		scoreMap[key] = value
	}
	var keys = make([]string, 0, 20)
	for key := range scoreMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Println(key, scoreMap[key])
	}

	for k, v := range scoreMap {
		fmt.Println(k, v)
	}
}
