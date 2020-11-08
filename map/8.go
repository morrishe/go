package main

import (
	"fmt"
)

func main() {
	var mapSlice = make([]map[string]string, 3)
	for index, value := range mapSlice {
		fmt.Printf("index: %d, value: %v\n", index, value)
	}
	fmt.Println("after init")
	mapSlice[0] = make(map[string]string, 10)
	mapSlice[0]["name"] = "一五"
	mapSlice[0]["password"] = "123456"
	mapSlice[0]["address"] = "睛乡街道"
	for index, value := range mapSlice {
		fmt.Printf("index: %d, value: %v\n", index, value)
	}
}
