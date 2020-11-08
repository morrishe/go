package main

import (
	"fmt"
)

func main() {
	scoreMap := make(map[string]int, 8)
	scoreMap["张三"] = 90
	scoreMap["小明"] = 100
	
	v, ok := scoreMap["张三丰"]
	if ok {
		fmt.Println(v)
	} else {
		fmt.Println("查无此人")
	}
}
