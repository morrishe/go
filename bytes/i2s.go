package main

import (
	"bytes"
	"fmt"	
	"os"
	//"strings"
)


func ints2string(values []int) string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, v := range values {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%d", v)
	}
	buf.WriteByte(']')
	return buf.String()	
}


func main() {
	if len(os.Args) != 1 {
		fmt.Printf("USAGE: %s\n", os.Args[0])
		os.Exit(1)
	}

	fmt.Println(ints2string([]int{1,2,3,4,5,5,5,6,6,6}))
}
