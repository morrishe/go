package main

import (
	"fmt"
	"net/http"
)

func sayHello(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("hello, world"))
}	

func main() {
	http.HandleFunc("/", sayHello)
	err := http.ListenAndServe(":9000", nil)
	if err != nil {
		fmt.Printf("http.ListenAndServe() error: %v\n", err)
	}
}
