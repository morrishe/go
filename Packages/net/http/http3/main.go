package main

import (
	"fmt"
	"net/http"
	"time"
)

type MyHandler struct {}

func (h *MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello, World!, good bye, cruel world!")
}

func main() {
	var handler MyHandler
	var server = http.Server {
		Addr:		":9000",
		Handler:	&handler,
		ReadTimeout:	2 * time.Second,
		MaxHeaderBytes:	1 << 20,
	}
	var err = server.ListenAndServe()
	if err != nil {
		fmt.Printf("http server failed, err: %v\n", err)
	}
}	
