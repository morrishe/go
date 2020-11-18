package main

import (
	"fmt"
	"net/http"
)

func myHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data := r.URL.Query()
	fmt.Fprintln(w, data.Get("name"))
	fmt.Fprintln(w, data.Get("age"))
	answer := `{"status": "OK"}`
	w.Write([]byte(answer))
}

func main() {
	http.HandleFunc("/", myHandler)
	err := http.ListenAndServe(":9090", nil)
	if err != nil {
		fmt.Printf("http.ListenAndServe() error: %v\n", err)
	}
}
