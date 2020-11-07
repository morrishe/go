package main

import (
	"fmt"
	"net/http"
)

func myHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	fmt.Fprintln(w, "Method: ", r.Method)
	fmt.Fprintln(w, "URL: ", r.URL)
	for k,v := range r.Header {
		fmt.Fprintln(w, k, ": ", v)
	}
	fmt.Fprintln(w, "Body: ", r.Body)
	fmt.Fprintln(w, "RemoteAddr: ", r.RemoteAddr)
	w.Write([]byte("请求成功!!!"))
}

func main() {
	http.HandleFunc("/", myHandler)
	err := http.ListenAndServe(":9000", nil)
	if err != nil {
		fmt.Printf("http.ListenAndServe() error: %v\n", err)
	}
}	
