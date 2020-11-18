package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
)

func postHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	r.ParseForm()
	fmt.Println(r.PostForm)
	fmt.Println(r.PostForm.Get("name"), r.PostForm.Get("age"))
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("read request.Body failed, err: %v\n", err)
		return
	}	
	fmt.Println(string(b))
	answer := `{"status": "ok"}`
	w.Write([]byte(answer))
}

func main() {
	http.HandleFunc("/", postHandler)
	err := http.ListenAndServe(":9090", nil)
	if err != nil {
		fmt.Printf("http.ListenAndServe() error: %v\n", err)
	}
}
