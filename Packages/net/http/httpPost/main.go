package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
	"time"
)

type PostHandler struct {}

func (h *PostHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	var handler PostHandler
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
