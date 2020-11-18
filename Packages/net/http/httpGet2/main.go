package main

import (
	"fmt"
	"net/http"
	"net/url"
	"io/ioutil"
)

func main() {
	apiUrl := "http://127.0.0.1:9090/get"
	data := url.Values{}
	data.Set("name", "枯藤")
	data.Set("age", "18")
	u, err := url.ParseRequestURI(apiUrl)
	if err != nil {
		fmt.Printf("parse url requestUrl failed , err: %v\n", err)
	}
	u.RawQuery = data.Encode()
	fmt.Println(u.String())
	resp, err := http.Get(u.String())
	if err != nil {
		fmt.Println("post failed, err: %v\n", err)
		return
	}	
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("get resp failed, err:%v\n", err)
		return
	}
	fmt.Println(string(b))
}	
