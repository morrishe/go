package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

func main() {
	params := url.Values{}
	params.Set("name", "tibsl")
	params.Add("name", "hexinmin")
	params.Add("name", "Morris he")
	params.Set("hobby", "fishing")
	
	rawUrl := "http://www.baidu.com"
	reqURL, err := url.ParseRequestURI(rawUrl)
	if err != nil {
		fmt.Printf("url.ParseRequestURI() error: %v\n", err)
		return
	}
	reqURL.RawQuery = params.Encode()
	fmt.Println("#####", reqURL.String(), "#####")
	resp, err := http.Get(reqURL.String())
	if err != nil {
		fmt.Printf("http.Get() error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("ioutil.ReadAll() error: %v\n", err)
		return
	}
	fmt.Println(string(body))
}
