package main

import (
	"fmt"
	"net/url"
	"time"

	"github.com/lstoll/iceshift"
)

func main() {
	url, err := url.Parse("http://icehost:9090/station")
	if err != nil {
		panic(err)
	}
	iceshift.NewDiskShifter(url, 60*time.Second)
	fmt.Println("sup")
}
