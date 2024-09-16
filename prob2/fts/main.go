package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jszwec/s3fs"
)

const bucket = "mugund10"

func main() {
	start := time.Now()
	defer func() {
		fmt.Printf("timetaken : %v \n", time.Since(start))
	}()
	os.Args[0] = "s3"
	dirs := os.Args[:]
	pathc := make(chan []string, len(dirs))
	for _, dir := range dirs {
		var fsys fs.FS
		if dir == "s3" {
			fsys = s3call()
		} else {
			fsys = os.DirFS(dir)
		}
		go Files(fsys, pathc)
		fmt.Println(dir)
		fmt.Println("")
		fmt.Println(<-pathc)
		fmt.Println("")

	}

}

func Files(fsys fs.FS, path chan []string) {
	var paths []string
	fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		paths = append(paths, p)
		return nil
	})
	path <- paths
}

func s3call() fs.FS {
	s, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}

	s3fsg := s3fs.New(s3.New(s), bucket)
	return s3fsg
}
