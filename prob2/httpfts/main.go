package main

import (
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jszwec/s3fs"
)

const bucket = "mugund10"

func main() {
	os.Args[0] = "s3"
	arg := os.Args[:]

	for _, path := range arg {
		var fsys fs.FS
		if path == "s3" {
			fsys = s3call()
		} else {
			fsys = os.DirFS(path)
		}
		go fsserver(path, fsys)
	}

	http.ListenAndServe(":8020", nil)
}

func fsserver(p string, fsys fs.FS) {
	path := fmt.Sprintf("/%s/", path.Base(p))
	http.Handle(path, http.StripPrefix(path, http.FileServerFS(fsys)))
}

func s3call() fs.FS {
	s, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}

	s3fsg := s3fs.New(s3.New(s), bucket)
	return s3fsg
}
