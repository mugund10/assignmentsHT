package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/mugund10/hashedtokens/proj3/kafka"
	db "github.com/mugund10/hashedtokens/proj3/redis"
)

// Define a struct for JSON data without omitempty tag

type Data struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Datas struct {
	Lis []Data
}

var mesgdata string

func handleGet(w http.ResponseWriter, r *http.Request) {
	go retriever()

	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
	if mesgdata == "" {
		Consumer("geT")
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(mesgdata); err != nil {
		http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
		return
	}
}

func retriever() {

	var ds []Data

	rdb1 := db.New("localhost:6379", "")
	datmap := rdb1.GetAll()
	for key, val := range datmap {
		d := Data{
			Key:   key,
			Value: val,
		}
		ds = append(ds, d)
	}
	dats := Datas{Lis: ds}
	jsonData, err := json.Marshal(dats)
	if err != nil {
		log.Fatalf("could not encode to JSON: %v", err)
	}
	go Producer("geT", string(jsonData))

}

func handlePost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}
	go Producer("posT", string(body))
}

func Producer(topic, msg string) {
	prod := kafka.NewProducer()
	defer prod.Close()
	prod.SendMessage(topic, msg)
}

func Consumer(topic string) {
	var datas Data
	cons := kafka.NewConsumer()
	defer cons.Close()

	if topic == "posT" {
		mesg := cons.Listen(topic)
		for msg := range mesg {
			err := json.Unmarshal([]byte(msg.Value), &datas)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			rdb := db.New("localhost:6379", "")
			rdb.Put(datas.Key, datas.Value)
		}
	} else if topic == "geT" {

		mesg, err := cons.ListenOnce(topic)
		if err != nil {
			fmt.Println(err)
			return
		}
		mesgdata = string(mesg.Value)
	}
}

func main() {
	go Consumer("posT")
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/post", handlePost)

	fmt.Println("Server is running on port 8090...")
	if err := http.ListenAndServe(":8090", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
