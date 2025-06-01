package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
)

const (
	defaultPort  = "8070"
	dbDir        = "/app/dbdata"
	dbServiceURL = "http://db:8070/db/"
	teamName     = "wholelottago"
)

var db *datastore.Db

func waitForDB() error {
	timeout := time.After(10 * time.Second)
	tick := time.Tick(500 * time.Millisecond)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("DB service timeout")
		case <-tick:
			resp, err := http.Get(dbServiceURL + "health")
			if err == nil && resp.StatusCode == http.StatusOK {
				return nil
			}
		}
	}
}

func main() {
	var err error

	db, err = datastore.Open(dbDir)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/db/", dbHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	log.Printf("Database service started on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initDB() {
	var err error

	db, err = datastore.Open(dbDir)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	go func() {
		if err := waitForDB(); err != nil {
			log.Printf("Failed to wait for DB: %v", err)
			return
		}
	}()
}

func dbHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if !strings.HasPrefix(path, "/db/") {
		http.NotFound(w, r)
		return
	}
	key := strings.TrimPrefix(path, "/db/")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		handleGet(w, r, key)
	case http.MethodPost:
		handlePost(w, r, key)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handleGet(w http.ResponseWriter, r *http.Request, key string) {
	dataType := r.URL.Query().Get("type")
	if dataType == "" {
		dataType = "string"
	}

	var value interface{}
	var err error

	switch dataType {
	case "string":
		value, err = db.Get(key)
	case "int64":
		value, err = db.GetInt64(key)
	default:
		http.Error(w, "Invalid type parameter", http.StatusBadRequest)
		return
	}

	if err != nil {
		if err == datastore.ErrNotFound {
			w.WriteHeader(http.StatusNotFound)
		} else {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	response := struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	}{
		Key:   key,
		Value: value,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode response: %v", err)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request, key string) {
	var request struct {
		Value interface{} `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var err error
	switch v := request.Value.(type) {
	case string:
		err = db.Put(key, v)
	case float64: // JSON numbers decode as float64
		err = db.PutInt64(key, int64(v))
	default:
		http.Error(w, "Unsupported value type", http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, "Failed to store data", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
