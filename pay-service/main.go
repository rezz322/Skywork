package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/invoice/create", func(w http.ResponseWriter, r *http.Request) {
		// Placeholder for creating an invoice
		log.Println("Creating invoice placeholder...")
		w.Write([]byte("Creating invoice placeholder"))
	})

	http.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		// Placeholder for handling callback
		log.Println("Received callback placeholder...")
		w.WriteHeader(http.StatusOK)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("PayService started on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
