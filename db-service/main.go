package main

import (
    "log"

    _ "github.com/lib/pq"
)

func main() {
    // Initialize DB connection
    db, err := InitDB()
    if err != nil {
        log.Fatalf("failed to connect to DB: %v", err)
    }
    defer db.Close()

    log.Println("DB Service started. Waiting for tasks...")
    
    // Placeholder for future logic
    select {}
}
