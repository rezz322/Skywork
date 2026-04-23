package main

import (
    "context"
    "log"
    "time"

    _ "github.com/lib/pq"
)

func main() {
    // Initialize DB connection
    db, err := InitDB()
    if err != nil {
        log.Fatalf("failed to connect to DB: %v", err)
    }
    defer db.Close()

    // Placeholder for search operation
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Example usage of a query defined in queries.go
    rows, err := db.QueryContext(ctx, FindPersonByName, "John Doe")
    if err != nil {
        log.Printf("query error: %v", err)
        return
    }
    defer rows.Close()

    for rows.Next() {
        var id int
        var name, source, birthDate, snippet string
        if err := rows.Scan(&id, &name, &source, &birthDate, &snippet); err != nil {
            log.Printf("scan error: %v", err)
            continue
        }
        log.Printf("Result: %d %s %s %s %s", id, name, source, birthDate, snippet)
    }
    if err := rows.Err(); err != nil {
        log.Printf("rows error: %v", err)
    }

    log.Println("DB Service finished")
}
