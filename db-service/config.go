package main

import (
    "context"
    "database/sql"
    "log"
    "os"
    "time"

    _ "github.com/lib/pq"
)

type DBConfig struct {
    DBName   string
    User     string
    Password string
    Host     string
    Port     string
}

func GetDBConfig() DBConfig {
    cfg := DBConfig{
        DBName:   getEnv("DB_NAME", ""),
        User:     getEnv("DB_USER", ""),
        Password: getEnv("DB_PASSWORD", ""),
        Host:     getEnv("DB_HOST", ""),
        Port:     getEnv("DB_PORT", ""),
    }
    return cfg
}

func getEnv(key, fallback string) string {
    if v, ok := os.LookupEnv(key); ok {
        return v
    }
    return fallback
}

func InitDB() (*sql.DB, error) {
    cfg := GetDBConfig()
    dsn := "postgres://" + cfg.User + ":" + cfg.Password + "@" + cfg.Host + ":" + cfg.Port + "/" + cfg.DBName + "?sslmode=disable"
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        return nil, err
    }
    // Verify connection
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := db.PingContext(ctx); err != nil {
        return nil, err
    }
    log.Println("Connected to DB")
    return db, nil
}
