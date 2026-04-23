package main

import "fmt"

// SQL query constants
const (
    // FindPersonByName searches for persons by the provided name.
    FindPersonByName = "SELECT id, name, source, birth_date, snippet FROM public_search_results WHERE query_name = $1"

    // InsertPerson inserts a new person record into the database.
    InsertPerson = "INSERT INTO public_search_results (query_name, source, found_name, birth_date, snippet) VALUES ($1, $2, $3, $4, $5)"

    // DeleteByQuery removes all records for a specific query.
    DeleteByQuery = "DELETE FROM public_search_results WHERE query_name = $1"
)

// Helper to format queries (optional)
func formatQuery(query string, args ...interface{}) string {
    return fmt.Sprintf(query, args...)
}
