package main

import (
	"fmt"
	"log"
	"os"

	"github.com/yashagw/kvdb/internal/bitcask"
	"github.com/yashagw/kvdb/internal/config"
)

func main() {
	// Clean up any existing data
	dbPath := "./my_database"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	// Open database
	db, err := bitcask.Open(dbPath, config.DefaultConfig())
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Put some data
	db.Put("name", []byte("Alice"))
	db.Put("age", []byte("25"))
	db.Put("city", []byte("New York"))

	// Get data
	name, _ := db.Get("name")
	age, _ := db.Get("age")
	city, _ := db.Get("city")

	fmt.Printf("Name: %s\n", name)
	fmt.Printf("Age: %s\n", age)
	fmt.Printf("City: %s\n", city)

	// List all keys
	fmt.Println("\nAll keys:")
	keys := db.Keys()
	for _, key := range keys {
		fmt.Printf("- %s\n", key)
	}

	// Delete a key
	db.Delete("age")
	fmt.Println("\nAfter deleting 'age':")
	keys = db.Keys()
	for _, key := range keys {
		fmt.Printf("- %s\n", key)
	}
}
