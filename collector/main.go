package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

// Transaction represents the bank transaction structure
type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	Timestamp     string  `json:"timestamp"`
	AccountID     string  `json:"account_id"`
	Amount        float64 `json:"amount"`
	Category      string  `json:"category"`
	Merchant      string  `json:"merchant"`
	Status        string  `json:"status"`
}

const (
	InputFile    = "../data/raw/transactions.csv"
	OutputFile   = "../data/intermediate/transactions.jsonl"
	WorkerCount  = 4
)

func main() {
	log.Println("Starting Task 1: Basic Go Collector...")

	linesChan := make(chan []string, 100)
	resultsChan := make(chan Transaction, 100)
	
	// 1. Reader Goroutine: Reads CSV and feeds the channel
	go func() {
		defer close(linesChan)
		file, err := os.Open(InputFile)
		if err != nil {
			log.Fatalf("Failed to open input file: %v", err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		// Skip header
		if _, err := reader.Read(); err != nil {
			log.Fatalf("Failed to read CSV header: %v", err)
		}

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading CSV line: %v", err)
				continue
			}
			linesChan <- record
		}
	}()

	// 2. Worker Pool: Processes CSV rows in parallel (Task 1 requirement)
	var wg sync.WaitGroup
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range linesChan {
				if len(line) < 7 {
					continue
				}
				amount, _ := strconv.ParseFloat(line[3], 64)
				resultsChan <- Transaction{
					TransactionID: line[0],
					Timestamp:     line[1],
					AccountID:     line[2],
					Amount:        amount,
					Category:      line[4],
					Merchant:      line[5],
					Status:        line[6],
				}
			}
		}()
	}

	// Closer for results channel
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 3. Writer: Saves results to JSONL file
	outFile, err := os.Create(OutputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	count := 0
	for tx := range resultsChan {
		data, _ := json.Marshal(tx)
		outFile.Write(append(data, '\n'))
		count++
	}

	log.Printf("Task 1 complete. Processed %d records into %s\n", count, OutputFile)
}
