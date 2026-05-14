package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
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
	BatchSize    = 100               // N: Max records before flush
	BatchTimeout = 2 * time.Second   // T: Max time before flush
)

func main() {
	log.Println("Starting Task 2: Buffered Go Collector...")

	linesChan := make(chan []string, 100)
	resultsChan := make(chan Transaction, 100)
	doneChan := make(chan struct{})
	
	// 1. Reader Goroutine
	go func() {
		defer close(linesChan)
		file, err := os.Open(InputFile)
		if err != nil {
			log.Fatalf("Failed to open input file: %v", err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
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

	// 2. Worker Pool
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

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 3. Buffered Batch Writer (Task 2 implementation)
	go func() {
		outFile, err := os.Create(OutputFile)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outFile.Close()

		var buffer []Transaction
		ticker := time.NewTicker(BatchTimeout)
		defer ticker.Stop()

		flush := func() {
			if len(buffer) == 0 {
				return
			}
			log.Printf("Flushing batch of %d records to disk...", len(buffer))
			for _, tx := range buffer {
				data, _ := json.Marshal(tx)
				outFile.Write(append(data, '\n'))
			}
			buffer = buffer[:0]
		}

		for {
			select {
			case tx, ok := <-resultsChan:
				if !ok {
					flush()
					close(doneChan)
					return
				}
				buffer = append(buffer, tx)
				if len(buffer) >= BatchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()

	<-doneChan
	log.Println("Task 2 complete. Buffered processing finished.")
}
