package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
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
	BatchSize    = 100
	BatchTimeout = 2 * time.Second
)

func main() {
	log.Println("Starting Task 3: Graceful Shutdown Collector...")

	// Create a cancellable context based on OS signals
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	linesChan := make(chan []string, 100)
	resultsChan := make(chan Transaction, 100)
	writerDone := make(chan struct{})

	// 1. Reader Goroutine: Reads CSV and respects context cancellation
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
			select {
			case <-ctx.Done():
				log.Println("Reader received shutdown signal. Stopping read...")
				return
			default:
				record, err := reader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					log.Printf("Error reading CSV line: %v", err)
					continue
				}
				linesChan <- record
			}
		}
	}()

	// 2. Worker Pool: Processes lines and respects context
	var wg sync.WaitGroup
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range linesChan {
				// Even if context is cancelled, we process the lines already in the channel
				// to avoid losing data that was already read.
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

	// 3. Buffered Batch Writer: Handles final flush and shutdown
	go func() {
		defer close(writerDone)
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
					log.Println("Writer: All data processed, final flush completed.")
					return
				}
				buffer = append(buffer, tx)
				if len(buffer) >= BatchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-ctx.Done():
				// IMPORTANT: When context is cancelled, we must still empty the resultsChan
				// to ensure we don't lose data that workers already processed.
				log.Println("Writer received shutdown signal. Cleaning up remaining records...")
				
				// Drain the remaining results channel
				for tx := range resultsChan {
					buffer = append(buffer, tx)
					if len(buffer) >= BatchSize {
						flush()
					}
				}
				flush()
				log.Println("Writer: Graceful shutdown complete.")
				return
			}
		}
	}()

	// Wait for the writer to finish regardless of how it was triggered
	<-writerDone
	log.Println("Program exited cleanly.")
}
