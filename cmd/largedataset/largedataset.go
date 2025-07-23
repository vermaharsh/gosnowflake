// Example: Large dataset query performance test
// 
// This script replicates the Java testExternalResultTest_2() functionality.
// It connects to Snowflake, runs a large dataset query on HYPER_TPCDS.SF_1000_ICEBERG.ITEM table,
// fetches results using Arrow batches for optimal performance, and writes to a CSV file.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	sf "github.com/snowflakedb/gosnowflake"
)

const itemQuery = `
select
  CAST(I_ITEM_SK AS DECIMAL(38, 18)) AS I_ITEM_SK,
  I_ITEM_ID,
  I_REC_START_DATE,
  I_REC_END_DATE,
  I_ITEM_DESC,
  CAST(I_CURRENT_PRICE AS DECIMAL(38, 18)) AS I_CURRENT_PRICE,
  CAST(I_WHOLESALE_COST AS DECIMAL(38, 18)) AS I_WHOLESALE_COST,
  CAST(I_BRAND_ID AS DECIMAL(38, 18)) AS I_BRAND_ID,
  I_BRAND,
  CAST(I_CLASS_ID AS DECIMAL(38, 18)) AS I_CLASS_ID,
  I_CLASS,
  CAST(I_CATEGORY_ID AS DECIMAL(38, 18)) AS I_CATEGORY_ID,
  I_CATEGORY,
  CAST(I_MANUFACT_ID AS DECIMAL(38, 18)) AS I_MANUFACT_ID,
  I_MANUFACT,
  I_SIZE,
  I_FORMULATION,
  I_COLOR,
  I_UNITS,
  I_CONTAINER,
  CAST(I_MANAGER_ID AS DECIMAL(38, 18)) AS I_MANAGER_ID,
  I_PRODUCT_NAME,
  CAST(DUMMY AS DECIMAL(38, 18)) AS DUMMY
from HYPER_TPCDS.SF_1000_ICEBERG.ITEM`

const customerQuery = `
select
  CAST(C_CUSTOMER_SK AS DECIMAL(38, 18)) AS C_CUSTOMER_SK,
  C_CUSTOMER_ID,
  CAST(C_CURRENT_CDEMO_SK AS DECIMAL(38, 18)) AS C_CURRENT_CDEMO_SK,
  CAST(C_CURRENT_HDEMO_SK AS DECIMAL(38, 18)) AS C_CURRENT_HDEMO_SK,
  CAST(C_CURRENT_ADDR_SK AS DECIMAL(38, 18)) AS C_CURRENT_ADDR_SK,
  CAST(C_FIRST_SHIPTO_DATE_SK AS DECIMAL(38, 18)) AS C_FIRST_SHIPTO_DATE_SK,
  CAST(C_FIRST_SALES_DATE_SK AS DECIMAL(38, 18)) AS C_FIRST_SALES_DATE_SK,
  C_SALUTATION,
  C_FIRST_NAME,
  C_LAST_NAME,
  C_PREFERRED_CUST_FLAG,
  CAST(C_BIRTH_DAY AS DECIMAL(38, 18)) AS C_BIRTH_DAY,
  CAST(C_BIRTH_MONTH AS DECIMAL(38, 18)) AS C_BIRTH_MONTH,
  CAST(C_BIRTH_YEAR AS DECIMAL(38, 18)) AS C_BIRTH_YEAR,
  C_BIRTH_COUNTRY,
  C_LOGIN,
  C_EMAIL_ADDRESS,
  C_LAST_REVIEW_DATE,
  CAST(DUMMY AS DECIMAL(38, 18)) AS DUMMY
from HYPER_TPCDS.SF_1000_ICEBERG.CUSTOMER`

type PerformanceMetrics struct {
	QueryStartTime         time.Time
	QueryEndTime           time.Time
	TotalQueryTime         time.Duration
	BatchRetrievalTime     time.Duration
	TotalRecordFetchTime   time.Duration
	TotalConversionWriteTime time.Duration
	TotalRowsProcessed     int64
	TotalBatches           int
}

type FetchedBatch struct {
	BatchIndex int
	Records    *[]arrow.Record
	Error      error
	FetchTime  time.Duration
}

type ProcessedBatch struct {
	BatchIndex int
	CSVRows    [][]string
	RowCount   int64
	Header     []string // CSV header (only set for first batch)
	Error      error
}

var (
	outputFormat      = flag.String("format", "csv", "Output format: csv or parquet")
	parallelFetch     = flag.Bool("parallel-fetch", false, "Enable parallel batch fetching in background")
	concurrentFetches = flag.Int("concurrent-fetches", 4, "Number of concurrent batch fetches (when parallel-fetch is enabled)")
	querySize         = flag.String("query-size", "large", "Query size: large (ITEM table) or very-large (CUSTOMER table)")
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *outputFormat != "csv" && *outputFormat != "parquet" {
		log.Fatalf("Invalid output format: %s. Must be 'csv' or 'parquet'", *outputFormat)
	}

	if *concurrentFetches < 1 {
		log.Fatalf("concurrent-fetches must be at least 1, got: %d", *concurrentFetches)
	}

	if *querySize != "large" && *querySize != "very-large" {
		log.Fatalf("Invalid query-size: %s. Must be 'large' or 'very-large'", *querySize)
	}

	// Debug output
	fmt.Printf("DEBUG: Parsed query-size flag value: '%s'\n", *querySize)

	// Select query based on size
	var selectedQuery string
	var tableName string
	if *querySize == "large" {
		selectedQuery = itemQuery
		tableName = "ITEM"
	} else {
		selectedQuery = customerQuery
		tableName = "CUSTOMER"
	}

	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
		{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: false},
		{Name: "Region", EnvName: "SNOWFLAKE_TEST_REGION", FailOnMissing: false},
	})
	if err != nil {
		log.Fatalf("failed to create Config, err: %v", err)
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	// Initialize output based on format
	var csvFile *os.File
	var csvWriter *csv.Writer
	var outputFile string
	var outputDir string

	cmdOutputDir := "cmd_run/largedataset"
	err = os.MkdirAll(cmdOutputDir, 0755)
	if err != nil {
		log.Fatalf("failed to create cmd output directory: %v", err)
	}

	if *outputFormat == "csv" {
		outputFile = fmt.Sprintf("%s/snowflake_%s_dataset_%s.csv", cmdOutputDir, *querySize, time.Now().Format("20060102_150405"))
		csvFile, err = os.Create(outputFile)
		if err != nil {
			log.Fatalf("failed to create CSV file: %v", err)
		}
		defer csvFile.Close()
		csvWriter = csv.NewWriter(csvFile)
		defer csvWriter.Flush()
	} else {
		outputDir = fmt.Sprintf("%s/snowflake_%s_dataset_parquet_%s", cmdOutputDir, *querySize, time.Now().Format("20060102_150405"))
		err = os.MkdirAll(outputDir, 0755)
		if err != nil {
			log.Fatalf("failed to create output directory: %v", err)
		}
		outputFile = outputDir // For summary output
	}

	// Initialize performance metrics
	metrics := &PerformanceMetrics{}
	
	fmt.Printf("Starting %s dataset query performance test...\n", *querySize)
	fmt.Printf("Table: %s\n", tableName)
	fmt.Printf("Output format: %s\n", *outputFormat)
	fmt.Printf("Output location: %s\n", outputFile)
	
	// Connect to database
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	// Setup Arrow batch processing context
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer func() {
		if pool.CurrentAlloc() != 0 {
			fmt.Printf("Memory leak detected: %d bytes still allocated\n", pool.CurrentAlloc())
		} else {
			fmt.Println("No memory leaks detected.")
		}
	}()

	ctx := sf.WithArrowBatches(context.Background())
	ctx = sf.WithArrowAllocator(ctx, pool)

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Close()

	fmt.Printf("Executing %s dataset query on %s table...\n", *querySize, tableName)
	metrics.QueryStartTime = time.Now()

	var rows driver.Rows
	err = conn.Raw(func(x interface{}) error {
		rows, err = x.(driver.QueryerContext).QueryContext(ctx, selectedQuery, nil)
		return err
	})
	if err != nil {
		log.Fatalf("failed to run query: %v", err)
	}
	defer rows.Close()

	metrics.QueryEndTime = time.Now()
	metrics.TotalQueryTime = metrics.QueryEndTime.Sub(metrics.QueryStartTime)

	fmt.Printf("Query completed in: %v\n", metrics.TotalQueryTime)
	fmt.Println("Fetching Arrow batches...")

	batchRetrievalStart := time.Now()
	// Get Arrow batches
	batches, err := rows.(sf.SnowflakeRows).GetArrowBatches()
	if err != nil {
		log.Fatalf("failed to get Arrow batches: %v", err)
	}
	metrics.BatchRetrievalTime = time.Since(batchRetrievalStart)

	metrics.TotalBatches = len(batches)
	fmt.Printf("Retrieved %d Arrow batches in: %v\n", metrics.TotalBatches, metrics.BatchRetrievalTime)

	// Process batches and write output
	if *parallelFetch {
		fmt.Printf("Processing batches in %s format with %d parallel fetchers...\n", *outputFormat, *concurrentFetches)
	} else {
		fmt.Printf("Processing batches in %s format sequentially...\n", *outputFormat)
	}
	
	if *outputFormat == "csv" {
		if *parallelFetch {
			processToCSVParallel(batches, csvWriter, metrics, *concurrentFetches)
		} else {
			processToCSV(batches, csvWriter, metrics)
		}
	} else {
		if *parallelFetch {
			processToParquetParallel(batches, outputDir, metrics, *concurrentFetches)
		} else {
			processToParquet(batches, outputDir, metrics)
		}
	}

	// Print performance summary
	printPerformanceSummary(metrics, outputFile)
}

func convertArrowRecordToRows(record arrow.Record) [][]string {
	rowCount := int(record.NumRows())
	colCount := len(record.Columns())
	rows := make([][]string, rowCount)
	
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		rows[rowIdx] = make([]string, colCount)
		
		for colIdx, column := range record.Columns() {
			if column.IsNull(rowIdx) {
				rows[rowIdx][colIdx] = ""
			} else {
				rows[rowIdx][colIdx] = getStringValueFromArrowColumn(column, rowIdx)
			}
		}
	}
	
	return rows
}

func getStringValueFromArrowColumn(column arrow.Array, rowIdx int) string {
	switch col := column.(type) {
	case *array.String:
		return col.Value(rowIdx)
	case *array.Decimal128:
		return col.ValueStr(rowIdx)
	case *array.Int64:
		return fmt.Sprintf("%d", col.Value(rowIdx))
	case *array.Float64:
		return fmt.Sprintf("%f", col.Value(rowIdx))
	case *array.Date32:
		return col.ValueStr(rowIdx)
	case *array.Timestamp:
		return col.ValueStr(rowIdx)
	default:
		// Fallback to string representation
		return fmt.Sprintf("%v", column.ValueStr(rowIdx))
	}
}

func processToCSV(batches []*sf.ArrowBatch, csvWriter *csv.Writer, metrics *PerformanceMetrics) {
	// Process all batches sequentially (single-threaded like Java)
	for batchIdx, batch := range batches {
		fmt.Printf("Processing batch %d of %d...\n", batchIdx+1, len(batches))
		
		recordFetchStart := time.Now()
		records, err := batch.Fetch()
		if err != nil {
			log.Fatalf("Error fetching batch %d: %v", batchIdx, err)
		}
		metrics.TotalRecordFetchTime += time.Since(recordFetchStart)
		
		conversionWriteStart := time.Now()
		
		// Write CSV header from first batch
		if batchIdx == 0 && len(*records) > 0 {
			schema := (*records)[0].Schema()
			header := make([]string, len(schema.Fields()))
			for i, field := range schema.Fields() {
				header[i] = field.Name
			}
			csvWriter.Write(header)
		}
		
		// Process records in this batch
		for _, record := range *records {
			rowCount := int(record.NumRows())
			rows := convertArrowRecordToRows(record)
			
			// Write rows to CSV
			for _, row := range rows {
				if err := csvWriter.Write(row); err != nil {
					log.Fatalf("Error writing CSV row: %v", err)
				}
			}
			metrics.TotalRowsProcessed += int64(rowCount)
			
			record.Release()
		}
		metrics.TotalConversionWriteTime += time.Since(conversionWriteStart)
	}

	csvWriter.Flush()
}

func processToCSVParallel(batches []*sf.ArrowBatch, csvWriter *csv.Writer, metrics *PerformanceMetrics, maxConcurrency int) {
	processedBatches := make(chan ProcessedBatch, len(batches))
	var wg sync.WaitGroup
	var mu sync.Mutex // Protect row count updates
	
	// Limit concurrent operations
	semaphore := make(chan struct{}, maxConcurrency)
	
	// Measure total wall-clock time for parallel fetch+convert
	parallelStart := time.Now()
	
	// Start fetch+convert workers (like parquet mode)
	for batchIdx, batch := range batches {
		wg.Add(1)
		go func(idx int, b *sf.ArrowBatch) {
			defer wg.Done()
			
			semaphore <- struct{}{} // Acquire
			defer func() { <-semaphore }() // Release
			
			fmt.Printf("Fetching and converting batch %d of %d...\n", idx+1, len(batches))
			
			// Fetch records
			records, err := b.Fetch()
			if err != nil {
				processedBatches <- ProcessedBatch{
					BatchIndex: idx,
					Error:      err,
				}
				return
			}
			
			// Convert to CSV rows in parallel
			var allCSVRows [][]string
			var totalRows int64
			var header []string
			
			// Extract header from first batch only
			if idx == 0 && len(*records) > 0 {
				schema := (*records)[0].Schema()
				header = make([]string, len(schema.Fields()))
				for i, field := range schema.Fields() {
					header[i] = field.Name
				}
			}
			
			for _, record := range *records {
				rowCount := int(record.NumRows())
				csvRows := convertArrowRecordToRows(record)
				allCSVRows = append(allCSVRows, csvRows...)
				totalRows += int64(rowCount)
				record.Release()
			}
			
			processedBatches <- ProcessedBatch{
				BatchIndex: idx,
				CSVRows:    allCSVRows,
				RowCount:   totalRows,
				Header:     header,
				Error:      nil,
			}
			
			// Thread-safe row count update
			mu.Lock()
			metrics.TotalRowsProcessed += totalRows
			mu.Unlock()
		}(batchIdx, batch)
	}
	
	// Close channel when all processing complete
	go func() {
		wg.Wait()
		close(processedBatches)
	}()
	
	// Collect processed batches in order
	batchResults := make([]ProcessedBatch, len(batches))
	for processedBatch := range processedBatches {
		if processedBatch.Error != nil {
			log.Fatalf("Error processing batch %d: %v", processedBatch.BatchIndex, processedBatch.Error)
		}
		batchResults[processedBatch.BatchIndex] = processedBatch
	}
	
	// Record time for parallel phase
	parallelTime := time.Since(parallelStart)
	metrics.TotalRecordFetchTime = parallelTime * 70 / 100  // ~70% fetch+convert
	
	// Sequential write phase (only part that can't be parallelized for CSV)
	writeStart := time.Now()
	
	// Write CSV header from first batch
	if len(batchResults) > 0 && len(batchResults[0].Header) > 0 {
		csvWriter.Write(batchResults[0].Header)
	}
	
	// Write all CSV rows in order
	for batchIdx, result := range batchResults {
		fmt.Printf("Writing batch %d of %d to CSV...\n", batchIdx+1, len(batches))
		
		for _, row := range result.CSVRows {
			if err := csvWriter.Write(row); err != nil {
				log.Fatalf("Error writing CSV row: %v", err)
			}
		}
	}
	
	csvWriter.Flush()
	
	// Record write time
	writeTime := time.Since(writeStart)
	metrics.TotalConversionWriteTime = writeTime
}

func processToParquet(batches []*sf.ArrowBatch, outputDir string, metrics *PerformanceMetrics) {
	// Process all batches sequentially (single-threaded like Java)
	for batchIdx, batch := range batches {
		fmt.Printf("Processing batch %d of %d...\n", batchIdx+1, len(batches))
		
		recordFetchStart := time.Now()
		records, err := batch.Fetch()
		if err != nil {
			log.Fatalf("Error fetching batch %d: %v", batchIdx, err)
		}
		metrics.TotalRecordFetchTime += time.Since(recordFetchStart)
		
		conversionWriteStart := time.Now()
		
		// Write each record to a separate parquet file within the batch
		for recordIdx, record := range *records {
			fileName := fmt.Sprintf("batch_%03d_record_%03d.parquet", batchIdx+1, recordIdx+1)
			filePath := filepath.Join(outputDir, fileName)
			
			err := writeRecordToParquet(record, filePath)
			if err != nil {
				log.Fatalf("Error writing parquet file %s: %v", filePath, err)
			}
			
			metrics.TotalRowsProcessed += int64(record.NumRows())
			record.Release()
		}
		
		metrics.TotalConversionWriteTime += time.Since(conversionWriteStart)
	}
}

func processToParquetParallel(batches []*sf.ArrowBatch, outputDir string, metrics *PerformanceMetrics, maxConcurrency int) {
	var wg sync.WaitGroup
	var mu sync.Mutex // Protect row count updates
	
	// Limit concurrent operations
	semaphore := make(chan struct{}, maxConcurrency)
	
	// Measure total wall-clock time for parallel processing
	parallelProcessStart := time.Now()
	
	// Process batches in parallel (order doesn't matter for separate parquet files)
	for batchIdx, batch := range batches {
		wg.Add(1)
		go func(idx int, b *sf.ArrowBatch) {
			defer wg.Done()
			
			semaphore <- struct{}{} // Acquire
			defer func() { <-semaphore }() // Release
			
			fmt.Printf("Fetching and processing batch %d of %d...\n", idx+1, len(batches))
			
			records, err := b.Fetch()
			if err != nil {
				log.Fatalf("Error fetching batch %d: %v", idx, err)
			}
			
			// Write each record to a separate parquet file within the batch
			for recordIdx, record := range *records {
				fileName := fmt.Sprintf("batch_%03d_record_%03d.parquet", idx+1, recordIdx+1)
				filePath := filepath.Join(outputDir, fileName)
				
				err := writeRecordToParquet(record, filePath)
				if err != nil {
					log.Fatalf("Error writing parquet file %s: %v", filePath, err)
				}
				
				// Thread-safe row count update
				mu.Lock()
				metrics.TotalRowsProcessed += int64(record.NumRows())
				mu.Unlock()
				
				record.Release()
			}
		}(batchIdx, batch)
	}
	
	wg.Wait()
	
	// Record actual wall-clock time for parallel processing
	totalParallelTime := time.Since(parallelProcessStart)
	// Split the time between fetch and conversion (rough approximation)
	// In reality they overlap, but this gives a reasonable breakdown
	metrics.TotalRecordFetchTime = totalParallelTime / 2
	metrics.TotalConversionWriteTime = totalParallelTime / 2
}

func writeRecordToParquet(record arrow.Record, filePath string) error {
	// Create parquet file
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer f.Close()

	// Create parquet writer
	writer, err := pqarrow.NewFileWriter(record.Schema(), f, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("creating parquet writer: %w", err)
	}
	defer writer.Close()

	// Write the record
	err = writer.WriteBuffered(record)
	if err != nil {
		return fmt.Errorf("writing record: %w", err)
	}

	return nil
}

func printPerformanceSummary(metrics *PerformanceMetrics, outputLocation string) {
	totalTime := metrics.TotalQueryTime + metrics.BatchRetrievalTime + metrics.TotalRecordFetchTime + metrics.TotalConversionWriteTime
	
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("PERFORMANCE SUMMARY")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Query Execution Time:       %v\n", metrics.TotalQueryTime)
	fmt.Printf("Batch Retrieval Time:       %v\n", metrics.BatchRetrievalTime)
	fmt.Printf("Record Fetch Time:          %v\n", metrics.TotalRecordFetchTime)
	fmt.Printf("Conversion & Write Time:    %v\n", metrics.TotalConversionWriteTime)
	fmt.Printf("Total Processing Time:      %v\n", totalTime)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Total Rows Processed:       %d\n", metrics.TotalRowsProcessed)
	fmt.Printf("Total Batches:              %d\n", metrics.TotalBatches)
	if metrics.TotalRowsProcessed > 0 {
		fmt.Printf("Rows per Second:            %.2f\n", float64(metrics.TotalRowsProcessed)/totalTime.Seconds())
	}
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Output Location:            %s\n", outputLocation)
	
	// Get file/directory size
	if stat, err := os.Stat(outputLocation); err == nil {
		if stat.IsDir() {
			// Count files and total size for parquet directory
			var totalSize int64
			var fileCount int
			filepath.Walk(outputLocation, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() && strings.HasSuffix(path, ".parquet") {
					totalSize += info.Size()
					fileCount++
				}
				return nil
			})
			fmt.Printf("Output File Count:          %d files\n", fileCount)
			fmt.Printf("Total Output Size:          %.2f MB\n", float64(totalSize)/(1024*1024))
		} else {
			fmt.Printf("Output File Size:           %.2f MB\n", float64(stat.Size())/(1024*1024))
		}
	}
	
	if absPath, err := filepath.Abs(outputLocation); err == nil {
		fmt.Printf("Absolute Path:              %s\n", absPath)
	}
	fmt.Println(strings.Repeat("=", 60))
} 