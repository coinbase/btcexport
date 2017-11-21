package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/limits"
	"github.com/btcsuite/btclog"
	"github.com/coinbase/btcexport"
)

const (
	// blockDbNamePrefix is the prefix for the btcd block database.
	blockDbNamePrefix = "blocks"
)

var (
	cfg *config
	log btclog.Logger
)

// loadBlockDB opens the block database and returns a handle to it.
func loadBlockDB() (database.DB, error) {
	// The database name is based on the database type.
	dbName := blockDbNamePrefix + "_" + cfg.DbType
	dbPath := filepath.Join(cfg.DataDir, dbName)

	log.Infof("Loading block database from '%s'", dbPath)
	db, err := database.Open(cfg.DbType, dbPath, activeNetParams.Net)
	if err != nil {
		return nil, err
	}

	log.Info("Block database loaded")
	return db, nil
}

// realMain is the real main function for the utility.  It is necessary to work
// around the fact that deferred functions do not run when os.Exit() is called.
func realMain() error {
	// Load configuration and parse command line.
	tcfg, _, err := loadConfig()
	if err != nil {
		return err
	}
	cfg = tcfg

	// Setup logging.
	backendLogger := btclog.NewBackend(os.Stdout)
	defer os.Stdout.Sync()
	log = backendLogger.Logger("MAIN")
	// database.UseLogger(backendLogger.Logger("BCDB"))
	// blockchain.UseLogger(backendLogger.Logger("CHAN"))

	// Load the block database.
	db, err := loadBlockDB()
	if err != nil {
		log.Errorf("Failed to load database: %v", err)
		return err
	}
	defer db.Close()

	chain, err := blockchain.New(&blockchain.Config{
		DB:          db,
		ChainParams: activeNetParams,
		TimeSource:  blockchain.NewMedianTime(),
	})

	// Create a block importer for the database and input file and start it.
	// The done channel returned from start will contain an error if
	// anything went wrong.
	exporter, err := btcexport.New(btcexport.Config{
		DB:             db,
		Chain:          chain,
		NetParams:      activeNetParams,
		OpenWriter:     cfg.writerFactory,
		StartHeight:    cfg.StartHeight,
		EndHeight:      cfg.EndHeight,
		ConfirmedDepth: 6,
		FileSizeLimit:  1024 * 1024,
	})
	if err != nil {
		log.Errorf("Failed create block exporter: %v", err)
		return err
	}

	// Perform the import asynchronously.  This allows blocks to be
	// processed and read in parallel.  The results channel returned from
	// Import contains the statistics about the import including an error
	// if something went wrong.
	log.Info("Starting export")
	err = exporter.Start()
	if err != nil {
		log.Errorf("Failed create start block exporter: %v", err)
		return err
	}

	errorCountChan := make(chan uint)
	go handleErrors(exporter, errorCountChan)

	if cfg.Progress != 0 {
		go logProgress(exporter)
	}

	// Wait for exporter to finish.
	<-exporter.Done()

	// Wait for the error handling gorouting to exit.
	errCount := <-errorCountChan
	if errCount > 0 {
		return fmt.Errorf("Exporter exited with %d errors", errCount)
	}

	log.Infof("Completed successfully")
	return nil
}

// handleErrors handles any errors coming from a block exporter's error channel.
// This function will log any errors and immediately stop the export process
// after any occur. Once the export routine is done, this reports the total
// number of errors to the errorCountChan parameter channel.
//
// This function should be run as a goroutine.
func handleErrors(exporter *btcexport.BlockExporter,
	errorCountChan chan<- uint) {

	var errorCount uint
	for {
		err, more := <-exporter.Errors()
		if !more {
			errorCountChan <- errorCount
			return
		}

		log.Error(err)
		errorCount++

		// Stop export process after the first error.
		if errorCount == 1 {
			exporter.Stop()
		}
	}
}

// logProgress periodically logs the status of the export routine and exits once
// the export routine ends.
//
// This function should be run as a goroutine.
func logProgress(exporter *btcexport.BlockExporter) {
	ticker := time.NewTicker(time.Duration(cfg.Progress) * time.Second)
	for {
		select {
		case <-ticker.C:
			log.Infof("Processed %d / %d blocks", exporter.BlocksProcessed(),
				exporter.TotalBlocks())
		case <-exporter.Done():
			ticker.Stop()
			return
		}
	}
}

func main() {
	// Use all processor cores and up some limits.
	runtime.GOMAXPROCS(runtime.NumCPU())
	if err := limits.SetLimits(); err != nil {
		os.Exit(1)
	}

	// Work around defer not working after os.Exit()
	if err := realMain(); err != nil {
		os.Exit(1)
	}
}
