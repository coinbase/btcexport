package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/coinbase/btcexport"
	flags "github.com/jessevdk/go-flags"
)

const (
	defaultDbType   = "ffldb"
	defaultProgress = 10
)

var (
	btcdHomeDir     = btcutil.AppDataDir("btcd", false)
	defaultDataDir  = filepath.Join(btcdHomeDir, "data")
	knownDbTypes    = database.SupportedDrivers()
	activeNetParams = &chaincfg.MainNetParams
)

// config defines the configuration options for findcheckpoint.
//
// See loadConfig for details on the configuration load process.
type config struct {
	DataDir        string `short:"b" long:"datadir" description:"Location of the btcd data directory"`
	DbType         string `long:"dbtype" description:"Database backend to use for the Block Chain"`
	TestNet3       bool   `long:"testnet" description:"Use the test network"`
	RegressionTest bool   `long:"regtest" description:"Use the regression test network"`
	SimNet         bool   `long:"simnet" description:"Use the simulation test network"`
	OutputDir      string `long:"output" description:"Directory to write output files to"`
	S3Bucket       string `long:"s3-bucket" description:"S3 bucket to write output files to"`
	S3Prefix       string `long:"s3-prefix" description:"Key prefix of S3 objects to upload"`
	StartHeight    uint   `long:"start-height" description:"Optional beginning height of export range (default=0)"`
	EndHeight      uint   `long:"end-height" description:"Ending height of of export range (default=tip-6)"`
	Progress       int    `short:"p" long:"progress" description:"Show a progress message each time this number of seconds have passed -- Use 0 to disable progress announcements"`

	writerFactory btcexport.WriterFactory
}

// validDbType returns whether or not dbType is a supported database type.
func validDbType(dbType string) bool {
	for _, knownType := range knownDbTypes {
		if dbType == knownType {
			return true
		}
	}

	return false
}

// netName returns the name used when referring to a bitcoin network.  At the
// time of writing, btcd currently places blocks for testnet version 3 in the
// data and log directory "testnet", which does not match the Name field of the
// chaincfg parameters.  This function can be used to override this directory name
// as "testnet" when the passed active network matches wire.TestNet3.
//
// A proper upgrade to move the data and log directories for this network to
// "testnet3" is planned for the future, at which point this function can be
// removed and the network parameter's name used instead.
func netName(chainParams *chaincfg.Params) string {
	switch chainParams.Net {
	case wire.TestNet3:
		return "testnet"
	default:
		return chainParams.Name
	}
}

// makeWriterFactory returns an appropriate WriterFactory determined by the
// command line arguments.
func makeWriterFactory(cfg *config) (btcexport.WriterFactory, error) {
	switch {
	case cfg.OutputDir != "":
		if err := checkDirEmptyOrCreate(cfg.OutputDir); err != nil {
			return nil, err
		}
		return btcexport.RotatingFileWriter(cfg.OutputDir), nil
	case cfg.S3Bucket != "":
		sess := session.Must(session.NewSession())
		uploader := s3manager.NewUploader(sess)
		options := s3manager.UploadInput{
			Bucket: &cfg.S3Bucket,
			Key:    &cfg.S3Prefix,
		}
		return btcexport.RotatingS3Writer(uploader, &options), nil
	}

	return nil, fmt.Errorf("No output destination specified")
}

// checkDirEmptyOrCreate ensures that the given file path either does not exist
// yet or is the path to an empty directory. If the directory does not exist,
// this function will create it.
func checkDirEmptyOrCreate(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		// Create empty directory if it does not exist and return.
		if os.IsNotExist(err) {
			err = os.Mkdir(dirPath, 0755)
			if err != nil {
				return fmt.Errorf("Failed to create output directory: %v", err)
			}
			return nil
		}

		return err
	}
	defer dir.Close()

	_, err = dir.Readdirnames(1)
	if err != io.EOF {
		return fmt.Errorf("Output directory %s must be empty", dirPath)
	}
	return nil
}

// loadConfig initializes and parses the config using command line options.
func loadConfig() (*config, []string, error) {
	// Default config.
	cfg := config{
		DataDir:  defaultDataDir,
		DbType:   defaultDbType,
		Progress: defaultProgress,
	}

	// Parse command line options.
	parser := flags.NewParser(&cfg, flags.Default)
	remainingArgs, err := parser.Parse()
	if err != nil {
		if e, ok := err.(*flags.Error); !ok || e.Type != flags.ErrHelp {
			parser.WriteHelp(os.Stderr)
		}
		return nil, nil, err
	}

	// Multiple networks can't be selected simultaneously.
	funcName := "loadConfig"
	numNets := 0
	// Count number of network flags passed; assign active network params
	// while we're at it
	if cfg.TestNet3 {
		numNets++
		activeNetParams = &chaincfg.TestNet3Params
	}
	if cfg.RegressionTest {
		numNets++
		activeNetParams = &chaincfg.RegressionNetParams
	}
	if cfg.SimNet {
		numNets++
		activeNetParams = &chaincfg.SimNetParams
	}
	if numNets > 1 {
		str := "%s: The testnet, regtest, and simnet params can't be " +
			"used together -- choose one of the three"
		err := fmt.Errorf(str, funcName)
		fmt.Fprintln(os.Stderr, err)
		parser.WriteHelp(os.Stderr)
		return nil, nil, err
	}

	// Validate database type.
	if !validDbType(cfg.DbType) {
		str := "%s: The specified database type [%v] is invalid -- " +
			"supported types %v"
		err := fmt.Errorf(str, "loadConfig", cfg.DbType, knownDbTypes)
		fmt.Fprintln(os.Stderr, err)
		parser.WriteHelp(os.Stderr)
		return nil, nil, err
	}

	// Append the network type to the data directory so it is "namespaced"
	// per network.  In addition to the block database, there are other
	// pieces of data that are saved to disk such as address manager state.
	// All data is specific to a network, so namespacing the data directory
	// means each individual piece of serialized data does not have to
	// worry about changing names per network and such.
	cfg.DataDir = filepath.Join(cfg.DataDir, netName(activeNetParams))

	// Use config to create writer factory.
	cfg.writerFactory, err = makeWriterFactory(&cfg)
	if err != nil {
		err = fmt.Errorf("loadConfig: %v", err)
		fmt.Fprintln(os.Stderr, err)
		parser.WriteHelp(os.Stderr)
		return nil, nil, err
	}

	return &cfg, remainingArgs, nil
}
