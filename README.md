# btcexport

btcexport is a utility for parsing the blockchain as stored by a [btcd](https://github.com/btcsuite/btcd) full node and exporting it as CSV to be loaded into a columnar data store. The project supports multiple file destinations, currently the local filesystem and Amazon S3. To run btcexport, you must have a btcd node that is synced to the chain height you intend to export to. The node *must be off* while the export process is running.

# Usage

```
Usage:
  btcexport [OPTIONS]

Application Options:
  -b, --datadir=      Location of the btcd data directory (default: /home/nobody/.btcd/data)
      --dbtype=       Database backend to use for the Block Chain (default: ffldb)
      --testnet       Use the test network
      --regtest       Use the regression test network
      --simnet        Use the simulation test network
      --output=       Directory to write output files to
      --s3-bucket=    S3 bucket to write output files to
      --s3-prefix=    Key prefix of S3 objects to upload
      --start-height= Optional beginning height of export range (default=0)
      --end-height=   Ending height of of export range (default=tip-6)
  -p, --progress=     Show a progress message each time this number of seconds have passed -- Use 0 to disable progress announcements (default: 10)

Help Options:
  -h, --help          Show this help message
```

# Installing

```bash
$ go get -u github.com/coinbase/btcexport
$ cd $GOPATH/src/github.com/coinbase/btcexport
$ dep ensure
$ go install ./cmd/...
```
