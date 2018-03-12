package btcexport

import (
	"encoding/hex"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

// BlockEncoder is used to encode block, tx, input, and output data as rows in a
// tabular data format.
type BlockEncoder interface {
	// BlockRecordHeader returns column headers for the blocks table.
	BlockRecordHeader() []string

	// TxRecordHeader returns column headers for the transactions table.
	TxRecordHeader() []string

	// TxInRecordHeader returns column headers for the transaction inputs table.
	TxInRecordHeader() []string

	// TxOutRecordHeader returns column headers for the transaction outputs
	// table.
	TxOutRecordHeader() []string

	// GenBlockRecord returns a row in the blocks table derived from the given
	// block.
	GenBlockRecord(block *btcutil.Block) ([]string, error)

	// GenTxRecord returns a row in the transactions table derived from the
	// given transaction.
	GenTxRecord(tx *btcutil.Tx, height uint) ([]string, error)

	// GetTxInRecord returns a row in the transaction inputs table derived from
	// the given input.
	GenTxInRecord(txHash *chainhash.Hash, index int, txIn *wire.TxIn, isCoinbase bool) ([]string, error)

	// GetTxOutRecord returns a row in the transaction outputs table derived from
	// the given output.
	GenTxOutRecord(txHash *chainhash.Hash, index int, txOut *wire.TxOut) ([]string, error)
}

type blockEncoder struct {
	netParams *chaincfg.Params
}

func (be *blockEncoder) BlockRecordHeader() []string {
	return []string{
		"Hash", "Height", "Version", "Previous block", "Merkle root",
		"Timestamp", "Difficulty bits", "Nonce", "Transaction count",
		"Base size", "Size with witness",
	}
}

func (be *blockEncoder) GenBlockRecord(block *btcutil.Block) ([]string, error) {
	header := &block.MsgBlock().Header
	record := []string{
		block.Hash().String(),
		strconv.FormatInt(int64(block.Height()), 10),
		strconv.FormatInt(int64(header.Version), 10),
		header.PrevBlock.String(),
		header.MerkleRoot.String(),
		header.Timestamp.UTC().Format(time.RFC3339),
		strconv.FormatInt(int64(header.Bits), 10),
		strconv.FormatInt(int64(header.Nonce), 10),
		strconv.Itoa(len(block.Transactions())),
		strconv.Itoa(block.MsgBlock().SerializeSizeStripped()),
		strconv.Itoa(block.MsgBlock().SerializeSize()),
	}
	return record, nil
}

func (be *blockEncoder) TxRecordHeader() []string {
	return []string{
		"Hash", "Height", "Index", "Version", "Lock time", "Input count",
		"Output count", "Has witness",
	}
}

func (be *blockEncoder) GenTxRecord(tx *btcutil.Tx, height uint,
) ([]string, error) {

	msgTx := tx.MsgTx()
	record := []string{
		tx.Hash().String(),
		strconv.FormatInt(int64(height), 10),
		strconv.Itoa(tx.Index()),
		strconv.FormatInt(int64(msgTx.Version), 10),
		strconv.FormatInt(int64(msgTx.LockTime), 10),
		strconv.Itoa(len(tx.MsgTx().TxIn)),
		strconv.Itoa(len(tx.MsgTx().TxOut)),
		strconv.FormatBool(msgTx.HasWitness()),
	}
	return record, nil
}

func (be *blockEncoder) TxInRecordHeader() []string {
	return []string{
		"Transaction hash", "Index", "Previous output hash",
		"Previous output index", "Sequence", "Signature script",
	}
}

// TODO: reedeemScript, witness data
func (be *blockEncoder) GenTxInRecord(txHash *chainhash.Hash, index int,
	txIn *wire.TxIn, isCoinbase bool) ([]string, error) {

	record := []string{
		txHash.String(),
		strconv.Itoa(index),
		txIn.PreviousOutPoint.Hash.String(),
		strconv.FormatInt(int64(txIn.PreviousOutPoint.Index), 10),
		strconv.FormatInt(int64(txIn.Sequence), 10),
		"",
	}

	if isCoinbase {
		record[5] = "COINBASE: " + hex.EncodeToString(txIn.SignatureScript)
	} else {
		scriptSig, err := txscript.DisasmString(txIn.SignatureScript)
		if err != nil {
			return record, err
		}
		record[5] = scriptSig
	}

	return record, nil
}

func (be *blockEncoder) TxOutRecordHeader() []string {
	return []string{
		"Transaction hash", "Index", "Value", "Type", "Address",
		"PubKey Script",
	}
}

func (be *blockEncoder) GenTxOutRecord(txHash *chainhash.Hash, index int,
	txOut *wire.TxOut) ([]string, error) {

	record := []string{
		txHash.String(),
		strconv.Itoa(index),
		strconv.FormatInt(txOut.Value, 10),
		"",
		"",
		"",
	}

	scriptClass, addresses, _, err := txscript.ExtractPkScriptAddrs(
		txOut.PkScript, be.netParams)

	// Some output scripts may be unparseable, so return without error.
	if err != nil {
		return record, nil
	}

	record[3] = scriptClass.String()
	switch scriptClass {
	case txscript.PubKeyHashTy, txscript.WitnessV0PubKeyHashTy, txscript.PubKeyTy, txscript.ScriptHashTy, txscript.WitnessV0ScriptHashTy:
		if len(addresses) == 1 {
			record[4] = addresses[0].String()
		}
	}

	scriptPubKey, err := txscript.DisasmString(txOut.PkScript)
	if err != nil {
		return record, err
	}

	record[5] = scriptPubKey
	return record, nil
}
