package netsync

import (
	"errors"
	"reflect"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/data"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcd/blockchain"
)

type keyValue struct {
	key   string
	value int64
}

func (sm *SyncManager) explore(blockMsg *blockMsg) {
	txs := make(map[chainhash.Hash]*btcutil.Tx)
	addrMap := make(map[string]int64)
	
	for _, tx := range blockMsg.block.Transactions() {
		txs[*tx.Hash()] = tx
	}

	for _, tx := range blockMsg.block.Transactions() {
		msgTx := tx.MsgTx()

		// explore input transactions
		for i, txIn := range msgTx.TxIn {
			prevOut := txIn.PreviousOutPoint
			if prevOut.Hash.String() == "0000000000000000000000000000000000000000000000000000000000000000" {
				continue
			}

			var originPkScript []byte
			var originValue int64
			var internalOriginTx *wire.TxOut = nil

			internalTx := txs[prevOut.Hash]
			if internalTx != nil {
				internalOriginTx = internalTx.MsgTx().TxOut[prevOut.Index]
			}

			if internalOriginTx != nil {
				originValue = internalOriginTx.Value
				originPkScript = internalOriginTx.PkScript

				log.Debugf("Internal transaction %+v, PrevOut: %+v", internalOriginTx, prevOut)
			} else {
				entry, err := sm.chain.FetchUtxoEntry(&prevOut.Hash)
				if err != nil {
					log.Error(err)
				}

				if entry == nil {
					log.Infof("TxIn #%d %+v", i, txIn)
					log.Infof("Tx %+v", tx)
					log.Infof("Block height %+v", blockMsg.block.Height())
					panic("aaaaaaaaaaaaaaa")
				}

				log.Errorf("Entry %+v, PrevOut: %+v, Tx: %+v", entry, prevOut, tx.Hash())

				originValue = entry.AmountByIndex(prevOut.Index)
				originPkScript = entry.PkScriptByIndex(prevOut.Index)

				//	var pkScript []byte
				//	var value int64
				//	var err error
				//	count := 0
				//
				//out:
				//	for {
				//		pkScript, value, err = sm.fetchInput(prevOut)
				//		if err == nil {
				//			break out
				//		}
				//		log.Error(err)
				//
				//		if count == 3 {
				//			panic(err)
				//		}
				//		count ++
				//		log.Warnf("Retrying %d", count)
				//	}
				//
				//	log.Debugf("PkScript %+v, Value %+v, PrevOut: %+v, Tx: %+v", pkScript, value, prevOut, tx.Hash())
				//
				//	originPkScript = pkScript
				//	originValue = value
			}

			_, addresses, _, _ := txscript.ExtractPkScriptAddrs(originPkScript, sm.chainParams)

			if len(addresses) != 1 {
				log.Warnf("Number of inputs %d, Inputs: %+v, PrevOut: %+v", len(addresses), addresses, &prevOut)
				continue
			}

			pubKey, err := convertToPubKey(addresses[0])
			if err != nil {
				log.Infof("TxOut %+v, Type: %+v", addresses[0], reflect.TypeOf(addresses[0]))
				log.Error(err)
				continue
			}

			log.Debugf("TxIn #%d, PrevOut: %+v, Address: %+v, OriginalValue: %d", i, &prevOut, addresses[0], originValue)
			addrMap[pubKey] -= originValue
		}

		// explore output transactions
		for i, txOut := range msgTx.TxOut {
			_, addresses, _, _ := txscript.ExtractPkScriptAddrs(txOut.PkScript, sm.chainParams)

			if len(addresses) != 1 {
				log.Warnf("Number of outputs %d, Outputs: %+v, Hash: %+v, Tx#: %d, Value: %+v", len(addresses), addresses, tx.Hash(), i, txOut.Value)
				continue
			}

			pubKey, err := convertToPubKey(addresses[0])
			if err != nil {
				log.Infof("TxOut %+v, Type: %+v", addresses[0], reflect.TypeOf(addresses[0]))
				log.Error(err)
				continue
			}
			addrMap[pubKey] += txOut.Value
		}
	}

	sm.writeTxs(addrMap)
}

func (sm *SyncManager) fetchInput(prevOut wire.OutPoint) (pkScript []byte, value int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	var entry *blockchain.UtxoEntry
	entry, err = sm.chain.FetchUtxoEntry(&prevOut.Hash)
	if err != nil {
		return
	}

	value = entry.AmountByIndex(prevOut.Index)
	pkScript = entry.PkScriptByIndex(prevOut.Index)
	return
}

func convertToPubKey(addr btcutil.Address) (string, error) {
	switch addr := addr.(type) {
	case *btcutil.AddressPubKeyHash:
		return addr.EncodeAddress(), nil

	case *btcutil.AddressScriptHash:
		//log.Infof("AddressScriptHash %+v", addr)
		return addr.EncodeAddress(), nil

	case *btcutil.AddressPubKey:
		return addr.AddressPubKeyHash().String(), nil

	case *btcutil.AddressWitnessScriptHash:
		log.Infof("AddressWitnessScriptHash %+v", addr)
		return addr.EncodeAddress(), nil

	case *btcutil.AddressWitnessPubKeyHash:
		log.Infof("AddressWitnessPubKeyHash %+v", addr)
		return addr.EncodeAddress(), nil
	}

	errUnsupportedAddressType := errors.New("address type is not supported " +
		"by the address index")
	return "", errUnsupportedAddressType
}

func (sm *SyncManager) writeTxs(addressMap map[string]int64) {
	done := make(chan bool)

	amount := 0
	for k := range addressMap {
		if addressMap[k] == 0 {
			continue
		}

		pair := &keyValue{
			key:   k,
			value: addressMap[k],
		}

		amount++
		go sm.writeTx(done, pair)
	}

	for i := 0; i < amount; i++ {
		<-done
	}
}

func (sm *SyncManager) writeTx(c chan bool, item *keyValue) {
	entry, err := sm.balanceRepo.Get(item.key)
	if err != nil {
		log.Error(err)
		panic("WWWWWW")
	}

	if entry == nil {
		bEntry := new(data.Balance)
		bEntry.PublicKey = item.key
		bEntry.Value = item.value

		err := sm.balanceRepo.Insert(bEntry)
		if err != nil {
			log.Error(err)
			c <- false
			panic("WWWWWW")
		}
		c <- true
		return
	}

	entry.Value += item.value

	err = sm.balanceRepo.Update(entry)
	if err != nil {
		log.Error(err)
		c <- false
		panic("WWWWWW")
	}
	c <- true
}
