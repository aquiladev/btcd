package netsync

import (
	"errors"
	"math"
	"reflect"

	"github.com/aquiladev/btcd/data"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcd/txscript"
)

func (sm *SyncManager) collect(bmsg *blockMsg) {
	var txs map[chainhash.Hash]*btcutil.Tx = make(map[chainhash.Hash]*btcutil.Tx)
	var addrMap map[string]int64 = make(map[string]int64)

	for _, tx := range bmsg.block.Transactions() {
		msgTx := tx.MsgTx()
		txs[*tx.Hash()] = tx

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
				originPkScript = internalOriginTx.PkScript
				originValue = internalOriginTx.Value

				log.Debugf("Internal transaction %+v, PrevOut: %+v", internalOriginTx, prevOut)
			} else {
				entry, err := sm.chain.FetchUtxoEntry(&prevOut.Hash)
				if err != nil {
					log.Error(err)
				}

				originValue = entry.AmountByIndex(prevOut.Index)
				originPkScript = entry.PkScriptByIndex(prevOut.Index)

				log.Debugf("Entry %+v, PrevOut: %+v", entry, prevOut)
			}

			_, addresses, _, _ := txscript.ExtractPkScriptAddrs(originPkScript, sm.chainParams)

			if len(addresses) != 1 {
				log.Warnf("Number of inputs %d, Inputs: %+v, PrevOut: %+v", len(addresses), addresses, &prevOut)
				continue
			}

			pubKey, err := addrToPubKey(addresses[0])
			if err != nil {
				log.Infof("TxOut %+v, Type: %+v", addresses[0], reflect.TypeOf(addresses[0]))
				log.Error(err)
				continue
			}

			log.Debugf("TxIn #%d, PrevOut: %+v, Address: %+v, OriginalValue: %d", i, &prevOut, addresses[0], originValue)
			addrMap[pubKey] -= originValue
		}

		for i, txOut := range msgTx.TxOut {
			_, addresses, _, _ := txscript.ExtractPkScriptAddrs(txOut.PkScript, sm.chainParams)

			if len(addresses) != 1 {
				log.Warnf("Number of outputs %d, Outputs: %+v, Hash: %+v, Tx#: %d, Value: %+v", len(addresses), addresses, tx.Hash(), i, txOut.Value)
				continue
			}

			pubKey, err := addrToPubKey(addresses[0])
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

func addrToPubKey(addr btcutil.Address) (string, error) {
	switch addr := addr.(type) {
	case *btcutil.AddressPubKeyHash:
		return addr.EncodeAddress(), nil

	case *btcutil.AddressScriptHash:
		log.Infof("AddressScriptHash %+v", addr)
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

type KeyPair struct {
	key   string
	value int64
}

func (sm *SyncManager) writeTxs(addressMap map[string]int64) {
	bucketSize := 50
	keys := make([]*KeyPair, len(addressMap))

	i := 0
	for k := range addressMap {
		if addressMap[k] == 0 {
			continue
		}

		keys[i] = &KeyPair{
			key:   k,
			value: addressMap[k],
		}
		i++
	}
	amount := i

	for amount > 0 {
		restPages := amount - bucketSize
		size := bucketSize

		if restPages < 0 {
			size = amount
		}

		sm.writeTxsBundle(keys, int(math.Max(float64(restPages), 0)), size)
		amount = restPages
	}
}

func (sm *SyncManager) writeTxsBundle(keys []*KeyPair, from, amount int) {
	done := make(chan bool)
	for i := 0; i < amount; i++ {
		item := keys[from+i]

		go sm.writeTx(done, item)
	}

	for i := 0; i < amount; i++ {
		<-done
	}
}

func (sm *SyncManager) writeTx(c chan bool, pair *KeyPair) {
	entry, err := sm.balanceRepo.Get(pair.key)
	if err != nil {
		log.Error(err)
		panic("WWWWWW")
	}

	if entry == nil {
		bEntry := new(data.Balance)
		bEntry.PublicKey = pair.key
		bEntry.Value = pair.value

		err := sm.balanceRepo.Insert(bEntry)
		if err != nil {
			log.Error(err)
			c <- false
			panic("WWWWWW")
		}
		c <- true
		return
	}

	entry.Value += pair.value

	err = sm.balanceRepo.Update(entry)
	if err != nil {
		log.Error(err)
		c <- false
		panic("WWWWWW")
	}
	c <- true
}
