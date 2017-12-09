package netsync

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/btcsuite/btcd/data"
)

func TestWriteTx(t *testing.T) {
	balanceRepo, err := data.NewLevelDbBalanceRepository("D:\\LevelDB-test")
	if err != nil {
		panic(err)
	}

	sm := SyncManager{
		balanceRepo: balanceRepo,
	}

	done := make(chan bool)
	go sm.writeTx(done, &keyValue{
		key:   "1",
		value: 1,
	})

	<-done
}

func BenchmarkWriteTxs(b *testing.B) {
	path := fmt.Sprintf("D:\\LevelDB-test\\%d", b.N)
	balanceRepo, err := data.NewLevelDbBalanceRepository(path)
	if err != nil {
		panic(err)
	}

	sm := SyncManager{
		balanceRepo: balanceRepo,
	}

	addressMap := make(map[string]int64)
	for i := 1; i <= 1; i++ {
		addressMap[strconv.Itoa(i)] = int64(i)
	}

	b.ResetTimer()
	fmt.Println(addressMap)

	for i := 1; i <= b.N; i++ {
		sm.writeTxs(addressMap)
	}
}
