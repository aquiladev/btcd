package data

import (
	"github.com/azure/azure-sdk-for-go/storage"
)

type AzureBalanceRepository struct {
	tableRepository AzureStorageTableRepository
	table           *storage.Table
}

func NewAzureBalanceRepository(repository AzureStorageTableRepository, tableName string) (*AzureBalanceRepository, error) {
	repo := new(AzureBalanceRepository)
	repo.tableRepository = repository

	table, _ := repo.tableRepository.Ensure(tableName)
	if table == nil {
		return nil, nil
	}
	repo.table = table

	return repo, nil
}

func (t *AzureBalanceRepository) Get(partitionKey string) (*Balance, error) {
	entities, err := t.tableRepository.Get(partitionKey, "", t.table)

	if err != nil || len(entities) == 0 {
		return nil, err
	}

	prop := entities[0].Properties
	balance := &Balance{
		PublicKey: entities[0].PartitionKey,
		Value:     prop["Value"].(int64),
	}

	return balance, nil
}

func (t *AzureBalanceRepository) Insert(balance *Balance) error {
	props := map[string]interface{}{
		"Value": balance.Value,
	}

	return t.tableRepository.Insert(balance.PublicKey, "", props, t.table)
}

func (t *AzureBalanceRepository) Update(balance *Balance) error {
	props := map[string]interface{}{
		"Value": balance.Value,
	}

	entities, err := t.tableRepository.Get(balance.PublicKey, "", t.table)
	if err != nil {
		return err
	}

	return t.tableRepository.Update(entities[0], props)
}
