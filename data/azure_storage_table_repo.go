package data

import (
	"net/http"

	"github.com/azure/azure-sdk-for-go/storage"
	"github.com/pkg/errors"
)

type AzureStorageTableRepository struct {
	client storage.Client
}

func NewAzureStorageTableRepository(accountName, accountKey string) AzureStorageTableRepository {
	client, _ := storage.NewBasicClient(accountName, accountKey)
	client.HTTPClient.Transport = &http.Transport{DisableKeepAlives: true}

	return AzureStorageTableRepository{client: client}
}

func (t *AzureStorageTableRepository) Ensure(tableName string) (*storage.Table, error) {
	tableClient := t.client.GetTableService()
	table := tableClient.GetTableReference(tableName)

	err := table.Create(30, storage.EmptyPayload, nil)
	if err != nil {
		return table, err
	}

	return table, nil
}

func (t *AzureStorageTableRepository) Get(partitionKey, rowKey string, table *storage.Table) ([]*storage.Entity, error) {
	filter := "PartitionKey eq '" + partitionKey + "'"
	if rowKey != "" {
		filter += "and RowKey eq '" + rowKey + "'"
	}

	options := storage.QueryOptions{
		Filter: filter,
	}
	res, err := table.QueryEntities(30, storage.FullMetadata, &options)
	if err != nil {
		return nil, errors.Wrap(err, "Error getting entity")
	}

	return res.Entities, nil
}

func (t *AzureStorageTableRepository) Insert(partitionKey, rowKey string, props map[string]interface{}, table *storage.Table) error {
	entity := table.GetEntityReference(partitionKey, rowKey)
	entity.Properties = props
	return entity.Insert(storage.EmptyPayload, nil)
}

func (t *AzureStorageTableRepository) Update(entity *storage.Entity, props map[string]interface{}) error {
	entity.Properties = props
	err := entity.Merge(false, nil)

	if err != nil {
		return errors.Wrap(err, "Error merging entity")
	}

	return nil
}
