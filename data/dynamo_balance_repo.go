package data

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoBalanceRepository struct {
	db        *dynamodb.DynamoDB
	tableName string
}

func NewDynamoBalanceRepository(clientId, clientSecret, tableName string) (*DynamoBalanceRepository, error) {
	repo := new(DynamoBalanceRepository)
	repo.tableName = tableName

	//TODO update settings
	sessionConfig := &aws.Config{
		Credentials: credentials.NewStaticCredentials(clientId, clientSecret, ""),
		Region:      aws.String(endpoints.UsEast2RegionID),
		Endpoint:    aws.String("http://localhost:8000"),
	}
	repo.db = dynamodb.New(session.Must(session.NewSession(sessionConfig)))

	return repo, nil
}

func (t *DynamoBalanceRepository) Get(publicKey string) (*Balance, error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PublicKey": {
				S: aws.String(publicKey),
			},
		},
		TableName: aws.String(t.tableName),
	}

	result, err := t.db.GetItem(input)
	if err != nil || len(result.Item) == 0 {
		return nil, err
	}

	prop := result.Item
	value, err := strconv.ParseInt(*prop["Value"].N, 10, 64)
	if err != nil {
		return nil, err
	}

	balance := &Balance{
		PublicKey: *prop["PublicKey"].S,
		Value:     value,
	}

	return balance, nil
}

func (t *DynamoBalanceRepository) Insert(balance *Balance) error {
	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PublicKey": {
				S: aws.String(balance.PublicKey),
			},
			"Value": {
				N: aws.String(strconv.FormatInt(balance.Value, 10)),
			},
		},
		ReturnConsumedCapacity: aws.String("NONE"),
		TableName:              aws.String(t.tableName),
	}
	_, err := t.db.PutItem(input)
	return err
}

func (t *DynamoBalanceRepository) Update(balance *Balance) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#V": aws.String("Value"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {
				N: aws.String(strconv.FormatInt(balance.Value, 10)),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"PublicKey": {
				S: aws.String(balance.PublicKey),
			},
		},
		ReturnValues:     aws.String("NONE"),
		TableName:        aws.String(t.tableName),
		UpdateExpression: aws.String("SET #V = :v"),
	}

	_, err := t.db.UpdateItem(input)
	return err
}
