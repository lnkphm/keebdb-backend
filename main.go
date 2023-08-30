package main

import (
	"fmt"
	"context"
	"errors"
	"log"

	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gin-gonic/gin"
)

type Keyboard struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

func (kb Keyboard) GetKey() map[string]types.AttributeValue {
	id, err := attributevalue.Marshal(kb.Id)
	if err != nil {
		log.Fatal(err)
	}
	name, err := attributevalue.Marshal(kb.Name)
	if err != nil {
		log.Fatal(err)
	}
	return map[string]types.AttributeValue{
		"id":   id,
		"name": name,
	}
}

type TableBasics struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func (basics TableBasics) TableExists() (bool, error) {
	exists := true
	_, err := basics.DynamoDbClient.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(basics.TableName)},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("Table %v does not exist.\n", basics.TableName)
		} else {
			log.Printf("Couldn't determine existence of table %v. Here's why: %v\n", basics.TableName, err)
		}
		exists = false
	}
	return exists, err
}

func (basics TableBasics) ListTables() ([]string, error) {
	var tableNames []string
	tables, err := basics.DynamoDbClient.ListTables(
		context.TODO(), &dynamodb.ListTablesInput{},
	)
	if err != nil {
		log.Fatal(err)
	} else {
		tableNames = tables.TableNames
	}
	return tableNames, err
}

func (basics TableBasics) CreateKeyboardTable() (*types.TableDescription, error) {
	var tableDesc *types.TableDescription
	table, err := basics.DynamoDbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("id"),
			AttributeType: types.ScalarAttributeTypeN,
		}, {
			AttributeName: aws.String("name"),
			AttributeType: types.ScalarAttributeTypeS,
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("id"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("name"),
			KeyType:       types.KeyTypeRange,
		}},
		TableName: aws.String(basics.TableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	if err != nil {
		log.Fatal(err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(basics.DynamoDbClient)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(basics.TableName),
		}, 5*time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		tableDesc = table.TableDescription
	}
	return tableDesc, err
}

func (basics TableBasics) GetKeyboardByID(id string) (Keyboard, error) {
	keyboard := Keyboard{Id: id}
	response, err := basics.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key: keyboard.GetKey(), TableName: aws.String(basics.TableName),
	})
	if err != nil {
		log.Fatal(err)
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &keyboard)
		if err != nil {
			log.Fatal(err)
		}
	}
	return keyboard, err

}

func (basics TableBasics) AddKeyboard(keyboard Keyboard) error {
	item, err := attributevalue.MarshalMap(keyboard)
	if err != nil {
		log.Fatal(err)
	}
	_, err = basics.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(basics.TableName), Item: item,
	})
	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
	}
	return err
}

func (basics TableBasics) Scan() ([]Keyboard, error) {
	var keyboards []Keyboard
	var err error
	var response *dynamodb.ScanOutput
	projEx := expression.NamesList(
		expression.Name("id"),
		expression.Name("name"),
	)
	expr, err := expression.NewBuilder().WithProjection(projEx).Build()
	if err != nil {
		log.Printf("Couldn't build expressions for scan. Here's why: %v\n", err)
	} else {
		response, err = basics.DynamoDbClient.Scan(context.TODO(), &dynamodb.ScanInput{
			TableName:                 aws.String(basics.TableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
		})
		if err != nil {
			log.Printf("Couldn't scan for keyboards. Here's why: %v\n", err)
		} else {
			err = attributevalue.UnmarshalListOfMaps(response.Items, &keyboards)
			if err != nil {
				log.Printf("Could't unmarshal query response. Here's why: %v\n", err)
			}
		}
	}
	return keyboards, err
}

func (basics TableBasics) GetKeyboardsHandler(c *gin.Context) {
	keyboards, err := basics.Scan()
	if err != nil {
		log.Fatal(err)
	}
	c.IndentedJSON(http.StatusOK, keyboards)
}

func main() {
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	dynamoClient := dynamodb.NewFromConfig(config)

	keyboardTable := TableBasics{
		DynamoDbClient: dynamoClient,
		TableName:      "keebdb-keyboards",
	}

	exists, err := keyboardTable.TableExists()
	if err != nil {
		if !exists {
			log.Printf("Table not found. Creating new one...\n")
			_, err := keyboardTable.CreateKeyboardTable()
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	
	keyboards, err := keyboardTable.Scan()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(keyboards)

	router := gin.New()
	router.GET("/api/keyboards", keyboardTable.GetKeyboardsHandler)
	// router.GET("/api/keyboards/:id", getKeyboardByID)
	// router.POST("/api/keyboards", postKeyboard)

	router.Run("localhost:8080")
}
