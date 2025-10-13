// Package dynamo contains controls and objects for DynamoDB CRUD operations.
// Operations in this package are abstracted from all other application logic
// and are designed to be used with any DynamoDB table and any object schema.
// This file contains CRUD operations for working with DynamoDB.
package dynamo

/* TO DO:
- add expression logic to Create, Read, Delete operations
*/

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/ggarcia209/go-aws/goaws"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const ErrRequestThrottled = "ERR_REQUEST_THROTTLED"

type DynamoDbLogic interface {
	ListTables() ([]string, int, error)
	CreateTable(table *Table) error
	CreateItem(item interface{}, tableName string) error
	DeleteTable(svc *dynamodb.DynamoDB, tableName string) error
	GetItem(q *Query, tableName string, item interface{}, expr Expression) (interface{}, error)
	UpdateItem(q *Query, tableName string, expr Expression) error
	DeleteItem(q *Query, tableName string) error
	BatchWriteCreate(tableName string, fc *FailConfig, items []interface{}) error
	BatchWriteDelete(tableName string, fc *FailConfig, queries []*Query) error
	BatchGet(tableName string, fc *FailConfig, queries []*Query, refObjs []interface{}, expr Expression) ([]interface{}, error)
	ScanItems(tableName string, model interface{}, startKey interface{}, expr Expression) ([]interface{}, error)
	TxWrite(items []TransactionItem, requestToken string) ([]TransactionItem, error)
}

type DynamoDB struct {
	svc        *dynamodb.DynamoDB
	tables     map[string]*Table
	failConfig *FailConfig
}

func NewDynamoDB(sess goaws.Session, tables []*Table, failConfig *FailConfig) *DynamoDB {
	tm := make(map[string]*Table)
	for _, t := range tables {
		tm[t.TableName] = t
	}
	return &DynamoDB{
		svc:        dynamodb.New(sess.GetSession()),
		tables:     tm,
		failConfig: failConfig,
	}
}

// InitSesh initializes a new session with default config/credentials.
func InitSesh(sess goaws.Session) *dynamodb.DynamoDB {
	return dynamodb.New(sess.GetSession())
}

// ListTables lists the tables in the database.
func (d *DynamoDB) ListTables() ([]string, int, error) {
	names := []string{}
	t := 0
	input := &dynamodb.ListTablesInput{}

	for {
		// Get the list of tables
		result, err := d.svc.ListTables(input)
		if err != nil {
			// if aerr, ok := err.(awserr.Error); ok {
			// 	switch aerr.Code() {
			// 	case dynamodb.ErrCodeInternalServerError:
			// 		fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			// 	default:
			// 		fmt.Println(aerr.Error())
			// 	}
			// }
			return nil, 0, fmt.Errorf("d.svc.ListTables: %w", err)
		}

		for _, n := range result.TableNames {
			names = append(names, *n)
			t++
		}

		// assign the last read tablename as the start for our next call to the ListTables function
		// the maximum number of table names returned in a call is 100 (default), which requires us to make
		// multiple calls to the ListTables function to retrieve all table names
		input.ExclusiveStartTableName = result.LastEvaluatedTableName

		if result.LastEvaluatedTableName == nil {
			break
		}
	}
	return names, t, nil
}

// CreateTable creates a new table with the parameters passed to the Table struct.
// NOTE: CreateTable creates Table in * On-Demand * billing mode.
func (d *DynamoDB) CreateTable(table *Table) error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{ // Primary Key
				AttributeName: aws.String(table.PrimaryKeyName),
				AttributeType: aws.String(table.PrimaryKeyType),
			},
			{
				AttributeName: aws.String(table.SortKeyName),
				AttributeType: aws.String(table.SortKeyType),
			},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(table.PrimaryKeyName),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(table.SortKeyName),
				KeyType:       aws.String("RANGE"),
			},
		},
		TableName: aws.String(table.TableName),
	}

	if _, err := d.svc.CreateTable(input); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceInUseException" {
				return fmt.Errorf(awsErr.Code())
			}
		} else {
			return fmt.Errorf("d.svc.CreateTable: %w", err)
		}
	}

	d.tables[table.TableName] = table

	return nil
}

// CreateItem puts a new item in the table.
func (d *DynamoDB) CreateItem(item interface{}, tableName string) error {
	// check if table exists
	t := d.tables[tableName]
	if t == nil {
		return NewTableNotFoundErr(tableName)
	}

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("dynamodbattribute.MarshalMap: %w", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	if _, err = d.svc.PutItem(input); err != nil {
		return fmt.Errorf("d.svc.PutItem: %w", err)
	}

	return nil
}

// GetItem reads an item from the database.
// Returns Attribute Value map interface (map[stirng]interface{}) if object found.
// Returns interface of type item if object not found.
func (d *DynamoDB) GetItem(q *Query, tableName string, item interface{}, expr Expression) (interface{}, error) {
	// get table
	t := d.tables[tableName]
	if t == nil {
		return nil, NewTableNotFoundErr(tableName)
	}

	key := keyMaker(q, t)
	input := &dynamodb.GetItemInput{
		TableName: aws.String(t.TableName),
		Key:       key,
	}
	if expr.Projection() != nil {
		input.ExpressionAttributeNames = expr.Names()
		input.ProjectionExpression = expr.Projection()
	}

	result, err := d.svc.GetItem(input)
	if err != nil {
		return nil, fmt.Errorf("d.svc.GetItem: %w", err)
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		return nil, fmt.Errorf("dynamodbattribute.UnmarshalMap: %w", err)
	}

	return item, nil
}

// UpdateItem updates the specified item's attribute defined in the
// Query object with the UpdateValue defined in the Query.
func (d *DynamoDB) UpdateItem(q *Query, tableName string, expr Expression) error {
	// get table
	t := d.tables[tableName]
	if t == nil {
		return NewTableNotFoundErr(tableName)
	}

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 aws.String(t.TableName),
		Key:                       keyMaker(q, t),
		ReturnValues:              aws.String("UPDATED_NEW"),
		UpdateExpression:          expr.Update(),
	}
	if expr.Condition() != nil {
		input.ConditionExpression = expr.Condition()
	}
	if expr.Filter() != nil {
		input.ConditionExpression = expr.Filter()
	}
	if expr.KeyCondition() != nil {
		input.ConditionExpression = expr.KeyCondition()
	}
	if expr.Projection() != nil {
		input.ConditionExpression = expr.Projection()
	}

	if _, err := d.svc.UpdateItem(input); err != nil {
		if err.(awserr.Error).Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return fmt.Errorf("d.svc.UpdateItem: %w", ErrConditionalCheck)
		}
		if err.(awserr.Error).Code() == dynamodb.ErrCodeProvisionedThroughputExceededException {
			return fmt.Errorf("d.svc.UpdateItem: %w", ErrRequestThrottled)
		}

		return fmt.Errorf("d.svc.UpdateItem: %w", err)
	}

	return nil
}

// DeleteTable deletes the selected table.
func (d *DynamoDB) DeleteTable(tableName string) error {
	// get table
	t := d.tables[tableName]
	if t == nil {
		return NewTableNotFoundErr(tableName)
	}

	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(t.TableName),
	}
	if _, err := d.svc.DeleteTable(input); err != nil {
		return fmt.Errorf("d.svc.DeleteTable: %w", err)
	}

	delete(d.tables, tableName)

	return nil
}

// DeleteItem deletes the specified item defined in the Query
func (d *DynamoDB) DeleteItem(q *Query, tableName string) error {
	// get table
	t := d.tables[tableName]
	if t == nil {
		return NewTableNotFoundErr(tableName)
	}

	input := &dynamodb.DeleteItemInput{
		Key:       keyMaker(q, t),
		TableName: aws.String(t.TableName),
	}

	if _, err := d.svc.DeleteItem(input); err != nil {
		return fmt.Errorf("d.svc.DeleteItem: %w", err)
	}

	return nil
}

// BatchWriteCreate writes a list of items to the database.
func (d *DynamoDB) BatchWriteCreate(tableName string, fc *FailConfig, items []interface{}) error {
	if len(items) > 25 {
		return fmt.Errorf("too many items to process")
	}

	// get table
	t := d.tables[tableName]
	if t == nil {
		return NewTableNotFoundErr(tableName)
	}

	// create map of RequestItems
	reqItems := make(map[string][]*dynamodb.WriteRequest)
	wrs := []*dynamodb.WriteRequest{}

	// create PutRequests for each item
	for _, item := range items {
		if item == nil {
			continue
		}

		// marshal each item
		av, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			return fmt.Errorf("ynamodbattribute.MarshalMap: %w", err)
		}
		// create put request, reformat as write request, and add to list
		pr := &dynamodb.PutRequest{Item: av}
		wr := &dynamodb.WriteRequest{PutRequest: pr}
		wrs = append(wrs, wr)
	}
	// populate reqItems map
	reqItems[t.TableName] = wrs

	// generate input from reqItems map
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchWriteItemOutput
	var err error
	for {
		result, err = d.batchWriteUtil(input)
		if err != nil {
			// if not HTTP 5xx error
			if err.(awserr.Error).Code() != dynamodb.ErrCodeInternalServerError {
				// return fmt.Errorf("BatchWriteCreate failed: %v", err)
				return fmt.Errorf("d.batchWriteUtil: %w", err)
			}

			// Retry with exponential backoff algorithm
			if err.(awserr.Error).Code() == dynamodb.ErrCodeInternalServerError && result.UnprocessedItems != nil {
				input = &dynamodb.BatchWriteItemInput{
					RequestItems: result.UnprocessedItems,
				}
				fc.ExponentialBackoff() // waits
				if fc.MaxRetriesReached {
					return fmt.Errorf("d.batchWriteUtil: %w", err)
				}
			}
		}

		if len(result.UnprocessedItems) == 0 {
			fc.Reset() // reset configuration after loop
			break
		}

	}

	return nil
}

// BatchWriteDelete deletes a list of items from the database.
func (d *DynamoDB) BatchWriteDelete(tableName string, fc *FailConfig, queries []*Query) error {
	if len(queries) > 25 {
		return fmt.Errorf("too many items to process")
	}

	// get table
	t := d.tables[tableName]
	if t == nil {
		return NewTableNotFoundErr(tableName)
	}

	// create map of RequestItems
	reqItems := make(map[string][]*dynamodb.WriteRequest)
	wrs := []*dynamodb.WriteRequest{}

	// create PutRequests for each item
	for _, q := range queries {
		if q == nil {
			continue
		}

		// create put request, reformat as write request, and add to list
		dr := &dynamodb.DeleteRequest{Key: keyMaker(q, t)}
		wr := &dynamodb.WriteRequest{DeleteRequest: dr}
		wrs = append(wrs, wr)
	}
	// populate reqItems map
	reqItems[t.TableName] = wrs

	// generate input from reqItems map
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchWriteItemOutput
	var err error
	for {
		result, err = d.batchWriteUtil(input)
		if err != nil {
			// if not HTTP 5xx error
			if err.(awserr.Error).Code() != dynamodb.ErrCodeInternalServerError {
				return fmt.Errorf("d.batchWriteUtil: %w", err)
			}

			// Retry with exponential backoff algorithm
			if err.(awserr.Error).Code() == dynamodb.ErrCodeInternalServerError && result.UnprocessedItems != nil {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedItems)
				input = &dynamodb.BatchWriteItemInput{
					RequestItems: result.UnprocessedItems,
				}
				fc.ExponentialBackoff() // waits
				if fc.MaxRetriesReached {
					return fmt.Errorf("d.batchWriteUtil: %w", err)
				}
			}
		}

		if len(result.UnprocessedItems) == 0 {
			fc.Reset() // reset configuration after loop
			break
		}

	}

	return nil
}

// BatchGet retrieves a list of items from the database
// refObjs must be non-nil pointers of the same type,
// 1 for each query/object returned.
//   - Returns err if len(queries) != len(refObjs).
func (d *DynamoDB) BatchGet(tableName string, fc *FailConfig, queries []*Query, refObjs []interface{}, expr Expression) ([]interface{}, error) {
	if len(queries) > 100 {
		return nil, fmt.Errorf("too many items to process")
	}

	if len(queries) != len(refObjs) {
		return nil, fmt.Errorf("number of queries does not match number of reference objects")
	}

	// get table
	t := d.tables[tableName]
	if t == nil {
		return nil, NewTableNotFoundErr(tableName)
	}

	items := []interface{}{}

	// create map of RequestItems
	reqItems := make(map[string]*dynamodb.KeysAndAttributes)
	keys := []map[string]*dynamodb.AttributeValue{}

	// create Get requests for each query
	for _, q := range queries {
		if q == nil {
			continue
		}

		item := keyMaker(q, t)
		keys = append(keys, item)
	}
	// populate reqItems map
	ka := &dynamodb.KeysAndAttributes{Keys: keys}
	reqItems[t.TableName] = ka

	// generate input from reqItems map
	input := &dynamodb.BatchGetItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchGetItemOutput
	var err error
	for {
		result, err = d.batchGetUtil(input)
		if err != nil {
			// if not HTTP 5xx error
			if err.(awserr.Error).Code() != dynamodb.ErrCodeInternalServerError {
				return nil, fmt.Errorf("d.batchGetUtil: %w", err)
			}
			if err.(awserr.Error).Code() == "ValidationException" {
				return nil, fmt.Errorf("d.batchGetUtil: %w", err)
			}
			if err.(awserr.Error).Code() == "RequestError" {
				return nil, fmt.Errorf("d.batchGetUtil: %w", err)
			}

			// Retry with exponential backoff algorithm
			if err.(awserr.Error).Code() == dynamodb.ErrCodeInternalServerError && result.UnprocessedKeys != nil {
				input = &dynamodb.BatchGetItemInput{
					RequestItems: result.UnprocessedKeys,
				}
				fc.ExponentialBackoff() // waits
				if fc.MaxRetriesReached {
					return nil, fmt.Errorf("d.batchGetUtil: %w", err)
				}
			}
		}

		for i, r := range result.Responses[t.TableName] {
			ref := refObjs[i]
			if err := dynamodbattribute.UnmarshalMap(r, &ref); err != nil {
				return nil, fmt.Errorf("dynamodbattribute.UnmarshalMap, %w", err)
			}
			items = append(items, ref)
		}

		if len(result.UnprocessedKeys) == 0 {
			fc.Reset() // reset configuration after loop
			break
		}

	}

	return items, nil
}

func (d *DynamoDB) batchWriteUtil(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	result, err := d.svc.BatchWriteItem(input)
	if err != nil {
		// if aerr, ok := err.(awserr.Error); ok {
		// 	switch aerr.Code() {
		// 	case dynamodb.ErrCodeProvisionedThroughputExceededException:
		// 		fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
		// 	case dynamodb.ErrCodeResourceNotFoundException:
		// 		fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
		// 	case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
		// 		fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
		// 	case dynamodb.ErrCodeRequestLimitExceeded:
		// 		fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
		// 	case dynamodb.ErrCodeInternalServerError:
		// 		fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
		// 	default:
		// 		fmt.Println(aerr.Error())
		// 	}
		// } else {
		// 	// Print the error, cast err to awserr.Error to get the Code and
		// 	// Message from an error.
		// 	fmt.Println(err.Error())
		// }
		return nil, fmt.Errorf("d.svc.BatchWriteItem: %w", err)
	}
	return result, nil
}

// ScanItems scans the given Table for items matching the given expression parameters.
func (d *DynamoDB) ScanItems(tableName string, model interface{}, startKey interface{}, expr Expression) ([]interface{}, error) {
	// get table
	t := d.tables[tableName]
	if t == nil {
		return nil, NewTableNotFoundErr(tableName)
	}

	items := []interface{}{}

	av, err := dynamodbattribute.MarshalMap(startKey)
	if err != nil {
		return items, fmt.Errorf("dynamodbattribute.MarshalMap: %w", err)
	}

	// Build the query input parameters
	input := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(t.TableName),
	}

	if startKey != nil {
		input.ExclusiveStartKey = av
	}

	// Make the DynamoDB Query API call
	result, err := d.svc.Scan(input)
	if err != nil {
		return items, fmt.Errorf("d.svc.Scan: %w", err)
	}

	// ADD LOGIC FOR HANDLING PAGINATION

	for _, res := range result.Items {
		item := model
		err = dynamodbattribute.UnmarshalMap(res, &item)
		if err != nil {
			return []interface{}{}, fmt.Errorf("dynamodbattribute.UnmarshalMap: %w", err)
		}
		items = append(items, item)
	}

	return items, nil
}

func (d *DynamoDB) batchGetUtil(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	result, err := d.svc.BatchGetItem(input)
	if err != nil {
		// if aerr, ok := err.(awserr.Error); ok {
		// 	switch aerr.Code() {
		// 	case dynamodb.ErrCodeProvisionedThroughputExceededException:
		// 		fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
		// 	case dynamodb.ErrCodeResourceNotFoundException:
		// 		fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
		// 	case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
		// 		fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
		// 	case dynamodb.ErrCodeRequestLimitExceeded:
		// 		fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
		// 	case dynamodb.ErrCodeInternalServerError:
		// 		fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
		// 	default:
		// 		fmt.Println(aerr.Error())
		// 	}
		// } else {
		// 	// Print the error, cast err to awserr.Error to get the Code and
		// 	// Message from an error.
		// 	fmt.Println(err.Error())
		// }
		return nil, fmt.Errorf("d.svc.BatchGetItem: %w", err)
	}
	return result, nil
}

// marshalMap marshals an interface object into an AttributeValue map
func marshalMap(input interface{}) (map[string]*dynamodb.AttributeValue, error) {
	marshal, err := dynamodbattribute.MarshalMap(input)
	if err != nil {
		return nil, fmt.Errorf("dynamodbattribute.MarshalMap: %w", err)
	}
	return marshal, nil
}
