package dynamo

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// TransactionItem contains an item to create / update
// in a transaction operation.
type TransactionItem struct {
	Item  interface{}
	Table *Table
	Query *Query
	Expr  Expression
}

// NewTxItem initializes a new TransactionItem object.
func NewTxItem(item interface{}, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Item:  item,
		Table: t,
		Query: q,
		Expr:  e,
	}
	return tx
}

// TxUpdate creates an update transaction for a given list of TransactionItems.
func TxUpdate(svc *dynamodb.DynamoDB, items []TransactionItem, requestToken string) error {
	// verify <= 25 tx items
	if len(items) > 25 {
		log.Printf("TxUpdate failed: tx items exceeds max size")
		return fmt.Errorf("TX_ITEMS_EXCEEDS_LIMIT")
	}

	txInput := &dynamodb.TransactWriteItemsInput{}
	// set client request token / idempotency key if provided
	if requestToken != "" {
		txInput.ClientRequestToken = aws.String(requestToken)
	}

	// create tx write items for input
	for _, ti := range items {
		txItem := &dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
				UpdateExpression:          ti.Expr.Update(),
			},
		}

		txInput.TransactItems = append(txInput.TransactItems, txItem)
	}

	_, err := svc.TransactWriteItems(txInput)
	if err != nil {
		switch t := err.(type) {
		case *dynamodb.TransactionCanceledException:
			log.Fatalf("failed to write items: %s\n%v",
				t.Message(), t.CancellationReasons)
			return err
		default:
			log.Fatalf("failed to write items: %v", err)
			return err
		}
	}

	log.Printf("Successfully updated items!\n")
	return nil
}
