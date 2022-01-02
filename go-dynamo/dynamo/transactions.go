package dynamo

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const ErrTxConditionCheckFailed = "TX_CONDITION_CHECK_FAILED"

// TransactionItem contains an item to create / update
// in a transaction operation.
type TransactionItem struct {
	Name    string // arbitrary name to reference transaction item
	request string // C,R,U,D, CC (condition check)
	Item    interface{}
	Table   *Table
	Query   *Query
	Expr    Expression
}

func (t *TransactionItem) GetRequest() string {
	return t.request
}

// NewCreateTxItem initializes a new TransactionItem object for create requests.
func NewCreateTxItem(name string, item interface{}, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "C",
		Item:    item,
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NewUpdateTxItem initializes a new TransactionItem object for update requests.
func NewUpdateTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "U",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NewReadTxItem initializes a new TransactionItem object for read requests.
func NewReadTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "R",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NeDeletewTxItem initializes a new TransactionItem object for delete requests.
func NewDeleteTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "D",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NewConditionalCheckTxItem initializes a new TransactionItem object for conditional check requests.
func NewConditionCheckTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "CC",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// TxConditionCheck checks that each conditional check for a list of transaction items passes. Failed condition checks
// return an error value, and a list of the TransactionItems that failed their condition checks. Successful condition
// checks return an empty list of TransactionItems and nil error value.
func TxWrite(svc *dynamodb.DynamoDB, items []TransactionItem, requestToken string) ([]TransactionItem, error) {
	// verify <= 25 tx items
	if len(items) > 25 {
		log.Printf("TxUpdate failed: tx items exceeds max size")
		return []TransactionItem{}, fmt.Errorf("TX_ITEMS_EXCEEDS_LIMIT")
	}

	txInput := &dynamodb.TransactWriteItemsInput{}
	// set client request token / idempotency key if provided
	if requestToken != "" {
		txInput.ClientRequestToken = aws.String(requestToken)
	}

	// create tx write items for input
	for _, ti := range items {
		txItem := newTxWriteItem(ti)
		txInput.TransactItems = append(txInput.TransactItems, txItem)
	}

	failed := []TransactionItem{}

	_, err := svc.TransactWriteItems(txInput)
	if err != nil {
		switch t := err.(type) {
		case *dynamodb.TransactionCanceledException:
			log.Fatalf("failed to write items: %s\n%v", t.Message(), t.CancellationReasons)
			check := false // denotes conditional checks failed

			for i, r := range t.CancellationReasons {
				if *r.Code == "ConditionalCheckFailed" {
					log.Printf("Check failed: %v", r.Item)
					check = true
					failed = append(failed, items[i])
				}
			}

			if check {
				return failed, fmt.Errorf(ErrTxConditionCheckFailed)
			}
			return failed, err
		default:
			log.Fatalf("failed to check items: %v", err)
			return failed, err
		}
	}

	log.Printf("Successfully checked items!\n")
	return failed, nil
}

func newTxWriteItem(ti TransactionItem) *dynamodb.TransactWriteItem {
	req := ti.GetRequest()

	switch req {
	case "C":
		txItem := &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				// Item: item (to do)
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
			},
		}
		return txItem
	case "U":
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
		return txItem
	case "D":
		txItem := &dynamodb.TransactWriteItem{
			Delete: &dynamodb.Delete{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
			},
		}
		return txItem
	case "CC":
		txItem := &dynamodb.TransactWriteItem{
			ConditionCheck: &dynamodb.ConditionCheck{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
			},
		}
		return txItem
	default:
		log.Printf("invalid request type")
		return &dynamodb.TransactWriteItem{}
	}

}
