package dynamo

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	// ErrTxConditionCheckFailed is returned when a transaction item fails it's conditional check.
	// This error cannot be retried.
	ErrTxConditionCheckFailed = "TX_CONDITION_CHECK_FAILED"
	// ErrTxConflict is returned when another transaction is in progress for a transaction item.
	// This error can be retried.
	ErrTxConflict = "TX_CONFLICT"
	// ErrTxInProgress is returned when multiple transactions are attempted with the same idempotency key.
	// This error cannot be retried.
	ErrTxInProgress = "TX_IN_PROGRESS"
	// ErrTxThrottled is returned when a transaction item fails due to throttling.
	// This error can be retried.
	ErrTxThrottled = "TX_THROTTLED"
)

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
		txItem, err := newTxWriteItem(ti)
		if err != nil {
			log.Fatalf("TxWrite failed: %v", err)
			return []TransactionItem{}, err
		}
		txInput.TransactItems = append(txInput.TransactItems, txItem)
	}

	failed := []TransactionItem{}

	_, err := svc.TransactWriteItems(txInput)
	if err != nil {
		switch t := err.(type) {
		case *dynamodb.TransactionCanceledException:
			log.Printf("failed to write items: %s\n %v", t.Message(), t.CancellationReasons)
			check := false     // denotes conditional checks failed
			throttled := false // denotes if tx failed due to throttling

			for i, r := range t.CancellationReasons {
				log.Printf("cancellation code: %s", *r.Code)
				if *r.Code == "ConditionalCheckFailed" {
					check = true
					failed = append(failed, items[i])
				}
				if *r.Code == "ThrottlingError" {
					throttled = true
					failed = append(failed, items[i])
				}
			}

			if check {
				// no retry
				return failed, fmt.Errorf(ErrTxConditionCheckFailed)
			}
			if throttled {
				// retry
				return failed, fmt.Errorf(ErrTxThrottled)
			}
			// no retry
			return failed, err
		case *dynamodb.TransactionConflictException:
			// retry
			log.Printf("failed to write items: %s", t.Message())
			return failed, fmt.Errorf(ErrTxConflict)
		case *dynamodb.TransactionInProgressException:
			// no retry
			log.Printf("failed to write items: %s", t.Message())
			return failed, fmt.Errorf(ErrTxInProgress)
		default:
			log.Printf("failed to check items: %v", err)
			return failed, err
		}
	}

	log.Printf("Write TX Success!\n")
	return failed, nil
}

func newTxWriteItem(ti TransactionItem) (*dynamodb.TransactWriteItem, error) {
	req := ti.GetRequest()

	switch req {
	case "C":
		m, err := marshalMap(ti.Item)
		if err != nil {
			log.Printf("newTxWriteItem failed: %v", err)
			return nil, err
		}
		txItem := &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				Item:                      m,
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
			},
		}
		return txItem, nil
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
		return txItem, nil
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
		return txItem, nil
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
		return txItem, nil
	default:
		log.Printf("invalid request type")
		return &dynamodb.TransactWriteItem{}, fmt.Errorf("INVALID_REQUEST_TYPE")
	}

}
