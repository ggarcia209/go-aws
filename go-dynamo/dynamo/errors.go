package dynamo

import (
	"errors"
	"fmt"
)

var (
	ErrTableNotFound = errors.New("table not found")
	// ErrTxConditionCheckFailed is returned when a transaction item fails it's conditional check.
	// This error cannot be retried.
	ErrTxConditionCheckFailed = errors.New("TX_CONDITION_CHECK_FAILED")
	// ErrTxConflict is returned when another transaction is in progress for a transaction item.
	// This error can be retried.
	ErrTxConflict = errors.New("TX_CONFLICT")
	// ErrTxInProgress is returned when multiple transactions are attempted with the same idempotency key.
	// This error cannot be retried.
	ErrTxInProgress = errors.New("TX_IN_PROGRESS")
	// ErrTxThrottled is returned when a transaction item fails due to throttling.
	// This error can be retried.
	ErrTxThrottled        = errors.New("TX_THROTTLED")
	ErrInvalidRequestType = errors.New("INVALID_REQUEST_TYPE")
)

type TableNotFoundErr struct {
	tableName string
}

func (e *TableNotFoundErr) Error() string {
	return fmt.Sprintf("table %s not found", e.tableName)
}

func NewTableNotFoundErr(tableName string) *TableNotFoundErr {
	return &TableNotFoundErr{tableName: tableName}
}
