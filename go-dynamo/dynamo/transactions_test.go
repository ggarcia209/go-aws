package dynamo

import (
	"testing"
)

func TestNewTxItem(t *testing.T) {
	var tests = []struct {
		t *Table
		q *Query
		e Expression
	}{
		{t: &Table{TableName: "test001"}, q: &Query{}, e: Expression{}},
	}

	for _, test := range tests {
		txc := NewCreateTxItem(nil, test.t, test.q, test.e)
		t.Logf("create: %v", txc)
		txu := NewUpdateTxItem(test.t, test.q, test.e)
		t.Logf("update: %v", txu)
		txcc := NewConditionCheckTxItem(test.t, test.q, test.e)
		t.Logf("condition check: %v", txcc)
	}
}

func TestNewTxWriteItem(t *testing.T) {
	var tests = []struct {
		t *Table
		q *Query
		e Expression
	}{
		{t: &Table{TableName: "test001"}, q: &Query{}, e: Expression{}},
	}

	for _, test := range tests {
		txc := newTxWriteItem(NewCreateTxItem(nil, test.t, test.q, test.e))
		t.Logf("create: %v", txc)
		txu := newTxWriteItem(NewUpdateTxItem(test.t, test.q, test.e))
		t.Logf("update: %v", txu)
		txcc := newTxWriteItem(NewConditionCheckTxItem(test.t, test.q, test.e))
		t.Logf("condition check: %v", txcc)
	}
}
