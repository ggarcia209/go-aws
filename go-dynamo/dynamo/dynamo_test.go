package dynamo

import (
	"fmt"
	"testing"
)

const TableName = "go-dynamo-test"

var svc = InitSesh()

var table = &Table{
	TableName:      TableName,
	PrimaryKeyName: "partition",
	PrimaryKeyType: "string",
	SortKeyName:    "uuid",
	SortKeyType:    "type",
}

var dbInfo = InitDbInfo()

type record struct {
	Partition string          `json:"partition"`
	UUID      string          `json:"uuid"`
	Count     int             `json:"count"`
	Price     float32         `json:"price"`
	Set       map[string]bool `json:"set"`
}

func TestCreateItem(t *testing.T) {
	dbInfo.SetSvc(svc)
	dbInfo.AddTable(table)
	var tests = []struct {
		input record
		want  error
	}{
		{
			record{
				Partition: "A",
				UUID:      "001",
				Count:     3,
				Price:     19.95,
				Set:       map[string]bool{"A": true, "B": false, "C": true},
			}, nil,
		},
		{
			record{
				Partition: "A",
				UUID:      "002",
				Count:     5,
				Price:     9.95,
				Set:       map[string]bool{"A": false, "B": false, "C": true},
			}, nil,
		},
		{
			record{
				Partition: "B",
				UUID:      "003",
				Count:     10,
				Price:     10.00,
				Set:       map[string]bool{"A": false, "B": true, "C": true},
			}, nil,
		},
		{
			record{
				Partition: "C",
				UUID:      "004",
				Count:     0,
				Price:     0.00,
				Set:       map[string]bool{"A": false, "B": false, "C": false},
			}, nil,
		},
	}
	for _, test := range tests {
		err := CreateItem(dbInfo.Svc, test.input, dbInfo.Tables[TableName])
		if err != test.want {
			t.Errorf("FAIL: %v", err)
		}
	}
}

func TestGetItem(t *testing.T) {
	dbInfo.SetSvc(svc)
	dbInfo.AddTable(table)
	var tests = []struct {
		pk       string
		sk       string
		model    *record
		wantUuid string
		wantErr  error
	}{
		{pk: "A", sk: "001", model: &record{}, wantUuid: "001", wantErr: nil},
		{pk: "A", sk: "002", model: &record{}, wantUuid: "002", wantErr: nil},
		{pk: "B", sk: "003", model: &record{}, wantUuid: "003", wantErr: nil},
		{pk: "C", sk: "004", model: &record{}, wantUuid: "004", wantErr: nil},
	}

	for _, test := range tests {
		q := CreateNewQueryObj(test.pk, test.sk)
		item, err := GetItem(dbInfo.Svc, q, dbInfo.Tables[TableName], test.model)
		if err != test.wantErr {
			t.Errorf("FAIL: %v; want: %v", err, test.wantErr)
		}
		if item.(*record).UUID != test.wantUuid {
			t.Errorf("FAIL - DATA: %v; want: %v", item.(*record).UUID, test.wantUuid)
		}
	}
}

func TestUpdateWithCondition(t *testing.T) {
	dbInfo.SetSvc(svc)
	dbInfo.AddTable(table)
	var tests = []struct {
		pk          string
		sk          string
		updateField string
		size        string
		variable    bool
		updateValue interface{}
		wantErr     error
	}{
		{pk: "A", sk: "001", updateField: "count", size: "", variable: true, updateValue: 2, wantErr: nil},                                  // top-level value
		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: nil},                             // nested value - original count: 6
		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: nil},                             // 4
		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: nil},                             // 2
		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: fmt.Errorf(ErrConditionalCheck)}, // 0 - Condition fail
		{pk: "Z", sk: "000", updateField: "count", size: "", variable: true, updateValue: 2, wantErr: nil},                                  // non-existent partition
		{pk: "A", sk: "000", updateField: "count", size: "", variable: true, updateValue: 2, wantErr: nil},                                  // non-existent item
		// {pk: "B", sk: "003", updateField: "price", exprKey: ":p", updateValue: 10.05, wantErr: nil},                                          // orig: "price": 10
		// {pk: "C", sk: "004", updateField: "set", exprKey: ":s", updateValue: map[string]bool{"A": true, "B": true, "C": true}, wantErr: nil}, // orig: "set": ["A":false, "B":false, "C":false]
	}
	for _, test := range tests {
		qt := test.updateValue
		q := CreateNewQueryObj(test.pk, test.sk)
		q.UpdateCurrent(test.updateField, qt)

		keyName := test.updateField
		if test.size != "" {
			keyName = fmt.Sprintf("%s.%s", test.updateField, test.size)
		}

		// condition = if curr - quantity >= 0
		cond := NewCondition()
		cond.GreaterThanEqual(keyName, qt)

		// update = setMinus - current - quantity
		ud := NewUpdateExpr()
		ud.SetMinus(keyName, keyName, qt, test.variable)

		// build expression
		eb := NewExprBuilder()
		eb.SetCondition(cond)
		eb.SetUpdate(ud)
		expr, err := eb.BuildExpresssion()
		if err != nil {
			t.Errorf("FAIL %v", err)
			return
		}

		err = UpdateItem(dbInfo.Svc, q, dbInfo.Tables[TableName], expr)
		if err != nil && test.wantErr != nil {
			if err.Error() != test.wantErr.Error() {
				t.Errorf("FAIL: %v; want: %v", err, test.wantErr)
			}
		}
	}
}
