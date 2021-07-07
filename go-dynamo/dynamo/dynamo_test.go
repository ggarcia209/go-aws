package dynamo

import (
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
