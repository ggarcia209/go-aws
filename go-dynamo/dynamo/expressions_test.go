package dynamo

import (
	"testing"
)

const TABLE = "go-dynamo-test"

func TestNewExpression(t *testing.T) {
	var intf interface{}
	expr := NewExpression()
	intf = expr
	_, ok := intf.(Expression)
	if !ok {
		t.Errorf("FAIL - invalid type")
	}
	t.Logf("SUCCESS")
}

func TestExpressionBuild(t *testing.T) {
	var tests = []struct {
		fieldname string
		quantity  int
		wantErr   error
	}{
		{"count", 2, nil},
		{"quantity", 5, nil},
		{"test", 0, nil}, // Throws unset parameter error if fieldname == ""
	}
	for _, test := range tests {
		// condition = if curr - quantity >= 0
		cond := NewCondition()
		cond.GreaterThanEqual(test.fieldname, test.quantity)

		// update = setMinus - current - quantity
		ud := NewUpdateExpr()
		ud.SetMinus(test.fieldname, test.fieldname, test.quantity, true)

		// build expression
		eb := NewExprBuilder()
		eb.SetCondition(cond)
		eb.SetUpdate(ud)
		expr, err := eb.BuildExpression()
		if err != nil {
			t.Errorf("FAIL %v", err)
			return
		}

		t.Logf("%v", *expr.Condition())
		t.Logf("%v", *expr.Update())
		for k, v := range expr.Names() {
			t.Logf("%s: %s", k, *v)
		}
		for k, v := range expr.Values() {
			t.Logf("%s: %s", k, *v)
		}
	}

}
