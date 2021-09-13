package gos3

import (
	"fmt"
	"testing"
)

func TestGetObject(t *testing.T) {
	var tests = []struct {
		bucket string
		key    string
		want   error
	}{
		{bucket: "tpillz-presents-dev-2", key: "html/email-receipt-tmpl.html", want: nil},
		{bucket: "tpillz-presents-dev-2", key: "img/pw-banner.jpg", want: nil},
		{bucket: "tpillz-presents-dev", key: "img/pw-banner.jpg", want: fmt.Errorf("ITEM_NOT_FOUND")},
	}
	svc := InitSesh()
	for _, test := range tests {
		_, err := GetObject(svc, test.bucket, test.key)
		if err != nil {
			if test.want == nil {
				t.Errorf("FAIL: %v", err)
			}
			if err.Error() != test.want.Error() {
				t.Errorf("FAIL: %v; want: %v", err, test.want.Error())
			}

		}
		// t.Logf("result: %v", obj)
	}
}
