package gosqs

import "testing"

var avs = []MsgAV{
	MsgAV{
		Key:      "department",
		DataType: "String",
		Value:    "IT-Eng",
	},
	MsgAV{
		Key:      "project",
		DataType: "String",
		Value:    "uBook",
	},
}
var SQS = InitSesh()

var avMap = CreateMsgAttributes(avs)

var msg1 = SendMsgOptions{
	DelaySeconds:            SendMsgDefault.DelaySeconds,
	MessageAttributes:       nil,
	MessageBody:             "msg-test001",
	MessageDeduplicationId:  SendMsgDefault.MessageDeduplicationId,
	MessageGroupId:          SendMsgDefault.MessageGroupId,
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

var msg2 = SendMsgOptions{
	DelaySeconds:            SendMsgDefault.DelaySeconds,
	MessageAttributes:       avMap,
	MessageBody:             "msg-test002",
	MessageDeduplicationId:  "test-dedup-001",
	MessageGroupId:          "test-group-001",
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

func TestSendMessage(t *testing.T) {
	var tests = []struct {
		name    string
		options SendMsgOptions
	}{
		{name: "test-001", options: msg1},
		{name: "test-002.fifo", options: msg2},
	}
	for _, test := range tests {
		url, err := GetQueueURL(SQS, test.name)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test.name, err)
		}
		test.options.QueueURL = url
		err = SendMessage(SQS, test.options)
		if err != nil {
			t.Errorf("SendMessage failed: %v", err)
		}
	}
}
