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
var sqsTest = InitSesh()

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

var msg3 = SendMsgOptions{
	DelaySeconds:            SendMsgDefault.DelaySeconds,
	MessageAttributes:       avMap,
	MessageBody:             "msg-test003",
	MessageDeduplicationId:  "",
	MessageGroupId:          "",
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

var recMsg1 = RecMsgOptions{}

var recMsg2 = RecMsgOptions{}

// 5/6/2021 - PASS
func TestSendMessage(t *testing.T) {
	var tests = []struct {
		name    string
		options SendMsgOptions
	}{
		{name: "test-001", options: msg1},
		{name: "test-002.fifo", options: msg2},
		{name: "test-002.fifo", options: msg3},
	}
	for _, test := range tests {
		url, err := GetQueueURL(sqsTest, test.name)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test.name, err)
		}
		test.options.QueueURL = url
		err = SendMessage(sqsTest, test.options)
		if err != nil {
			t.Errorf("SendMessage failed: %v", err)
		}
	}
}

// 5/6/2021 - PASS
func TestCheckFifo(t *testing.T) {
	var tests = []struct {
		input string
		want  bool
	}{
		{input: "https://sqs.us-west-2.amazonaws.com/840111470667/test-001", want: false},
		{input: "https://sqs.us-west-2.amazonaws.com/840111470667/test-002.fifo", want: true},
	}
	for _, test := range tests {
		fifo := checkFifo(test.input)
		if fifo != test.want {
			t.Errorf("fail - %v; want: %v", fifo, test.want)
		}
	}
}

// 5/6/2021 - PASS
func TestGenerateDedupID(t *testing.T) {
	var tests = []string{
		"https://sqs.us-west-2.amazonaws.com/840111470667/test-001",
		"https://sqs.us-west-2.amazonaws.com/840111470667/test-002.fifo",
	}
	for _, test := range tests {
		hash := GenerateDedupeID(test)
		t.Logf(hash)
	}
}

// 5/6/2021 - PASS
func TestReceiveMessage(t *testing.T) {
	var tests = []struct {
		name    string
		options RecMsgOptions
	}{
		{name: "test-001", options: RecMsgDefault},
		{name: "test-002.fifo", options: RecMsgDefault},
		{name: "test-002.fifo", options: RecMsgDefault},
	}
	for _, test := range tests {
		url, err := GetQueueURL(sqsTest, test.name)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test.name, err)
		}
		test.options.QueueURL = url
		msgs, err := ReceiveMessage(sqsTest, test.options)
		if err != nil {
			t.Errorf("SendMessage failed: %v", err)
		}
		t.Log(msgs)
	}
}

// 5/6/2021 - PASS
func TestDeleteMessage(t *testing.T) {
	var tests = []struct {
		name    string
		options RecMsgOptions
	}{
		{name: "test-001", options: RecMsgDefault},
		{name: "test-002.fifo", options: RecMsgDefault},
	}
	for _, test := range tests {
		url, err := GetQueueURL(sqsTest, test.name)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test.name, err)
		}
		test.options.QueueURL = url
		msgs, err := ReceiveMessage(sqsTest, test.options)
		if err != nil {
			t.Errorf("ReceiveMessage failed: %v", err)
		}
		for _, msg := range msgs {
			handle := msg.ReceiptHandle
			err = DeleteMessage(sqsTest, url, *handle)
			if err != nil {
				t.Errorf("DeleteMessage failed: %v", err)
			}
		}
	}
}
