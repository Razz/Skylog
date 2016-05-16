package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"io"

	//"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type log_event struct {
	location  string
	message   string
	timestamp int64
}

func LogReader(log_file string, log_channel chan log_event) {
	file, _ := os.Open(log_file)
	file_reader := bufio.NewReader(file)
	for {
		line, _, err := file_reader.ReadLine()
		if err == io.EOF {
			time.Sleep(time.Second * 2)
		} else {
			log_channel <- log_event{
				location:  log_file,
				message:   string(line),
				timestamp: time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)),
			}
		}
	}
}

func CreateGroupAndStream(sess *session.Session, stream string, group string) error {
	svc := cloudwatchlogs.New(sess)
	resp, err := svc.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  &group,
		LogStreamName: &stream,
	})
	if err != nil {
		fmt.Println(resp)
	}
	return err
}

func LogSender(log_channel chan log_event) {
	var (
		credential   *credentials.Credentials
		logEvents    []*cloudwatchlogs.InputLogEvent
		nextSeqToken string
	)

	logGroupName := "test3"
	logStreamName := "alexs-mbp"

	aws_config := aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credential}

	sess := session.New(&aws_config)
	svc := cloudwatchlogs.New(sess)
	checkTime := time.Now().Unix()

	for {
		select {
		case elm, status := <-log_channel:
			if status {
				//send every 100 log events, or every 5 seconds
				if len(logEvents) < 100 || time.Now().Unix()-checkTime > 2 {
					logEvents = append(logEvents, &cloudwatchlogs.InputLogEvent{
						Timestamp: aws.Int64(elm.timestamp), Message: &elm.message})
				} else {
					eventInput := cloudwatchlogs.PutLogEventsInput{
						LogGroupName:  &logGroupName,
						LogStreamName: &logStreamName,
						LogEvents:     logEvents,
					}
					if nextSeqToken != "" {
						eventInput.SequenceToken = &nextSeqToken
					}
					resp, err := svc.PutLogEvents(&eventInput)
					for err != nil {
						fmt.Println(err)
						errorString := strings.Split(string(err.Error()), " ")
						issue := strings.TrimSpace(errorString[4])
						switch {
						// needs rewrite incase amazon changes their errors.
						// needs to be something like: 'if "group" in errorString:'
						case issue == "stream":
							_, _ = svc.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
								LogGroupName:  &logGroupName,
								LogStreamName: &logStreamName,
							})
						case issue == "group":
							_, _ = svc.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
								LogGroupName: &logGroupName})
						case strings.TrimSpace(errorString[0]) == "InvalidSequenceTokenException:":
							nextSeqToken = strings.Split(errorString[len(errorString)-6], "\n")[0]
						}
						eventInput = cloudwatchlogs.PutLogEventsInput{
							LogGroupName:  &logGroupName,
							LogStreamName: &logStreamName,
							LogEvents:     logEvents,
						}
						if nextSeqToken != "" {
							eventInput.SequenceToken = &nextSeqToken
						}
						resp, err = svc.PutLogEvents(&eventInput)
					}
					nextSeqToken = *resp.NextSequenceToken
					if resp != nil {
						fmt.Println(resp.GoString())
					}
					checkTime = time.Now().Unix()
					logEvents = logEvents[:0]
				}
			}
		default:
			time.Sleep(time.Second * 2)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(2)
	var wg sync.WaitGroup
	wg.Add(2)
	file_channel := make(chan log_event, 1000000)
	go LogReader("/var/log/system.log", file_channel)
	go LogSender(file_channel)
	wg.Wait()
}
