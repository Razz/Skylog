package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	//	"github.com/vaughan0/go-ini"
	//	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v2"
	//"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type LogEvent struct {
	location  string
	message   string
	timestamp int64
}

type Config struct {
	Path   string
	Group  string
	Stream string
}

type Configs struct {
	Config []Config
}

func LogReader(logPath string, logChannel chan LogEvent) {
	fmt.Println(logPath)
	file, _ := os.Open(logPath)
	file_reader := bufio.NewReader(file)
	for {
		line, _, err := file_reader.ReadLine()
		if err == io.EOF {
			time.Sleep(time.Second * 2)
		} else {
			logChannel <- LogEvent{
				location:  logPath,
				message:   string(line),
				timestamp: time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)),
			}
		}
	}
}

func LogSender(logGroupName string, logStreamName string, logChannel chan LogEvent) {
	var (
		credential   *credentials.Credentials
		logEvents    []*cloudwatchlogs.InputLogEvent
		nextSeqToken string
	)

	aws_config := aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credential}

	sess := session.New(&aws_config)
	svc := cloudwatchlogs.New(sess)
	checkTime := time.Now().Unix()

	for {
		select {
		case elm, status := <-logChannel:
			if status {
				//send every 100 log events, or every 2 seconds
				if len(logEvents) >= 100 || time.Now().Unix()-checkTime > 2 {
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
				} else {
					logEvents = append(logEvents, &cloudwatchlogs.InputLogEvent{
						Timestamp: aws.Int64(elm.timestamp), Message: &elm.message})
				}
			}
		default:
			time.Sleep(time.Second * 2)
		}
	}
}
func printTest(items []string) {
	for _, i := range items {
		fmt.Println(i)
	}
}

func main() {
	runtime.GOMAXPROCS(2)
	var wg sync.WaitGroup
	var config map[string]Config
	configFile, _ := ioutil.ReadFile("/Users/acornford/skylog/test.conf")
	if err := yaml.Unmarshal(configFile, &config); err != nil {
		fmt.Println(err)
	}

	for _, v := range config {
		fileChannel := make(chan LogEvent, 1000)
		wg.Add(2)
		go LogReader(v.Path, fileChannel)
		go LogSender(v.Group, v.Stream, fileChannel)
		printTest([]string{v.Path, v.Group, v.Stream})
	}
	wg.Wait()
}
