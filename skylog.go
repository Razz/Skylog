package main

import (
	"bufio"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
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

func LogReader(logPath string,
	logger *log.Logger,
	logChannel chan LogEvent) {
	fileToRead, fileError := os.Open(logPath)
	if fileError != nil {
		logger.Fatalln(fileError)
	}
	fileReader := bufio.NewReader(fileToRead)
	for {
		line, _, err := fileReader.ReadLine()
		if err == io.EOF {
			time.Sleep(time.Second)
		} else {
			logChannel <- LogEvent{
				location:  logPath,
				message:   string(line),
				timestamp: time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)),
			}
		}
	}
}

func LogSender(logGroupName string,
	logStreamName string,
	logChannel chan LogEvent,
	logger *log.Logger,
	awsConfig aws.Config) {

	var (
		logEvents    []*cloudwatchlogs.InputLogEvent
		nextSeqToken string
	)
	sess := session.New(&awsConfig)
	svc := cloudwatchlogs.New(sess)
	checkTime := time.Now().Unix()

	for {
		select {
		case elm, status := <-logChannel:
			if status {
				//send every 100 log events, or every 2 seconds, if there are any events
				// in the logEvents queue
				if len(logEvents) >= 100 || time.Now().Unix()-checkTime > 2 && len(logEvents) > 1 {
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
						logger.Println(err)
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
					checkTime = time.Now().Unix()
					logEvents = logEvents[:0]
				} else {
					logEvents = append(logEvents, &cloudwatchlogs.InputLogEvent{
						Timestamp: aws.Int64(elm.timestamp), Message: &elm.message})
				}
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

func main() {
	var (
		wg            sync.WaitGroup
		config        map[string]Config
		skylogLogFile *os.File
		credential    *credentials.Credentials
	)

	// Add reading from a config file for keys
	// if they dont exist, try env
	// if those dont exist bail out

	awsConfig := aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credential}

	if _, err := os.Stat("/var/log/skylog.log"); os.IsNotExist(err) {
		skylogLogFile, _ = os.Create("/var/log/skylog.log")
	} else {
		skylogLogFile, _ = os.Open("/var/log/skylog.log")
	}
	logger := log.New(skylogLogFile, "Log:", 0)

	runtime.GOMAXPROCS(2)
	configFile, _ := ioutil.ReadFile("/etc/skylog/skylog.conf")
	if err := yaml.Unmarshal(configFile, &config); err != nil {
		logger.Println(err)
	}

	for _, v := range config {
		fileChannel := make(chan LogEvent, 1000)
		if _, testFileLoc := os.Stat(v.Path); os.IsNotExist(testFileLoc) {
			logger.Fatalln("File and path: ", v.Path, " does not exist, or can  not be opened")
		} else {
			wg.Add(2)
			go LogReader(v.Path, logger, fileChannel)
			go LogSender(v.Group, v.Stream, fileChannel, logger, awsConfig)
		}
	}
	wg.Wait()
	//porbably should add saftey by defering file closes. I dont know if it's
	// needed.
}
