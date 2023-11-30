package mr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	isActive := true // indicate if worker is active
	for isActive {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.RPCGiveTask", &args, &reply)
		
		if ok {
			switch reply.ReplyType {
			case "Map":
				fmt.Println("The worker receives reply type Map")
				handleMapTask(&args, &reply, mapf)
			case "Reduce":
				fmt.Println("The worker receives reply type Reduce")
				handleReduceTask(&args, &reply, reducef)
			case "Wait":
				fmt.Println("The worker receives reply type Wait")
				time.Sleep(1*time.Second)
			case "Done":
				fmt.Println("The worker receives reply type Done")
				fmt.Println("Coordinator is done. Shut down the worker.")
				isActive = false
			default:
				fmt.Println(errors.New("ReplyType unrecognized. Shut down the worker!"))
				isActive = false
			}
		} else {
			// should crash
			fmt.Printf("The coordinator is shut down. Worker exits.\n") // ????
			isActive = false
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "54.91.102.195:8080")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}

func handleMapTask(args *Args, reply *Reply, mapf func(string, string) []KeyValue) {
	bucket := "aws-logs-853658779161-us-east-1"
	region := "us-east-1"
	// read the file and call mapf
	fileContent, err := os.ReadFile("../" + reply.MapTask.Value) // change to ../ if using bash test, change to ./ if local testing
	if err!=nil {
		currentDir, err := os.Getwd()
		// Print the current working directory
		fmt.Println("Current Directory:", currentDir)
		fmt.Printf("Error when opening file %v\n", err)
		return
	} else {
		// Split Map output into NReduce chunks
		intermediateOutputs := mapf(reply.MapTask.Value, string(fileContent[:])) // mapf takes filename and file content
		mapOutputBuckets := make([][]KeyValue, reply.NReduce)
		for _, pair := range intermediateOutputs{
			ReduceNumber := ihash(pair.Key) % reply.NReduce
			mapOutputBuckets[ReduceNumber] = append(mapOutputBuckets[ReduceNumber], pair)
		}

		// Write NReduce chunks into files naming like mr-X-Y
		MapNumber := reply.MapTask.Key

		// for sorting by key.
		for ReduceNumber, content := range mapOutputBuckets{
			// sort the content
			sort.Sort(ByKey(content))
			intermediateFile := "./mr-" + MapNumber + "-" + strconv.Itoa(ReduceNumber) + ".txt"
			temp := "temp.txt"
			tempFile, err := ioutil.TempFile("", temp)
			if err != nil {
				log.Fatal(err)
				return
			}
			enc := json.NewEncoder(tempFile)
			for _, kv := range content {
				enc.Encode(&kv)
			}
			os.Rename(tempFile.Name(), intermediateFile)

			// open the intermediateFile
			openedFile, err := os.Open(intermediateFile)
			if err != nil {
				fmt.Printf("failed to open intermediate files for map tasks, %v", err)
				return
			}
			defer openedFile.Close()
			// upload files to aws S3
			sess, err := session.NewSession(&aws.Config{
				Region: aws.String(region),
			})

			uploader := s3manager.NewUploader(sess)
			result, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(bucket),
				Key: aws.String(intermediateFile),
				Body: openedFile,
			})
			if err != nil {
				fmt.Printf("failed to upload intermediate files for map tasks, %v", err)
			}else{
				fmt.Printf("file uploaded to, %s\n", aws.StringValue(&result.Location))
			}
		}

		args.StartTime = reply.StartTime
		args.IsMap = true
		args.MapTask = reply.MapTask


		mapFinishOk := call("Coordinator.RPCFinishTask", &args, &reply)
		if !mapFinishOk {
			fmt.Println("Execution time out!")
		}
	}
}

func matchesPattern(fileName string, reducer int) bool {
	// Expected format: "mr-*-5.txt"
	// splits[0] = mr
	// splits[1] is an integer
	// splits[2] = reducer + ".txt"

	splits := strings.Split(fileName, "-")
	return splits[0]=="mr" && splits[1]!="out" && splits[2]==strconv.Itoa(reducer)+".txt"
}

func handleReduceTask(args *Args, reply *Reply, reducef func(string, []string) string){
	// reducef will be called in a loop
	// First group/merge the files for the reducer
	// Create an empty dictionary like {key, [value1, value2...]}
	// Read files that should be handled by the reducer
	// if the key is in the dictionary, append the value to the list
	// if the key doesn't exist, create an entry

	reducer := reply.ReduceTask
	reduceDic := make(map[string][]string)
	// Create a session with S3 on AWS
	bucket := "aws-logs-853658779161-us-east-1"
	region := "us-east-1"

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})

	if err != nil {
		fmt.Println("Error when reading from S3", err)
		return
	}

	// Create an S3 client
	s3Client := s3.New(sess)
	
	// List objects in S3 bucket with the specified prefix
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	result, err := s3Client.ListObjectsV2(input)
	if err != nil {
		fmt.Println("Error when getting files with the specified pattern", err)
		return
	}

	for _, obj := range result.Contents {
		// Get the relavant intermediate files
		if !matchesPattern(*obj.Key, reducer) {
			continue
		}
		// Download the file via obj and get content
		downloadInput := &s3.GetObjectInput {
			Bucket: aws.String(bucket),
			Key: aws.String(*obj.Key),
		}
		output, err := s3Client.GetObject(downloadInput)
		if err != nil {
			fmt.Println("Error when downloading object", err)
			return
		}
		defer output.Body.Close()
		
		// before using NewDecoder, open the file
		fileContent, err := io.ReadAll(output.Body)

		if err != nil {
			fmt.Println("Cannot read the file", err)
			return
		}

		dec := json.NewDecoder(bytes.NewReader(fileContent))
		// process the key-value pairs and put them in reduceDic

		for {
			var kv KeyValue
			if err:= dec.Decode(&kv); err != nil {
				break
			}
			// check if the key is inside reduceDic
			val, ok := reduceDic[kv.Key]
			if ok {
				// the key exists in reduceDic, append the value to the list
				reduceDic[kv.Key] = append(val, kv.Value)		
			}else{
				// create a new entry
				reduceDic[kv.Key] = []string{kv.Value}
			}
		}
	}

	// Apply reducef on the dictionary
	// create the output file
	reduceOutputFile := "./mr-out-" + strconv.Itoa(reducer) + ".txt"
	temp := "tempFile.txt"
	tempFile, err := ioutil.TempFile("./", temp)
	if err != nil {
		log.Fatal(err)
		return
	}
	for key, values := range reduceDic {
		output := reducef(key, values)
		stringToWrite := fmt.Sprintf("%v %v\n", key, output)
		_, err := tempFile.WriteString(stringToWrite)
		if err != nil {
			fmt.Println("Cannot write reduce output", err)
			return
		}
	}
	os.Rename(tempFile.Name(), reduceOutputFile)

	// upload files to aws S3
	uploader := s3manager.NewUploader(sess)
	resultUploader, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key: aws.String(reduceOutputFile),
		Body: bytes.NewReader([]byte(reduceOutputFile)),
	})
	if err != nil {
		fmt.Printf("failed to upload reduce file, %v", err)
	}else{
		fmt.Printf("file uploaded to, %s\n", aws.StringValue(&resultUploader.Location))
	}

	// Notify the coordinator that this is done
	args.StartTime = reply.StartTime
	args.IsMap = false
	args.ReduceTask = reply.ReduceTask

	reduceFinishOk := call("Coordinator.RPCFinishTask", &args, &reply)
	if !reduceFinishOk {
		fmt.Println("Execution time out!")
	}
}