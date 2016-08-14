package mapreduce

import (
	"hash/fnv"
  "io/ioutil"
  "encoding/json"
  "os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	filedata, err := ioutil.ReadFile(inFile)
	checkErr(err)
	content := string(filedata)

	// Call user-defined map function for the file's content.
	kvpairs := mapF(inFile, content)

	// Stores json encoders for each intermediate file
	fileEncoders := make(map[string]*json.Encoder)

	// Create nReduce number of intermediate files
	for i:= 0; i < nReduce; i++ {
		outputFileName := reduceName(jobName, mapTaskNumber, i)
		outputFile, err := os.Create(outputFileName)
		defer outputFile.Close()

		checkErr(err)

		encoder := json.NewEncoder(outputFile)
		fileEncoders[outputFileName] = encoder
	}

	// Assign kv pairs to corresponding intermediate files for reducer to read.
	for _, kv := range kvpairs {
		key := kv.Key
		reduceTaskNumber := (int(ihash(key)) % nReduce)
		reduceFileName := reduceName(jobName, mapTaskNumber, reduceTaskNumber)

		err := fileEncoders[reduceFileName].Encode(kv)
		checkErr(err)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
