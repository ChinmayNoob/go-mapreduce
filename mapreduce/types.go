package mapreduce

import (
	"sync"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type (
	MapFunc    func(id string, data string) []KeyValue
	ReduceFunc func(key string, values []string) string
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
)

type Task struct {
	Type       TaskType
	TaskID     int
	ChunkIndex int
	Data       string
}

type Master struct {
	numMap    int
	numReduce int

	inputFiles   []string
	mapStatus    []TaskStatus
	reduceStatus []TaskStatus

	intermediateFiles map[int]map[string][]string
	midMutex          sync.Mutex

	outputFiles map[int]string
	outMutex    sync.Mutex

	mu sync.Mutex
}

type TaskStatus struct {
	Status     string
	AssignedAt time.Time
}
