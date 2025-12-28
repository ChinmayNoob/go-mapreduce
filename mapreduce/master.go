package mapreduce

import "time"

func NewMaster(inputs []string, numReduce int) *Master {
	m := &Master{
		numMap:            len(inputs),
		numReduce:         numReduce,
		inputFiles:        inputs,
		mapStatus:         make([]TaskStatus, len(inputs)),
		reduceStatus:      make([]TaskStatus, numReduce),
		intermediateFiles: make(map[int]map[string][]string),
		outputFiles:       make(map[int]string),
	}

	for i := range m.mapStatus {
		m.mapStatus[i].Status = "idle"
	}
	for i := range m.reduceStatus {
		m.reduceStatus[i].Status = "idle"
	}
	for i := 0; i < numReduce; i++ {
		m.intermediateFiles[i] = make(map[string][]string)
	}
	return m
}

func (m *Master) RequestTask() Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	// assign idle map tasks
	for i := 0; i < m.numMap; i++ {
		if m.mapStatus[i].Status == "idle" {
			m.mapStatus[i].Status = "in-progress"
			m.mapStatus[i].AssignedAt = time.Now()

			return Task{
				Type:       MapTask,
				TaskID:     i,
				ChunkIndex: i,
				Data:       m.inputFiles[i],
			}
		}
	}

	// reassinng timed out map tasks
	for i := 0; i < m.numMap; i++ {
		if m.mapStatus[i].Status == "in-progress" && time.Since(m.mapStatus[i].AssignedAt) > 5*time.Second {
			m.mapStatus[i].AssignedAt = time.Now()
			return Task{
				Type:       MapTask,
				TaskID:     i,
				ChunkIndex: i,
				Data:       m.inputFiles[i],
			}
		}
	}

	allMapDone := true
	for i := 0; i < m.numMap; i++ {
		if m.mapStatus[i].Status != "done" {
			allMapDone = false
			break
		}
	}
	if !allMapDone {
		return Task{
			Type: NoTask,
		}
	}

	// assign idle reduce tasks
	for i := 0; i < m.numReduce; i++ {
		if m.reduceStatus[i].Status == "idle" {
			m.reduceStatus[i].Status = "in-progress"
			m.reduceStatus[i].AssignedAt = time.Now()
			return Task{
				Type:       ReduceTask,
				TaskID:     i,
				ChunkIndex: i,
			}
		}
	}

	for i := 0; i < m.numReduce; i++ {
		if m.reduceStatus[i].Status == "in-progress" && time.Since(m.reduceStatus[i].AssignedAt) > 10*time.Second {
			m.reduceStatus[i].AssignedAt = time.Now()
			return Task{
				Type:       ReduceTask,
				TaskID:     i,
				ChunkIndex: i,
			}
		}
	}

	return Task{
		Type: NoTask,
	}
}

func (m *Master) ReportMapSuccess(mapID int, partitions map[int][]KeyValue) {
	m.midMutex.Lock()
	for reduce, kvs := range partitions {
		for _, kv := range kvs {
			m.intermediateFiles[reduce][kv.Key] = append(m.intermediateFiles[reduce][kv.Key], kv.Value)
		}
	}
	m.midMutex.Unlock()
	m.mu.Lock()
	m.mapStatus[mapID].Status = "done"
	m.mu.Unlock()
}

func (m *Master) ReportReduceSuccess(reduceID int, outputFile string) {
	m.outMutex.Lock()
	m.outputFiles[reduceID] = outputFile
	m.outMutex.Unlock()
	m.mu.Lock()
	m.reduceStatus[reduceID].Status = "done"
	m.mu.Unlock()
}

func (m *Master) GetReducePartitions(reduceID int) map[string][]string {
	m.midMutex.Lock()
	defer m.midMutex.Unlock()

	copyMap := make(map[string][]string)

	for key, values := range m.intermediateFiles[reduceID] {
		copyVal := make([]string, len(values))
		copy(copyVal, values)
		copyMap[key] = copyVal
	}
	return copyMap
}

func (m *Master) Success() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := 0; i < m.numMap; i++ {
		if m.mapStatus[i].Status != "done" {
			return false
		}
	}
	for i := 0; i < m.numReduce; i++ {
		if m.reduceStatus[i].Status != "done" {
			return false
		}
	}
	return true
}

func (m *Master) GetNumReduce() int {
	return m.numReduce
}

func (m *Master) GetOutputFile(reduceID int) string {
	m.outMutex.Lock()
	defer m.outMutex.Unlock()
	return m.outputFiles[reduceID]
}
