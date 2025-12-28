# go-mapreduce

This is implemented from the 2004 White Paper from Google Regarding MapReduce [OSDI 2004](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

## 1. Input Data Splitting

### Concept from Paper:
> "Map invocations are distributed across multiple machines by automatically partitioning the input data into a set of M splits."

### In Our Project: 

 - Each anime entry is one input split, each split is stored in masters.inputFile[i]


 ## 2. Parallel Map Processing 

### Concept from Paper:
> "The input splits can be processed in parallel by different machines."

### In Our Project:

**Workers**: 4 worker goroutines (simulating 4 different machines)

- Master Assigns idle map tasks to available workers and they process different splits simultaneously 

```
Worker 0 ──→ Map Task 1  ──→ Processes "One Piece|9.0|..."
Worker 1 ──→ Map Task 3  ──→ Processes "Dragon Ball Z|..."
Worker 2 ──→ Map Task 6  ──→ Processes "My Hero Academia|..."
Worker 3 ──→ Map Task 0  ──→ Processes "Naruto|8.5|..."
```

## 3. Intermediate Key Partitioning (R Partitions)

### Concept from Paper:
> "Reduce invocations are distributed by partitioning the intermediate key space into R pieces using a partitioning function (e.g., hash(key) mod R)."

### In Our Project:

**Partitioning Function**: `hash(key) mod R`
```go
// mapreduce/worker.go lines 46-50
partitions := make(map[int][]KeyValue)
for _, kv := range kvs {
    r := ihash(kv.Key) % w.Master.NumReduce  // hash(genre) % 4
    partitions[r] = append(partitions[r], kv)
}
```

**Hash Function:**
```go
// mapreduce/utils.go lines 7-11
func ihash(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32() & 0x7fffffff)
}
```

**R = 4** (number of reduce partitions, specified by user)

```go
// main.go line 128
master := mapreduce.NewMaster(inputs, 4)  // R = 4
```

**How It Works:**

1. **Map Phase Output**: Each map task produces key-value pairs
   ```
   Map Task 0 (Naruto) produces:
   - ("action", "Naruto:8.5")
   - ("adventure", "Naruto:8.5")
   - ("shounen", "Naruto:8.5")
   ```

2. **Partitioning**: Each key is hashed and assigned to a partition
   ```
   ihash("action") % 4 = ? → Partition 3
   ihash("adventure") % 4 = ? → Partition 2
   ihash("shounen") % 4 = ? → Partition 1
   ```

3. **Intermediate Storage**: Keys are grouped by partition
   ```go
   // mapreduce/master.go lines 98-105
   func (m *Master) ReportMapDone(mapID int, partitions map[int][]KeyValue) {
       for r, kvs := range partitions {
           for _, kv := range kvs {
               m.intermediate[r][kv.Key] = append(m.intermediate[r][kv.Key], kv.Value)
           }
       }
   }
   ```

**Visual Representation:**
```
After All Map Tasks Complete:

Intermediate Data Structure:
┌─────────────────────────────────────────┐
│ Partition 0 (Reducer 0):                │
│   "drama": ["Attack on Titan:9.2", ...] │
│   "psychological": ["Death Note:9.0"]   │
│   "slice of life": ["Spy x Family:8.5"] │
├─────────────────────────────────────────┤
│ Partition 1 (Reducer 1):                │
│   "fantasy": ["Spirited Away:8.6"]      │
│   "supernatural": ["Demon Slayer:8.7",  │
│                    "Spirited Away:8.6", │
│                    "Your Name:8.4"]     │
├─────────────────────────────────────────┤
│ Partition 2 (Reducer 2):                │
│   "comedy": ["One Piece:9.0", ...]      │
│   "mystery": ["Death Note:9.0"]         │
├─────────────────────────────────────────┤
│ Partition 3 (Reducer 3):                │
│   "action": ["Naruto:8.5",              │
│             "Attack on Titan:9.2", ...] │
│   "adventure": ["Naruto:8.5", ...]      │
│   "shounen": ["Naruto:8.5", ...]        │
└─────────────────────────────────────────┘
R = 4 partitions
```

## 4. Reduce Task Distribution

### Concept from Paper:
> "Reduce invocations are distributed by partitioning the intermediate key space into R pieces."

### In Our Project:

**Reduce Tasks**: One per partition (R = 4)

**Parallel Reduce Processing:**
- Each worker can process a different reduce partition
- All reduce tasks run in parallel (after all map tasks complete)

**Code Location:**
```go
// mapreduce/worker.go lines 55-70
func (w *Worker) doReduce(task Task) {
    partition := w.Master.GetReducePartition(task.ChunkIndex)  // Get partition data
    
    // Process all keys in this partition
    for _, k := range keys {
        res := w.reduceFn(k, partition[k])  // Aggregate by key
        ...
    }
}
```

**Visual Representation:**
```
Reduce Phase (after all maps complete):

Worker 0 ──→ Reduce Task 0 ──→ Processes Partition 0
            (drama, psychological, slice of life)

Worker 1 ──→ Reduce Task 1 ──→ Processes Partition 1
            (fantasy, supernatural, historical, ...)

Worker 2 ──→ Reduce Task 2 ──→ Processes Partition 2
            (comedy, mystery, romance, ...)

Worker 3 ──→ Reduce Task 3 ──→ Processes Partition 3
            (action, adventure, shounen, thriller)
```

---

## Complete Data Flow

```
1. INPUT SPLITTING (M = 12)
   ┌─────────────────────────────────────┐
   │ anime_data.json (12 anime entries)  │
   └──────────────┬──────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────┐
   │ Split into 12 map tasks             │
   │ (one per anime entry)               │
   └──────────────┬──────────────────────┘

2. PARALLEL MAP PHASE
   ┌─────────────────────────────────────┐
   │ 4 Workers process 12 tasks          │
   │ in parallel                         │
   └──────────────┬──────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────┐
   │ Intermediate Key-Value Pairs:       │
   │ ("action", "Naruto:8.5")            │
   │ ("adventure", "Naruto:8.5")         │
   │ ("comedy", "One Piece:9.0")         │
   │ ...                                 │
   └──────────────┬──────────────────────┘

3. PARTITIONING (hash(key) mod R, R = 4)
   ┌─────────────────────────────────────┐
   │ Partition by hash(genre) % 4        │
   └──────────────┬──────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────┐
   │ Partition 0: drama, psychological   │
   │ Partition 1: fantasy, supernatural  │
   │ Partition 2: comedy, mystery        │
   │ Partition 3: action, adventure      │
   └──────────────┬──────────────────────┘

4. PARALLEL REDUCE PHASE
   ┌─────────────────────────────────────┐
   │ 4 Workers process 4 partitions      │
   │ in parallel                         │
   └──────────────┬──────────────────────┘
                  │
                  ▼
   ┌─────────────────────────────────────┐
   │ Final Output:                       │
   │ Genre statistics aggregated         │
   └─────────────────────────────────────┘
```

---