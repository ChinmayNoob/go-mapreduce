package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ChinmayNoob/go-mapreduce/mapreduce"
)

func mapFn(id string, data string) []mapreduce.KeyValue {

	parts := strings.Split(data, "|")
	if len(parts) != 3 {
		return []mapreduce.KeyValue{}
	}
	animeName := strings.TrimSpace(parts[0])
	rating := strings.TrimSpace(parts[1])
	genres := strings.TrimSpace(parts[2])

	genreList := strings.Split(genres, ",")

	var kvs []mapreduce.KeyValue

	for _, genre := range genreList {
		genre = strings.TrimSpace(strings.ToLower(genre))
		if genre != "" {
			value := fmt.Sprintf("%s:%s", animeName, rating)
			kvs = append(kvs, mapreduce.KeyValue{Key: genre, Value: value})
		}
	}
	return kvs
}

func reduceFn(key string, values []string) string {
	var animeNames []string
	var ratings []float64
	var sumRatings float64

	for _, val := range values {
		parts := strings.Split(val, ":")
		if len(parts) == 2 {
			animeName := parts[0]
			rating, err := strconv.ParseFloat(parts[1], 64)
			if err == nil {
				animeNames = append(animeNames, animeName)
				ratings = append(ratings, rating)
				sumRatings += rating
			}
		}
	}

	count := len(animeNames)
	if count == 0 {
		return fmt.Sprintf("%d|%.2f|", 0, 0.0)
	}

	avgRating := sumRatings / float64(count)

	animeList := strings.Join(animeNames, ",")
	return fmt.Sprintf("%d|%.2f|%s", count, avgRating, animeList)
}

type Anime struct {
	Name   string   `json:"name"`
	Rating float64  `json:"rating"`
	Genres []string `json:"genres"`
}

func loadAnimeData(filename string) ([]string, error) {
	// Read JSON file
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %v", filename, err)
	}
	defer file.Close()

	// Read file contents
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %v", filename, err)
	}

	// Parse JSON
	var animeList []Anime
	if err := json.Unmarshal(bytes, &animeList); err != nil {
		return nil, fmt.Errorf("error parsing JSON: %v", err)
	}

	// Convert to string format expected by mapF: "Name|Rating|Genre1,Genre2,..."
	inputs := make([]string, len(animeList))
	for i, anime := range animeList {
		genresStr := strings.Join(anime.Genres, ",")
		inputs[i] = fmt.Sprintf("%s|%.1f|%s", anime.Name, anime.Rating, genresStr)
	}

	return inputs, nil
}

func main() {
	// Load anime data from JSON file
	inputs, err := loadAnimeData("anime_data.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading anime data: %v\n", err)
		os.Exit(1)
	}

	// Create master with loaded entries and 4 reduce partitions
	master := mapreduce.NewMaster(inputs, 4)

	numWorkers := 4
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		w := mapreduce.NewWorker(i, master, mapFn, reduceFn)
		go w.Run(&wg)
	}

	wg.Wait()

	// Parse and format all outputs
	type GenreData struct {
		genre     string
		count     int
		avgRating float64
		anime     []string
	}
	var allGenres []GenreData

	for i := 0; i < master.GetNumReduce(); i++ {
		lines := strings.Split(strings.TrimSpace(master.GetOutputFile(i)), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			// Format from worker: "genre count|avgRating|anime1,anime2,..."
			// Split by "|" first, then extract genre and count from first part
			parts := strings.Split(line, "|")
			if len(parts) < 3 {
				continue
			}

			// First part is "genre count" - need to separate them
			firstPart := strings.TrimSpace(parts[0])
			lastSpaceIdx := strings.LastIndex(firstPart, " ")
			if lastSpaceIdx == -1 {
				continue
			}

			genre := strings.TrimSpace(firstPart[:lastSpaceIdx])
			countStr := strings.TrimSpace(firstPart[lastSpaceIdx+1:])
			count, err1 := strconv.Atoi(countStr)
			avgRating, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			animeStr := strings.TrimSpace(parts[2])

			if err1 == nil && err2 == nil {
				var animeList []string
				if animeStr != "" {
					animeList = strings.Split(animeStr, ",")
					for j := range animeList {
						animeList[j] = strings.TrimSpace(animeList[j])
					}
				}
				allGenres = append(allGenres, GenreData{genre, count, avgRating, animeList})
			}
		}
	}

	// Sort by genre name alphabetically
	sort.Slice(allGenres, func(i, j int) bool {
		return allGenres[i].genre < allGenres[j].genre
	})

	// Print output
	for _, g := range allGenres {
		sort.Strings(g.anime)
		animeList := strings.Join(g.anime, ", ")
		fmt.Printf("%s: %d anime, avg rating %.2f - %s\n", g.genre, g.count, g.avgRating, animeList)
	}
}
