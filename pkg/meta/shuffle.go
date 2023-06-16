package meta

//
//import (
//	"C"
//	"fmt"
//	"github.com/erikdubbelboer/gspt"
//	"github.com/juicedata/juicefs/pkg/utils"
//	"math/rand"
//	"os"
//	"strings"
//	"time"
//)
//
//func removePassword(uri string) {
//	uri2 := utils.RemovePassword(uri)
//	if uri2 != uri {
//		for i, a := range os.Args {
//			if a == uri {
//				os.Args[i] = uri2
//				break
//			}
//		}
//	}
//	gspt.SetProcTitle(strings.Join(os.Args, " "))
//}
//
////export Shuffle
//func Shuffle(name, metaUrl string, size int) []string {
//	var shuffles []string
//	//meta client
//	if metaUrl == "" {
//		panic("meta url is empty!")
//	}
//	name = "imagenet_4M"
//	metaUrl = "mysql://root:w995219@(10.151.11.61:3306)/juicefs3"
//	removePassword(metaUrl)
//	m := NewClient(metaUrl, &Config{Retries: 10, Strict: true})
//	_, err := m.Load(true)
//	if err != nil {
//		logger.Fatalf("load setting: %s", err)
//		panic(err)
//	}
//
//	maps, err := m.GetMetaInfo(name)
//
//	fmt.Println("name: %s, url: %s", name, metaUrl, maps)
//	shuffles, err = ShuffleChunks(maps, size)
//
//	return shuffles
//}
//
//func ShuffleChunks(chunkMaps map[uint64][]string, size int) ([]string, error) {
//	var chunkIds []uint64
//	for key := range chunkMaps {
//		chunkIds = append(chunkIds, key)
//	}
//
//	shuffleIds := groupChunkIds(shuffle(chunkIds).([]uint64), size)
//
//	var shuffleFiles []string
//	for _, groupIds := range shuffleIds {
//		var groupFiles []string
//		for _, chunkId := range groupIds {
//			groupFiles = append(groupFiles, chunkMaps[chunkId]...)
//		}
//		shuffleFiles = append(shuffleFiles, shuffle(groupFiles).([]string)...)
//	}
//	return shuffleFiles, nil
//}
//
//func shuffle(arr interface{}) interface{} {
//	r := rand.New(rand.NewSource(time.Now().Unix()))
//	switch a := arr.(type) {
//	case []uint64:
//		for len(a) > 0 {
//			n := len(a)
//			randIndex := r.Intn(n)
//			a[n-1], a[randIndex] = a[randIndex], a[n-1]
//			a = a[:n-1]
//		}
//	case []string:
//		for len(a) > 0 {
//			n := len(a)
//			randIndex := r.Intn(n)
//			a[n-1], a[randIndex] = a[randIndex], a[n-1]
//			a = a[:n-1]
//		}
//	}
//
//	return arr
//}
//
////func chunk(arr []int, size int) [][]int {
////	var chunks [][]int
////	for len(arr) > 0 {
////		if len(arr) < size {
////			chunks = append(chunks, arr[:])
////			break
////		}
////		chunks = append(chunks, arr[:size])
////		arr = arr[size:]
////	}
////	return chunks
////}
//
//func groupChunkIds(slice []uint64, size int) [][]uint64 {
//	if slice == nil {
//		return nil
//	}
//	if size <= 0 {
//		return [][]uint64{slice}
//	}
//	var result [][]uint64
//	for i := 0; i < len(slice); i += size {
//		end := i + size
//		if end > len(slice) {
//			end = len(slice)
//		}
//		result = append(result, slice[i:end])
//	}
//	return result
//}
//
//func getInoSlice(data interface{}) []Ino {
//	result := []Ino{}
//
//	arr := data.([]Ino)
//
//	for _, item := range arr {
//		result = append(result, item)
//	}
//	return result
//}
//
//func getStringSlice(data interface{}) []string {
//	result := []string{}
//
//	arr := data.([]string)
//
//	for _, item := range arr {
//		result = append(result, item)
//	}
//	return result
//}
//
////type chunkable interface {
////	len() int
////	slice(start, end int) chunkable
////}
////type intSlice []int
////
////func (s intSlice) len() int {
////	return len(s)
////}
////func (s intSlice) slice(start, end int) chunkable {
////	return s[start:end]
////}
////
////type stringSlice []string
////
////func (s stringSlice) len() int {
////	return len(s)
////}
////func (s stringSlice) slice(start, end int) chunkable {
////	return s[start:end]
////}
////func chunk(arr chunkable, size int) []chunkable {
////	var chunks []chunkable
////	for arr.len() > 0 {
////		if arr.len() < size {
////			chunks = append(chunks, arr.slice(0, arr.len()))
////			break
////		}
////		chunks = append(chunks, arr.slice(0, size))
////		arr = arr.slice(size, arr.len())
////	}
////	return chunks
////}
