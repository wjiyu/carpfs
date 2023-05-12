package utils

import (
	"github.com/juicedata/juicefs/pkg/meta"
	"math/rand"
	"time"
)

func ShuffleChunks(chunkMaps map[meta.Ino][]string) ([]string, error) {
	var chunkIds []meta.Ino
	for key := range chunkMaps {
		chunkIds = append(chunkIds, key)
	}

	shuffleIds := groupChunkIds(getInoSlice(shuffle(chunkIds)), 3)

	var shuffleFiles []string
	for _, groupIds := range shuffleIds {
		var groupFiles []string
		for _, chunkId := range groupIds {
			groupFiles = append(groupFiles, chunkMaps[chunkId]...)
		}
		shuffleFiles = append(shuffleFiles, getStringSlice(shuffle(groupFiles))...)
	}
	return shuffleFiles, nil
}

func shuffle(arr interface{}) interface{} {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	switch a := arr.(type) {
	case []meta.Ino:
		for len(a) > 0 {
			n := len(a)
			randIndex := r.Intn(n)
			a[n-1], a[randIndex] = a[randIndex], a[n-1]
			a = a[:n-1]
		}
	case []string:
		for len(a) > 0 {
			n := len(a)
			randIndex := r.Intn(n)
			a[n-1], a[randIndex] = a[randIndex], a[n-1]
			a = a[:n-1]
		}
	}

	return arr
}

//func chunk(arr []int, size int) [][]int {
//	var chunks [][]int
//	for len(arr) > 0 {
//		if len(arr) < size {
//			chunks = append(chunks, arr[:])
//			break
//		}
//		chunks = append(chunks, arr[:size])
//		arr = arr[size:]
//	}
//	return chunks
//}

func groupChunkIds(slice []meta.Ino, size int) [][]meta.Ino {
	if slice == nil {
		return nil
	}
	if size <= 0 {
		return [][]meta.Ino{slice}
	}
	var result [][]meta.Ino
	for i := 0; i < len(slice); i += size {
		end := i + size
		if end > len(slice) {
			end = len(slice)
		}
		result = append(result, slice[i:end])
	}
	return result
}

func getInoSlice(data interface{}) []meta.Ino {
	result := []meta.Ino{}
	// 使用类型断言将接口类型数据转换为 []interface{} 类型
	arr := data.([]interface{})
	// 遍历数组并将每个元素转换为 uint64 类型
	for _, item := range arr {
		result = append(result, item.(meta.Ino))
	}
	return result
}

func getStringSlice(data interface{}) []string {
	result := []string{}
	// 使用类型断言将接口类型数据转换为 []interface{} 类型
	arr := data.([]interface{})
	// 遍历数组并将每个元素转换为字符串类型
	for _, item := range arr {
		result = append(result, item.(string))
	}
	return result
}

//type chunkable interface {
//	len() int
//	slice(start, end int) chunkable
//}
//type intSlice []int
//
//func (s intSlice) len() int {
//	return len(s)
//}
//func (s intSlice) slice(start, end int) chunkable {
//	return s[start:end]
//}
//
//type stringSlice []string
//
//func (s stringSlice) len() int {
//	return len(s)
//}
//func (s stringSlice) slice(start, end int) chunkable {
//	return s[start:end]
//}
//func chunk(arr chunkable, size int) []chunkable {
//	var chunks []chunkable
//	for arr.len() > 0 {
//		if arr.len() < size {
//			chunks = append(chunks, arr.slice(0, arr.len()))
//			break
//		}
//		chunks = append(chunks, arr.slice(0, size))
//		arr = arr.slice(size, arr.len())
//	}
//	return chunks
//}
