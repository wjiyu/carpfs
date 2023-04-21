package cmd

import (
	"archive/tar"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const (
	maxPackSize = 4 * 1024 * 1024 // 4MB
	numWorkers  = 4               // number of workers in the thread pool
)

func cmdPack() *cli.Command {
	return &cli.Command{
		Name:      "pack",
		Action:    pack,
		Category:  "TOOL",
		Usage:     "package small file data sets",
		ArgsUsage: "SOURCE PATH AND DEST PATH",
		Description: `
It is used to package the raw small file data set to the storage system.

Examples:
$ juicefs pack /home/wjy/imagenet /mnt/jfs`,
		Flags: []cli.Flag{
			&cli.UintFlag{
				Name:    "pack-size",
				Aliases: []string{"s"},
				Value:   4,
				Usage:   "size of each pack in MiB(max size 4MB)",
			},

			&cli.UintFlag{
				Name:    "works",
				Aliases: []string{"w"},
				Value:   5,
				Usage:   "number of concurrent threads in the thread pool(max number 20)",
			},
		},
	}
}

func pack(ctx *cli.Context) error {
	setup(ctx, 2)
	if runtime.GOOS == "windows" {
		logger.Infof("Windows is not supported")
		return nil
	}

	if ctx.Uint("pack-size") <= 0 || ctx.Uint("pack-size") > 4 {
		return os.ErrInvalid
	}

	if ctx.Uint("works") <= 0 || ctx.Uint("works") > 20 {
		return os.ErrInvalid
	}

	src := ctx.Args().Get(0)
	dst := ctx.Args().Get(1)

	if src == dst {
		return os.ErrInvalid
	}

	//p, err := filepath.Abs(src)
	//if err != nil {
	//	logger.Errorf("abs of %s: %s", src, err)
	//}
	//d := filepath.Dir(p)
	//name := filepath.Base(p)

	packFolder(filepath.Clean(src), filepath.Clean(dst), int(ctx.Uint("pack-size")), int(ctx.Uint("works")))

	return nil
}

func packFolder(src, dst string, maxSize, numWorkers int) {
	// create a wait group to wait for all workers to finish
	var wg sync.WaitGroup

	// create a channel to receive file paths
	filePaths := make(chan string)

	// create a channel to receive arrays of file paths
	filePathArrays := make(chan []string)

	// create a channel to signal when all workers have finished
	done := make(chan bool)

	metaUri := "mysql://root:w995219@(10.151.11.61:3306)/juicefs3"
	removePassword(metaUri)
	m := meta.NewClient(metaUri, &meta.Config{Retries: 10, Strict: true, MountPoint: "/mnt/jfs2"})
	_, err := m.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}

	// start the workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(src, dst, filePathArrays, &wg, m)
	}

	// start the file path extractor
	go extractFilePaths(src, filePaths)

	// create a slice to hold file paths
	var filePathSlice []string

	// create a variable to hold the total size of the files in the slice
	var totalSize int64

	// create a ticker to periodically check the size of the slice
	ticker := time.NewTicker(time.Second)

	// loop over the file paths received from the extractor
	for filePath := range filePaths {
		// get the size of the file
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			logger.Errorf("Error getting file info for %s: %s", filePath, err)
			continue
		}
		fileSize := fileInfo.Size()

		// if adding the file would exceed the max size, send the slice to the workers
		if totalSize+fileSize > int64(maxSize*1024*1024) {
			// send the slice to the workers
			filePathArrays <- filePathSlice

			// create a new slice to hold file paths
			filePathSlice = []string{filePath}

			// reset the total size
			totalSize = fileSize
		} else {
			// add the file path to the slice
			filePathSlice = append(filePathSlice, filePath)

			// add the file size to the total size
			totalSize += fileSize
		}

		// check if the ticker has ticked
		select {
		case <-ticker.C:
			logger.Debugf("tick: %v", ticker.C)
			// do nothing
		default:
			logger.Debugf("default")
			// do nothing
		}
	}

	// send the final slice to the workers
	filePathArrays <- filePathSlice

	// close the file path arrays channel
	close(filePathArrays)

	// wait for all workers to finish
	go func() {
		wg.Wait()
		done <- true
	}()

	// wait for all workers to finish or for a timeout
	select {
	case <-done:
		logger.Infof("All workers finished!")

		//meta.SyncChunkInfo(m, 0, "", meta.Background)
		//logger.Infof("sync chunk files info finished!")
	case <-time.After(60 * time.Second):
		logger.Infof("Timeout waiting for workers to finish")
	}
}

func extractFilePaths(dirPath string, filePaths chan<- string) {
	// walk the directory tree
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Fatalf("Error walking path %s: %s", path, err)
			return nil
		}

		// if the path is a file, send it to the channel
		if !info.IsDir() {
			filePaths <- path
		}

		return nil
	})

	if err != nil {
		logger.Fatalf("Error walking directory %s: %s", dirPath, err)
	}

	// close the file paths channel
	close(filePaths)
}

func worker(src, dst string, filePathArrays <-chan []string, wg *sync.WaitGroup, m meta.Meta) {
	// loop over the file path arrays received from the channel
	for filePathArray := range filePathArrays {
		//tar name
		var name string
		//logger.Debugf("path array: %v", filePathArray)
		// create a tar file
		tarFile, err := os.CreateTemp(dst, "tar")
		if err != nil {
			logger.Errorf("Error creating tar file: %s", err)
			continue
		}

		name = tarFile.Name()

		logger.Debugf("tar name: %s", name)

		// create a new tar writer
		tarWriter := tar.NewWriter(tarFile)

		// loop over the file paths in the array
		for _, filePath := range filePathArray {
			// open the file
			file, err := os.Open(filePath)
			if err != nil {
				logger.Errorf("Error opening file %s: %s", filePath, err)
				continue
			}

			// get the file info
			fileInfo, err := file.Stat()
			if err != nil {
				logger.Errorf("Error getting file info for %s: %s", filePath, err)
				continue
			}

			// create a new header for the file
			relativePath, _ := filepath.Rel(filepath.Dir(src), filePath)
			header := &tar.Header{
				Name:    relativePath,
				Size:    fileInfo.Size(),
				Mode:    int64(fileInfo.Mode()),
				ModTime: fileInfo.ModTime(),
			}

			// write the header to the tar file
			err = tarWriter.WriteHeader(header)
			if err != nil {
				logger.Errorf("Error writing header for %s: %s", filePath, err)
				continue
			}

			// copy the file contents to the tar file
			_, err = io.Copy(tarWriter, file)
			if err != nil {
				logger.Errorf("Error copying file %s to tar file: %s", filePath, err)
				continue
			}

			// close the file
			err = file.Close()
			if err != nil {
				logger.Errorf("Error closing file %s: %s", filePath, err)
				continue
			}
		}

		// close the tar writer
		err = tarWriter.Close()
		if err != nil {
			logger.Errorf("Error closing tar writer: %s", err)
			continue
		}

		// close the tar file
		err = tarFile.Close()
		if err != nil {
			logger.Errorf("Error closing tar file: %s", err)
			continue
		}

		// remove the file path array from the channel
		//<-filePathArrays

		meta.SyncChunkInfo(m, 0, name, meta.Background)
		logger.Debugf("sync chunk files %s info finished!", name)
	}

	// signal that the worker has finished
	wg.Done()
}
