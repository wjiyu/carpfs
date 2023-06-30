package cmd

import (
	"archive/tar"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
)

//const (
//	//maxChunkSize = 4 * 1024 * 1024 // 4MB
//	numWorkers   = 4               // number of workers in the thread pool
//)

var (
	mutex sync.Mutex
	count uint64
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
$ juicefs pack /home/wjy/imagenet /mnt/jfs -m "mysql://jfs:mypassword@(127.0.0.1:3306)/juicefs"
# A safer alternative
$ export META_PASSWORD=mypassword 
$ juicefs pack /home/wjy/imagenet /mnt/jfs -m "mysql://jfs:@(127.0.0.1:3306)/juicefs"`,
		Flags: []cli.Flag{
			&cli.UintFlag{
				Name:    "pack-size",
				Aliases: []string{"s"},
				Value:   3,
				Usage:   "size of each pack in MiB(max size 4MB)",
			},

			&cli.UintFlag{
				Name:    "works",
				Aliases: []string{"w"},
				Value:   5,
				Usage:   "number of concurrent threads in the thread pool(max number 100)",
			},

			&cli.UintFlag{
				Name:    "scan-threads",
				Aliases: []string{"t"},
				Value:   5,
				Usage:   "number of scan dir concurrent threads in the thread pool(max number 100)",
			},

			&cli.UintFlag{
				Name:    "partition-threads",
				Aliases: []string{"r"},
				Value:   5,
				Usage:   "number of partition concurrent threads in the thread pool(max number 200)",
			},

			&cli.StringFlag{
				Name:    "meta-url",
				Aliases: []string{"m"},
				Usage:   "META-URL is used to connect the metadata engine (Redis, TiKV, MySQL, etc.)",
			},

			&cli.StringFlag{
				Name:    "mount-point",
				Aliases: []string{"p"},
				Usage:   "mount path",
			},
			&cli.BoolFlag{
				Name:    "compress",
				Aliases: []string{"c"},
				Value:   true,
				Usage:   "compressed aggregate data ",
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

	if ctx.Uint("works") <= 0 || ctx.Uint("works") > 100 {
		return os.ErrInvalid
	}

	if ctx.Uint("scan-threads") <= 0 || ctx.Uint("scan-threads") > 100 {
		return os.ErrInvalid
	}

	if ctx.Uint("partition-threads") <= 0 || ctx.Uint("partition-threads") > 200 {
		return os.ErrInvalid
	}

	if ctx.String("meta-url") == "" {
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

	packChunk(ctx, filepath.Clean(src), filepath.Clean(dst))

	return nil
}

func packChunk(ctx *cli.Context, src, dst string) {
	// create a wait group to wait for all workers to finish
	var wg sync.WaitGroup

	maxChunkSize := int(ctx.Uint("pack-size"))

	numWorkers := int(ctx.Uint("works"))

	scanWorkers := int(ctx.Uint("scan-threads"))

	partitionWorkers := int(ctx.Uint("partition-threads"))

	filePaths := make(chan map[string]int64)

	filePathArrays := make(chan []string)

	done := make(chan bool)

	//meta client
	metaUri := ctx.String("meta-url")
	removePassword(metaUri)
	m := meta.NewClient(metaUri, &meta.Config{Retries: 10, Strict: true, MountPoint: ctx.String("mount-point")})
	_, err := m.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
		panic(err)
	}

	//defer m.CloseSession()

	// start the workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(m, src, dst, filePathArrays, &wg, ctx.Bool("compress"))
	}

	// scan data set paths
	//go scanPaths(src, filePaths)
	// create a wait group to wait for all scan workers to finish
	// Define the semaphore to limit the number of threads
	sem := make(chan struct{}, scanWorkers)
	var wgScan sync.WaitGroup
	// scan data set paths
	wg.Add(1)
	wgScan.Add(1)
	go func() {
		defer func() {
			wg.Done()
			wgScan.Done()
		}()

		if err := scanPaths(src, filePaths, sem, &wgScan); err != nil {
			logger.Errorf("Error scanning directory: %v\n", err)
			os.Exit(1)
		}
		//close(filePaths)
	}()

	// Wait for all scan works to finish
	go func() {
		wgScan.Wait()
		close(filePaths)
	}()

	//// create a slice to hold file paths
	//var filePathSlice []string
	//
	//// create a variable to hold the total size of the files in the slice
	//var totalSize int64

	// create a ticker to periodically check the size of the slice
	//ticker := time.NewTicker(time.Second)

	pathSlices := make(chan []string)

	var wgPart sync.WaitGroup
	for i := 0; i < partitionWorkers; i++ {
		wg.Add(1)
		wgPart.Add(1)
		go func() {
			defer wg.Done()
			defer wgPart.Done()
			// create a variable to hold the total size of the files in the slice
			var totalSize int64
			// create a slice to hold file paths
			var filePathSlice []string
			// loop over the file paths received from the scan
			for fileMap := range filePaths {
				for filePath, fileSize := range fileMap {
					//// get the size of the file
					//fileInfo, err := os.Stat(filePath)
					//if err != nil {
					//	logger.Errorf("Error getting file info for %s: %s", filePath, err)
					//	continue
					//}
					//fileSize := fileInfo.Size()

					// if adding the file would exceed the max size, send the slice to the workers
					if totalSize+fileSize > int64(maxChunkSize*1024*1024) {
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

				}
				//check if the ticker has ticked
				select {
				//case <-ticker.C:
				//	logger.Debugf("tick: %v", ticker.C)
				// do nothing
				default:
					//logger.Debugf("default")
					// do nothing
				}
			}

			// send the final slice to the workers
			//sliceMap := make(map[int64][]string)
			//sliceMap[totalSize] = filePathSlice
			pathSlices <- filePathSlice
			//filePathArrays <- filePathSlice
		}()
	}

	//// loop over the file paths received from the scan
	//for fileMap := range filePaths {
	//	for filePath, fileSize := range fileMap {
	//		//// get the size of the file
	//		//fileInfo, err := os.Stat(filePath)
	//		//if err != nil {
	//		//	logger.Errorf("Error getting file info for %s: %s", filePath, err)
	//		//	continue
	//		//}
	//		//fileSize := fileInfo.Size()
	//
	//		// if adding the file would exceed the max size, send the slice to the workers
	//		if totalSize+fileSize > int64(maxChunkSize*1024*1024) {
	//			// send the slice to the workers
	//			filePathArrays <- filePathSlice
	//
	//			// create a new slice to hold file paths
	//			filePathSlice = []string{filePath}
	//
	//			// reset the total size
	//			totalSize = fileSize
	//		} else {
	//			// add the file path to the slice
	//			filePathSlice = append(filePathSlice, filePath)
	//
	//			// add the file size to the total size
	//			totalSize += fileSize
	//		}
	//	}
	//	// check if the ticker has ticked
	//	select {
	//	case <-ticker.C:
	//		logger.Debugf("tick: %v", ticker.C)
	//		// do nothing
	//	default:
	//		//logger.Debugf("default")
	//		// do nothing
	//	}
	//}
	//
	//// send the final slice to the workers
	//filePathArrays <- filePathSlice
	//
	//// close the file path arrays channel
	//close(filePathArrays)

	wg.Add(1)
	go scanSlice(pathSlices, filePathArrays, &wg, int64(maxChunkSize*1024*1024))
	// Wait for all scan works to finish
	go func() {
		wgPart.Wait()
		close(pathSlices)
	}()

	// wait for all workers to finish
	go func() {
		wg.Wait()
		done <- true
	}()

	// wait for all workers to finish or for a timeout
	select {
	case <-done:
		logger.Infof("All workers finished!")
		//case <-time.After(60 * time.Second):
		//	logger.Infof("Timeout waiting for workers to finish")
	}
}

func scanSlice(pathSlices <-chan []string, filePathArrays chan<- []string, wg *sync.WaitGroup, maxChunkSize int64) {
	var newSlices []string
	var sumSize int64
	defer func() {
		// close the file path arrays channel
		close(filePathArrays)
		wg.Done()
	}()
	// create a variable to hold the total size of the files in the slice
	for pathSlice := range pathSlices {
		//log.Printf("pathSlice: %v", pathSlice)
		for _, filePath := range pathSlice {
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				logger.Errorf("Error getting file info for %s: %s", filePath, err)
				continue
			}
			fileSize := fileInfo.Size()

			// if adding the file would exceed the max size, send the slice to the workers
			if sumSize+fileSize > maxChunkSize {
				// send the slice to the workers
				filePathArrays <- newSlices

				// create a new slice to hold file paths
				newSlices = []string{filePath}

				// reset the total size
				sumSize = fileSize
			} else {
				// add the file path to the slice
				newSlices = append(newSlices, filePath)

				// add the file size to the total size
				sumSize += fileSize
			}
		}
	}

	filePathArrays <- newSlices
}

func scanPaths(dirPath string, filePaths chan<- map[string]int64, sem chan struct{}, wg *sync.WaitGroup) error {
	//extract dir list
	files, err := os.ReadDir(dirPath)
	if err != nil {
		logger.Fatalf("Error read directory: %v\n", err)
	}

	for _, file := range files {
		if file.IsDir() {
			// Acquire a semaphore before starting a new worker
			sem <- struct{}{}
			wg.Add(1)
			path := filepath.Join(dirPath, file.Name())
			//log.Printf("dir: %s", path)
			go func(path string) {
				defer func() {
					// Release the semaphore after the worker is done
					<-sem
					wg.Done()
				}()
				if err := scanPaths(path, filePaths, sem, wg); err != nil {
					logger.Debugf("Error scanning directory: %v\n", err)
				}
			}(path)
			// Skip processing the directory in the current goroutine
			continue
		}

		fileMap := make(map[string]int64)
		info, _ := file.Info()
		fileMap[filepath.Join(dirPath, file.Name())] = info.Size()

		// if the path is a file, send it to the channel
		filePaths <- fileMap
	}

	return err
}

//func scanPaths(dirPath string, filePaths chan<- string) {
//	// walk the directory tree
//	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
//		if err != nil {
//			logger.Fatalf("Error walking path %s: %s", path, err)
//			return nil
//		}
//
//		//ignore hidden file and folder
//		_, fileName := filepath.Split(path)
//		if fileName == "" || strings.HasPrefix(path, ".") {
//			logger.Debugf("ignore hidden file!")
//			return nil
//		}
//
//		// if the path is a file, send it to the channel
//		if !info.IsDir() {
//			filePaths <- path
//		}
//
//		return nil
//	})
//
//	if err != nil {
//		logger.Fatalf("Error walking directory %s: %s", dirPath, err)
//	}
//
//	// close the file paths channel
//	close(filePaths)
//}

func worker(m meta.Meta, src, dst string, filePathArrays <-chan []string, wg *sync.WaitGroup, compression bool) {

	// Create a directory for the destination path
	dstDir := dst + string(os.PathSeparator) + "pack" + string(os.PathSeparator) + filepath.Base(src)
	// Check if the directory exists
	if _, err := os.Stat(dstDir); err != nil {
		// Create the directory if it does not exist
		err = os.MkdirAll(dstDir, os.ModePerm)
		if err != nil {
			logger.Error(err)
			panic(err)
		}
	}

	// loop over the file path arrays received from the channel
	for filePathArray := range filePathArrays {
		//process tar file name
		mutex.Lock()
		tarName := dstDir + string(os.PathSeparator) + filepath.Base(src) + "_" + strconv.FormatUint(count, 10)
		count++
		mutex.Unlock()

		// create a tar file
		tarFile, err := os.Create(tarName)
		if err != nil {
			logger.Errorf("Error creating tar file: %s", err)
			continue
		}

		name := tarFile.Name()
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

		//sync chunk file list info to table
		err = meta.SyncChunkInfo(meta.Background, m, 0, filepath.Base(name), compression)
		if err != nil {
			logger.Errorf("sync chunk file info error: %s", err)
			continue
		}
		logger.Debugf("sync chunk files %s info finished!", name)
	}

	// signal that the worker has finished
	defer func() {
		wg.Done()
	}()
}
