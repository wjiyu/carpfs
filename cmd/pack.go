package cmd

import (
	"archive/tar"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
		},
	}
}

func pack(ctx *cli.Context) error {
	setup(ctx, 2)
	if runtime.GOOS == "windows" {
		logger.Infof("Windows is not supported")
		return nil
	}

	if ctx.Uint("pack-size") == 0 || ctx.Uint("pack-size") > 4 {
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

	packFolder(src, dst, int(ctx.Uint("pack-size")))

	return nil
}

func packFolder(src, dst string, maxSize int) {
	dirPath := src
	dir, err := os.Open(dirPath)
	if err != nil {
		panic(err)
	}
	defer dir.Close()

	// Create a tar writer
	tarPath := dst
	tarFile, err := os.Create(tarPath)
	if err != nil {
		panic(err)
	}
	defer tarFile.Close()

	tarWriter := tar.NewWriter(tarFile)
	defer tarWriter.Close()

	var maxPackSize int64 = int64(maxSize * 1024 * 1024) // 4MB

	var currentTarFileSize int64 = 0
	var currentTarFileIndex int = 0
	var currentTarFile *os.File = nil
	defer func() {
		if currentTarFile != nil {
			currentTarFile.Close()
		}
	}()

	var currentTarWriter *tar.Writer = nil

	defer func() {
		if currentTarWriter != nil {
			currentTarWriter.Close()
		}
	}()

	// Walk through the directory and add files to the tar archive
	err = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Error(err)
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		log.Println("path: %s", path)

		// Open the file to be added to the archive
		file, err := os.Open(path)
		if err != nil {
			log.Println(err)
			return err
		}
		defer file.Close()

		// Create a new tar header
		header := &tar.Header{
			Name: path,
			Mode: int64(info.Mode()),
			Size: info.Size(),
		}

		currentTarFileSize += info.Size()

		if currentTarFileSize > maxPackSize {

			if tarWriter != nil {
				if err := tarWriter.Close(); err != nil {
					log.Println(err)
					return err
				}
				tarWriter = nil
			}

			if currentTarWriter != nil {
				if err := currentTarWriter.Close(); err != nil {
					return err
				}
				currentTarWriter = nil
				currentTarFileIndex++
			}

			currentTarFileSize = 0

			currentTarFilePath := dst[:strings.Index(dst, ".tar")] + "_" + strconv.Itoa(currentTarFileIndex) + ".tar"
			currentTarFile, err = os.Create(currentTarFilePath)
			if err != nil {
				return err
			}
			currentTarWriter = tar.NewWriter(currentTarFile)
		}

		if tarWriter != nil {
			//fmt.Println("tar: %v", tarWriter)
			// Write the header to the tar archive
			if err := tarWriter.WriteHeader(header); err != nil {
				log.Println(err)
				return err
			}

			// Copy the file to the tar archive
			if _, err := io.Copy(tarWriter, file); err != nil {
				log.Println(err)
				return err
			}
		}

		if currentTarWriter != nil {
			if err := currentTarWriter.WriteHeader(header); err != nil {
				log.Println(err)
				return err
			}

			if _, err := io.Copy(currentTarWriter, file); err != nil {
				log.Println(err)
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Println(err)
	}

	log.Println("Tar archives created successfully.")
}
