package cmd

import (
	"fmt"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

var err error

const (
	pipe       = "│   "
	tee        = "├── "
	lasttee    = "└── "
	defaultDir = "."
)

type FileNode struct {
	Level    int
	FileName string
	IsDir    bool
	Children []*FileNode
	Parent   *FileNode
	Left     *FileNode
	Right    *FileNode
}

func cmdView() *cli.Command {
	return &cli.Command{
		Name:      "view",
		Action:    view,
		Category:  "TOOL",
		Usage:     "displays the aggregated data set view",
		ArgsUsage: "AGGREGATED DATA SET PATH",
		Description: `It is used to display the aggregated view of the data set.

Examples:
$ juicefs view /home/wjy/pack/imagenet -m "mysql://jfs:mypassword@(127.0.0.1:3306)/juicefs"
# A safer alternative
$ export META_PASSWORD=mypassword 
$ juicefs view /home/wjy/pack/imagenet -m "mysql://jfs:@(127.0.0.1:3306)/juicefs"`,
		Flags: []cli.Flag{
			//&cli.BoolFlag{
			//	Name:    "tree",
			//	Aliases: []string{"t"},
			//	Usage:   "the tree structure displays the view",
			//},

			&cli.BoolFlag{
				Name:    "list",
				Aliases: []string{"l"},
				Value:   true,
				Usage:   "display the view in list format",
			},

			&cli.StringFlag{
				Name:    "meta-url",
				Aliases: []string{"m"},
				Usage:   "META-URL is used to connect the metadata engine (Redis, TiKV, MySQL, etc.)",
			},

			&cli.UintFlag{
				Name:    "works",
				Aliases: []string{"w"},
				Value:   50,
				Usage:   "number of concurrent threads in the thread pool(max number 500)",
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

func view(ctx *cli.Context) error {
	setup(ctx, 1)
	if runtime.GOOS == "windows" {
		logger.Infof("Windows is not supported!")
		return nil
	}

	if ctx.String("meta-url") == "" {
		return os.ErrInvalid
	}

	if ctx.Uint("works") <= 0 || ctx.Uint("works") > 500 {
		return os.ErrInvalid
	}

	path := ctx.Args().Get(0)

	//path is "" or ".", get current path
	if path == "" || path == "." {
		path, err = os.Getwd()
		if err != nil {
			logger.Errorln(err)
			return err
		}
	}

	//path whether exist
	pathInfo, err := os.Stat(path)
	if err != nil {
		logger.Errorln(err)
	}

	var name string
	if os.IsNotExist(err) {
		name = filepath.Base(path)
		path = filepath.Dir(path)
		pathInfo, err = os.Stat(path)
		if err != nil {
			logger.Errorln(err)
			return err
		}
	}

	stat, ok := pathInfo.Sys().(*syscall.Stat_t)
	if !ok {
		logger.Errorf("failed to get inode")
		panic("failed to get inode")
	}

	inode := stat.Ino

	//meta client
	metaUri := ctx.String("meta-url")
	removePassword(metaUri)
	m := meta.NewClient(metaUri, &meta.Config{Retries: 10, Strict: true, MountPoint: ctx.String("mount-point")})
	_, err = m.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
		panic(err)
	}

	mountPaths, _ := m.MountPaths()
	var isMountPath bool
	for _, mountPath := range mountPaths {
		mountPath = filepath.Join(mountPath, "pack")
		if strings.Contains(path, mountPath) {
			isMountPath = true
		}
	}

	if !isMountPath {
		logger.Errorf("path is not under mount path pack!")
		return os.ErrInvalid
	}

	err = viewMetaInfo(ctx, m, meta.Ino(inode), name, pathInfo.IsDir(), int(ctx.Uint("works")), bool(ctx.Bool("compress")))
	if err != nil {
		logger.Errorln(err)
		return err
	}
	return nil
}

func viewMetaInfo(ctx *cli.Context, m meta.Meta, inode meta.Ino, name string, isDir bool, work int, compression bool) error {
	_, files, err := m.GetChunkMetaInfo(meta.Background, inode, name, isDir, work, compression)
	if err != nil {
		logger.Errorln(err)
		return err
	}

	if len(files) <= 0 {
		return os.ErrInvalid
	}

	//sort.Slice(files, func(i, j int) bool {
	//	dirI := filepath.Dir(files[i])
	//	dirJ := filepath.Dir(files[j])
	//	return dirI < dirJ
	//})

	fmt.Println(len(files))
	var wg sync.WaitGroup
	size := int(math.Ceil(float64(len(files)) / float64(work)))
	for i := 0; i < work; i++ {
		start := i * size
		end := start + size
		if i == work-1 {
			end = len(files)
		}
		wg.Add(1)
		go func(slice []string) {
			defer wg.Done()
			for _, file := range slice {
				fmt.Println(file)
			}
		}(files[start:end])
	}

	wg.Wait()
	//if ctx.Bool("list") && !ctx.Bool("tree") {
	//for _, file := range files {
	//	fmt.Println(file)
	//}
	//}

	//if ctx.Bool("tree") {
	//	node := &utils.FileNode{}
	//	err := node.LTree(files)
	//	if err != nil {
	//		return err
	//	}
	//	node.ShowTree("")
	//}
	return nil
}
