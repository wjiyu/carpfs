package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juicedata/juicefs/pkg/meta"
	"testing"
)

type setting struct {
	Name  string `xorm:"pk"`
	Value string `xorm:"varchar(4096) notnull"`
}

func TestA(t *testing.T) {
	t.Logf("test main!")
	fmt.Println(meta.Shuffle("imagenet_4M", "mysql://root:w995219@(10.151.11.61:3306)/juicefs3", 3))
	//db, err := sql.Open("mysql", "root:w995219@tcp(10.151.11.61:3306)/juicefs2")
	//if err != nil {
	//	t.Log(err)
	//}
	//defer db.Close()
	//
	//// Perform a simple query to test the connection
	//var result string
	//err = db.QueryRow("SELECT 'Hello, MySQL!'").Scan(&result)
	//if err != nil {
	//	t.Log(err)
	//}
	//
	//fmt.Println(result)

}
