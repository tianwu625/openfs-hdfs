package main

import (
	"os"

	server "github.com/openfs/openfs-hdfs/cmd"
)

func main() {
	server.Main(os.Args)
}
