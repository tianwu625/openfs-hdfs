package main
import (
	"fmt"
	"os"
)

func genLDFlags() string {
	ldflagsStr := "-s -w"
	ldflagsStr += " -X github.com/openfs/openfs-hdfs/cmd.GOPATH=" + os.Getenv("GOPATH")
	ldflagsStr += " -X github.com/openfs/openfs-hdfs/cmd.GOROOT=" + os.Getenv("GOROOT")
	return ldflagsStr
}

func main() {
	fmt.Println(genLDFlags())
}
