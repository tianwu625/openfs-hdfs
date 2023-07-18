package cmd

import (
	"log"
)

const (
	actionAppend = "append"
	actionConcat = "concat"
	actionCreate = "create"
	actionCreateSnapshot = "createSnapshot"
	actionDelete = "delete"
	actionDeleteSnapshot = "deleteSnapshot"
)

type posixCheckPermissionRequest struct {
	absPath string
	action string
	user string
	groups []string
}


func checkPosixPermission(args *posixCheckPermissionRequest) bool {
	log.Printf("args %v", args)
	return true
}
