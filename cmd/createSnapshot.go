package cmd

import (
	"log"
	"time"
	"fmt"
	"errors"
	"strconv"
	"os/exec"
	"path"
	"strings"
	"os"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)


func createSnapshotDec(b []byte) (proto.Message, error) {
	req := new(hdfs.CreateSnapshotRequestProto)
	return parseRequest(b, req)
}

func createSnapshot(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.CreateSnapshotRequestProto)
	res, err := opfsCreateSnapshot(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func getFileName() string {
	t := time.Now()
	numStr := strconv.Itoa(t.Nanosecond())
        return fmt.Sprintf("s%d%02d%02d-%02d%02d%02d.%.3s", t.Year(), t.Month(), t.Day(),
	t.Hour(), t.Minute(), t.Second(), numStr)
}

func opfsDeleteTmpSnap(src string) {
	f, err := opfs.Open(src)
	if err != nil {
		log.Printf("src %v fail %v", src, err)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err == nil {
		id := fi.Size()
		deleteSnapWithCmd(int(id))
	}
	opfs.RemoveFile(src)
}

const (
	opfsCmdDir = "/usr/libexec/openfs"
	opfsCreateSnapCmd = "openfs-snap-create"
	opfsDeleteSnapCmd = "openfs-snap-delete"
)

var errSnapOpenfsFail error = errors.New("openfs snap operate fail")

func createSnapWithCmd() (int, error) {
	cmdPath := path.Join(opfsCmdDir, opfsCreateSnapCmd)
	cmd := exec.Command(cmdPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("run cmd %v fail %v", cmd, err)
		return 0, errSnapOpenfsFail
	}
	ostr := strings.TrimSuffix(string(output), "\n")
	res := strings.Split(ostr, ",")
	if len(res) != 2 {
		log.Printf("openfs return %v in invalid format", string(output))
		return 0, errSnapOpenfsFail
	}
	ret, err := strconv.Atoi(res[0])
	if err != nil {
		log.Printf("strconv ret %v fail %v", res[0], err)
		return 0, errSnapOpenfsFail
	}
	if ret != 0 {
		log.Printf("openfs operate fail %v", ret)
		return 0, errSnapOpenfsFail
	}

	sid, err := strconv.Atoi(res[1])
	if err != nil {
		log.Printf("strconv sid %v fail %v", res[1], err)
		return 0, errSnapOpenfsFail
	}

	return sid, nil
}

func deleteSnapWithCmd(sid int) error {
	cmdPath := path.Join(opfsCmdDir, opfsDeleteSnapCmd)
	cmd := exec.Command(cmdPath, fmt.Sprintf("%d", sid))
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("run cmd %v fail %v", cmd, err)
		return errSnapOpenfsFail
	}
	ostr := strings.TrimSuffix(string(output), "\n")
	ret, err := strconv.Atoi(ostr)
	if err != nil {
		log.Printf("run cmd %v output %v parse ret fail %v", cmd, string(output), err)
		return errSnapOpenfsFail
	}

	if ret != 0 {
		log.Printf("run cmd %v output %v", cmd, string(output))
		return errSnapOpenfsFail
	}

	return nil
}

func opfsCreateSnapshotWithOpenfs(src string) error {
	sid, err := createSnapWithCmd()
	if err != nil {
		return err
	}
	f, err := opfs.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	err = f.Truncate(int64(sid))
	if err != nil {
		return err
	}

	return nil
}

func opfsCreateSnapshotTransaction(root,name string) error {
	srcTmp := path.Join(hdfsSysDir, tmpdir, GetRandomFileName())

	err := opfs.MakeDirAll(path.Dir(srcTmp), os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	err = opfs.MakeDirAll(path.Dir(srcTmp), os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	f, err := opfs.OpenWithCreate(srcTmp, os.O_WRONLY | os.O_CREATE, os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	defer f.Close()
	defer opfsDeleteTmpSnap(srcTmp)
	if err := opfsCreateSnapshotWithOpenfs(srcTmp); err != nil {
		return err
	}
	srcSnapPath := path.Join(root, snapshotDir, name)
	if root == rootDir {
		srcSnapPath = path.Join(root, hdfsRootSnapDir, name)
	}
	log.Printf("srcSnapPath %v", srcSnapPath)
	if err := opfs.MakeDirAll(path.Dir(srcSnapPath), os.FileMode(defaultConfigPerm)); err != nil {
		log.Printf("mkdir fail %v", err)
		return err
	}
	if err := opfs.Rename(srcTmp, srcSnapPath); err != nil {
		log.Printf("rename from %v to %v fail %v", srcTmp, srcSnapPath, err)
		return err
	}

	return nil
}

var errDisallowSnapshot error = errors.New("disallow snapshot")

func opfsCreateSnapshot(r *hdfs.CreateSnapshotRequestProto) (*hdfs.CreateSnapshotResponseProto, error) {
	root := r.GetSnapshotRoot()
	name := r.GetSnapshotName()

	log.Printf("root %v, name %v", root, name)
	if root != "/" {
		return nil, errOnlySupportRoot
	}

	if !globalFs.GetAllowSnapshot() {
		return nil, errDisallowSnapshot
	}

	if name == "" {
		name = getFileName()
		log.Printf("name %v len %v", name, len(name))
	}

	err := opfsCreateSnapshotTransaction(root, name)
	if err != nil {
		return nil, err
	}

	return &hdfs.CreateSnapshotResponseProto {
		SnapshotPath: proto.String(path.Join(root, ".snapshot", name)),
	}, nil
}
