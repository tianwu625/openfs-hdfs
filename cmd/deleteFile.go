package cmd

import (
	"log"
	"path"
	"os"
	"errors"
	"io"
	"syscall"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

var errNotEmpty = errors.New("Not empty directory")

func deleteFileDec(b []byte) (proto.Message, error) {
	req := new(hdfs.DeleteRequestProto)
	return parseRequest(b, req)
}

func deleteFile(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.DeleteRequestProto)
	log.Printf("src %v\nrecursive %v\n", req.GetSrc(), req.GetRecursive())
	res, err := opfsDeleteFile(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsRemoveAllPath(dirPath string) (err error) {
	f, err := opfs.Open(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if fi.IsDir() {
		names, err := f.Readdirnames(-1)
		if err != nil && err != io.EOF {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for _, name := range names {
			newPath := path.Clean(path.Join(dirPath, name))
			err := opfsRemoveAllPath(newPath)
			if err != nil {
				return err
			}
		}
		err = opfs.RemoveDir(dirPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			} else if errors.Is(err, syscall.ENOTEMPTY) {
				return errNotEmpty
			}
			return err
		}
	} else {
		err := opfs.RemoveFile(dirPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func opfsDeleteFile(r *hdfs.DeleteRequestProto) (*hdfs.DeleteResponseProto, error) {
	res := new(hdfs.DeleteResponseProto)
	res.Result = proto.Bool(false)
	src := r.GetSrc()
	recursive := r.GetRecursive()
	f, err := opfs.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return res, nil
		}
		log.Printf("fail open %v\n", err)
		return res, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Printf("fail to Stat %v\n", err)
		return res, err
	}

	if recursive {
		err := opfsRemoveAllPath(src)
		if err != nil {
			log.Printf("fail to remove all path %v\n", err)
			return res, err
		}
	} else if !recursive && fi.IsDir() {
		err := opfs.RemoveDir(src)
		if err != nil {
			return res, err
		}
	} else if !recursive && !fi.IsDir() {
		err := opfs.RemoveFile(src)
		if err != nil {
			return res, err
		}
	}
	res.Result = proto.Bool(true)
	return res, nil
}
