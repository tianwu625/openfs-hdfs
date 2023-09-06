package cmd

import (
	"net"
	"errors"
	"path"
	"os"
	"io"
	"bytes"
	"log"
	"syscall"

	jsoniter "github.com/json-iterator/go"
	"github.com/openfs/openfs-hdfs/internal/opfs"
)

const (
	defaultMinioPort = "9000"
	defaultMinioWebPort = "9001"
)

var errOpfsOccupation error = errors.New("openfs occupy this port")

func checkOpfsOccupy(port string) error {
	if port == defaultMinioPort ||
	   port == defaultMinioWebPort {
		return errOpfsOccupation
	}

	return nil
}

func checkAddressValid(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}

	if err := checkOpfsOccupy(port); err != nil {
		return err
	}

	return nil
}

func loadFromConfig(src string, object interface{}) error {
	b, err := opfsReadAll(src)
	if err != nil {
		return err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(b, object); err != nil {
		return err
        }

	return nil
}

func saveToConfig(src string, object interface{}) error {
	srcTmp := path.Join(hdfsSysDir, tmpdir, GetRandomFileName())

	err := opfs.MakeDirAll(path.Dir(srcTmp), os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	f, err := opfs.OpenWithCreate(srcTmp, os.O_WRONLY | os.O_CREATE, os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	defer f.Close()
	defer cleanTmp(srcTmp)
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
		return err
	}

	if err := opfs.MakeDirAll(path.Dir(src), os.FileMode(defaultConfigPerm)); err != nil {
		log.Printf("mkdir fail %v", err)
		return err
	}
	if err := opfs.Rename(srcTmp, src); err != nil {
		log.Printf("rename fail %v", err)
		return err
	}

	return nil
}

func rmBlocksMap(filename string) error {
	bsm := getBlocksMap()
	if err := bsm.DeleteBlocks(filename); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func rmMeta(filename string) error {
	gmetas := getGlobalMeta()
	err := gmetas.Delete(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func rmFile(filename string) error {
	if err := opfs.RemoveFile(filename); err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}
	return nil
}

func rmDir(filename string) error {
	if err := opfs.RemoveDir(filename); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}

func removeFile(filename string) error {
	//1.delete blocks map
	if err := rmBlocksMap(filename); err != nil {
		return err
	}
	//2. delete meta 
	if err := rmMeta(filename); err != nil {
		return err
	}
	//3. delete openfs file
	if err := rmFile(filename); err != nil {
		return err
	}

	return nil
}

func removeDir(filename string) error {
	//1. delete meta
	if err := rmMeta(filename); err != nil {
		return err
	}
	//2. delete openfs dir
	if err := rmDir(filename); err != nil {
		return err
	}

	return nil
}

func removeAllPath(filename string) error {
	f, err := opfs.Open(filename)
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
			newPath := path.Clean(path.Join(filename, name))
			err := removeAllPath(newPath)
			if err != nil {
				return err
			}
		}
		err = removeDir(filename)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			} else if errors.Is(err, syscall.ENOTEMPTY) {
				return errNotEmpty
			}
			return err
		}
	} else {
		err := removeFile(filename)
		if err != nil {
			return err
		}
	}
	return nil
}

func renameBlocksMap(src, dst string) error {
	bsm := getBlocksMap()
	if err := bsm.RenameBlocksMap(src, dst); err != nil {
		return err
	}

	return nil
}

func renameMeta(src, dst string) error {
	gmetas := getGlobalMeta()
	return gmetas.Rename(src, dst)
}

func renameFile(src, dst string) error {
	isDir, err := opfsIsDir(src)
	if err != nil {
		return err
	}
	// 1. rename openfs file
	if err := opfs.Rename(src, dst); err != nil {
		return err
	}
	// 2. rename meta
	if err := renameMeta(src, dst); err != nil {
		return err
	}
	// 3. rename blocksMap
	if !isDir {
		if err := renameBlocksMap(src, dst); err != nil {
			return err
		}
	}

	return nil
}
