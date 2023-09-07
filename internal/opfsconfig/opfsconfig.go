package opfsconfig

import (
	"path"
	"log"
	"os"
	"io"
	"bytes"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/openfs/openfs-hdfs/internal/opfs"
	"github.com/openfs/openfs-hdfs/internal/opfsconstant"
)

func LoadFromConfig(src string, object interface{}) error {
	b, err := opfs.ReadAll(src)
	if err != nil {
		return err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(b, object); err != nil {
		return err
        }

	return nil
}

func getRandomFileName() string {
        u, err := uuid.NewRandom()
        if err != nil {
		log.Printf("get uuid fail for tmp file name %v", err)
        }

        return u.String()
}

func cleanTmp(src string) {
	opfs.RemoveFile(src)
}

func SaveToConfig(src string, object interface{}) error {
	srcTmp := path.Join(opfsconstant.HdfsSysDir, opfsconstant.HdfsTmp, getRandomFileName())
	defaultConfigPerm := opfsconstant.DefaultConfigPerm
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
