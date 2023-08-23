package cmd

import (
	"log"
	"fmt"
	"errors"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setXAttrDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetXAttrRequestProto)
	return parseRequest(b, req)
}

func setXAttr(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetXAttrRequestProto)
	log.Printf("src %v\nxattr %v\nflag %v\n", req.GetSrc(), req.GetXAttr(), req.GetFlag())
	return opfsSetXAttr(req)
}

var (
	xattrFlagCreate = uint32(1 << (hdfs.XAttrSetFlagProto_XATTR_CREATE.Number() -1))
	xattrFlagReplace = uint32(1 << (hdfs.XAttrSetFlagProto_XATTR_REPLACE.Number() - 1))
	xattrFlagAll = (xattrFlagCreate | xattrFlagReplace)
)

var (
	errXattrCreate error = errors.New("xattr exist")
	errXattrReplace error = errors.New("xattr not exist")
)


func processFlag(src string, xattr *hdfs.XAttrProto, flag uint32) error {
	log.Printf("create %v, replace %v, all %v", xattrFlagCreate, xattrFlagReplace, xattrFlagAll)

	if flag == xattrFlagAll {
		return nil
	}

	entries, err := opfs.GetXAttr(src)
	if err != nil {
		return err
	}

	found := false
	for _, e := range entries {
		if xattr.GetNamespace().String() == e.Scope &&
		   xattr.GetName() == e.Name {
			   found = true
			   break
		   }
	}
	switch flag {
	case xattrFlagCreate:
		if found {
			return errXattrCreate
		}
	case xattrFlagReplace:
		if !found {
			return errXattrReplace
		}
	default:
		panic(fmt.Errorf("not support flag %v", flag))
	}

	return nil
}

func opfsSetXAttr(r *hdfs.SetXAttrRequestProto) (*hdfs.SetXAttrResponseProto, error) {
	src := r.GetSrc()
	x := r.GetXAttr()
	flag := r.GetFlag()

	if err := processFlag(src, x, flag); err != nil {
		return nil, err
	}

	ox := opfs.OpfsXAttrEntry {
		Scope: x.GetNamespace().String(),
		Name: x.GetName(),
		Value: x.GetValue(),
	}

	if err := opfs.SetXAttr(src, ox); err != nil {
		return nil, err
	}
	return new(hdfs.SetXAttrResponseProto), nil
}
