package cmd

import (
	"log"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getXAttrsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetXAttrsRequestProto)
	return parseRequest(b, req)
}

func getXAttrs(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetXAttrsRequestProto)
	log.Printf("src %v\nxattr %v\n", req.GetSrc(), req.GetXAttrs())
	return opfsGetXAttrs(req)
}

var (
	nameSpaceMap = map[string]hdfs.XAttrProto_XAttrNamespaceProto{
		hdfs.XAttrProto_USER.String(): hdfs.XAttrProto_USER,
		hdfs.XAttrProto_TRUSTED.String(): hdfs.XAttrProto_TRUSTED,
		hdfs.XAttrProto_SECURITY.String(): hdfs.XAttrProto_SECURITY,
		hdfs.XAttrProto_SYSTEM.String(): hdfs.XAttrProto_SYSTEM,
		hdfs.XAttrProto_RAW.String(): hdfs.XAttrProto_RAW,
	}
)

func xattrInRequest(needs []*hdfs.XAttrProto, ox opfs.OpfsXAttrEntry) bool {
	//no special need name, all entries will returns
	if needs == nil || len(needs) == 0 {
		return true
	}

	for _, a := range needs {
		if a.GetNamespace().String() == ox.Scope &&
		   a.GetName() == ox.Name {
			   return true
		   }
	}

	return false
}

func opfsGetXAttrs(r *hdfs.GetXAttrsRequestProto) (*hdfs.GetXAttrsResponseProto, error) {
	src := r.GetSrc()
	xattrs := r.GetXAttrs()

	log.Printf("xattrs %v\n", xattrs)
	entries, err := opfs.GetXAttr(src)
	if err != nil {
		return nil, err
	}

	rxattrs := make([]*hdfs.XAttrProto, 0, len(entries))
	for _, e := range entries {
		if !xattrInRequest(xattrs, e) {
			continue
		}
		xe := &hdfs.XAttrProto {
			Namespace: nameSpaceMap[e.Scope].Enum(),
			Name: proto.String(e.Name),
			Value: e.Value,
		}
		rxattrs = append(rxattrs,xe)
	}

	return &hdfs.GetXAttrsResponseProto {
		XAttrs: rxattrs,
	}, nil
}
