package fileblob

import (
	"encoding/json"
	"fmt"
	"os"
)

const attrsExt = ".attrs"

var errAttrsExt = fmt.Errorf("file extension %q is reserved", attrsExt)

type xattrs struct {
	CacheControl       string            `json:"user.cache_control"`
	ContentDisposition string            `json:"user.content_disposition"`
	ContentEncoding    string            `json:"user.content_encoding"`
	ContentLanguage    string            `json:"user.content_language"`
	ContentType        string            `json:"user.content_type"`
	Metadata           map[string]string `json:"user.metadata"`
	MD5                []byte            `json:"md5"`
}

func setAttrs(path string, xa xattrs) error {
	f, err := os.Create(path + attrsExt)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(xa); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func getAttrs(path string) (xattrs, error) {
	f, err := os.Open(path + attrsExt)
	if err != nil {
		if os.IsNotExist(err) {

			return xattrs{
				ContentType: "application/octet-stream",
			}, nil
		}
		return xattrs{}, err
	}
	xa := new(xattrs)
	if err := json.NewDecoder(f).Decode(xa); err != nil {
		f.Close()
		return xattrs{}, err
	}
	return *xa, f.Close()
}
