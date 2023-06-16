package compress

import (
	"bytes"
	"compress/zlib"
	"io"
)

type Zlib struct {
	Level int
}

func (z Zlib) Name() string { return "Zlib" }

func (z Zlib) Compress(src []byte) ([]byte, error) {
	var b bytes.Buffer
	w, err := zlib.NewWriterLevel(&b, z.Level)
	if err != nil {
		return []byte{}, err
	}
	_, err = w.Write(src)
	if err != nil {
		return []byte{}, err
	}
	err = w.Close()
	if err != nil {
		return []byte{}, err
	}

	return b.Bytes(), err
}

func (z Zlib) Decompress(src []byte) ([]byte, error) {
	//b := bytes.NewReader(data)
	r, err := zlib.NewReader(bytes.NewReader(src))
	if err != nil {
		panic(err.Error())
	}
	defer func(r io.ReadCloser) {
		err := r.Close()
		if err != nil {

		}
	}(r)
	dst, err := io.ReadAll(r)
	if err != nil {
		return []byte{}, err
	}
	//log.Println(dst)
	return dst, err
}
