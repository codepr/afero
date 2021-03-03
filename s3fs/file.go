package s3fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3FileInfo struct {
	key            string
	s3ObjectOutput *s3.GetObjectOutput
	s3Object       *s3.Object
}

func (i *S3FileInfo) Name() string {
	return filepath.Base(i.key)
}

func (i *S3FileInfo) Size() int64 {
	if i.s3ObjectOutput != nil {
		return aws.Int64Value(i.s3ObjectOutput.ContentLength)
	}
	if i.s3Object != nil {
		return aws.Int64Value(i.s3Object.Size)
	}
	return 0
}

func (i *S3FileInfo) Mode() os.FileMode {
	if i.IsDir() {
		return os.ModeDir
	}
	return os.ModeIrregular
}

func (i *S3FileInfo) ModTime() time.Time {
	if i.s3ObjectOutput != nil {
		return aws.TimeValue(i.s3ObjectOutput.LastModified)
	}
	if i.s3Object != nil {
		return aws.TimeValue(i.s3Object.LastModified)
	}
	return time.Time{}
}

func (i *S3FileInfo) IsDir() bool {
	return i.s3ObjectOutput == nil && i.s3Object == nil
}

func (i *S3FileInfo) Sys() interface{} {
	if i.s3ObjectOutput != nil {
		return i.s3ObjectOutput
	}
	return i.s3Object
}

type s3lister interface {
	ListObjectsV2(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

// S3File implements afero.File
type S3File struct {
	m              sync.RWMutex
	s3Api          s3lister
	bucket         string
	key            string
	s3ObjectOutput *s3.GetObjectOutput
}

func (f *S3File) Close() error {
	if f.s3ObjectOutput != nil {
		return f.s3ObjectOutput.Body.Close()
	}
	return nil
}

func (f *S3File) Read(p []byte) (n int, err error) {
	if f.s3ObjectOutput == nil {
		return 0, fmt.Errorf("Cannot read")
	}
	f.m.RLock()
	defer f.m.RUnlock()
	return f.s3ObjectOutput.Body.Read(p)
}

func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("Not supported")
}

func (f *S3File) Readdir(count int) ([]os.FileInfo, error) {
	if f.s3ObjectOutput == nil {
		return nil, fmt.Errorf("Cannot read directory")
	}
	var (
		continuationToken *string
		fileInfos         []os.FileInfo
	)

	for {
		listObjectsV2Output, err := f.s3Api.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(f.bucket),
			Delimiter:         aws.String("/"),
			Prefix:            aws.String(strings.TrimLeft(f.key, "/")),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int64(int64(count)),
		})

		if err != nil {
			return nil, err
		}

		for _, object := range listObjectsV2Output.Contents {
			fileInfos = append(fileInfos, &S3FileInfo{
				key:      aws.StringValue(object.Key),
				s3Object: object,
			})
		}

		continuationToken = listObjectsV2Output.NextContinuationToken

		if !aws.BoolValue(listObjectsV2Output.IsTruncated) || listObjectsV2Output.NextContinuationToken == nil {
			break
		}
	}

	return fileInfos, nil
}

func (f *S3File) Readdirnames(n int) (names []string, err error) {
	fi, err := f.Readdir(n)
	names = make([]string, len(fi))
	for i, f := range fi {
		_, names[i] = filepath.Split(f.Name())
	}
	return names, err
}

func (f *S3File) Name() string {
	return filepath.Base(f.key)
}

func (f *S3File) Stat() (os.FileInfo, error) {
	return &S3FileInfo{
		key:            f.key,
		s3ObjectOutput: f.s3ObjectOutput,
	}, nil
}

func (f *S3File) Sync() error {
	return nil
}

// TODO
func (f *S3File) Write(p []byte) (n int, err error) {
	return 0, nil
}

// TODO
func (f *S3File) WriteAt(b []byte, off int64) (n int, err error) {
	return 0, nil
}

// TODO
func (f *S3File) WriteString(s string) (ret int, err error) {
	return 0, nil
}

// TODO
func (f *S3File) Truncate(size int64) error {
	return nil
}
