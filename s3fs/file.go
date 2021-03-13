package s3fs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3FileInfo implements os.FileInfo interface within the S3 context
type S3FileInfo struct {
	key            string
	s3ObjectOutput *s3.GetObjectOutput
	s3Object       *s3.Object
}

// Name returns the name of the file, represented by the basename of the key
// used to store the file into the S3 bucket
func (i *S3FileInfo) Name() string {
	return filepath.Base(i.key)
}

// Size returns the size of the file stored inside the S3 bucket
func (i *S3FileInfo) Size() int64 {
	if i.s3ObjectOutput != nil {
		return aws.Int64Value(i.s3ObjectOutput.ContentLength)
	}
	if i.s3Object != nil {
		return aws.Int64Value(i.s3Object.Size)
	}
	return 0
}

// Mode return the file permissions of the file, given that S3 doesn't really
// support an OS-like permission system for its content, this is limited to
// just separating directories and files
func (i *S3FileInfo) Mode() os.FileMode {
	if i.IsDir() {
		return os.ModeDir
	}
	return os.ModePerm
}

// ModTime returns the LastModified time of the S3 file if present, otherwise
// it fallbacks to the time.Time zero value
func (i *S3FileInfo) ModTime() time.Time {
	if i.s3ObjectOutput != nil {
		return aws.TimeValue(i.s3ObjectOutput.LastModified)
	}
	if i.s3Object != nil {
		return aws.TimeValue(i.s3Object.LastModified)
	}
	return time.Time{}
}

// IsDir returns true if no s3Object and s3ObjectOutput is set, in other words
// if the current S3FileInfo is represented by only the S3 key
func (i *S3FileInfo) IsDir() bool {
	return i.s3ObjectOutput == nil && i.s3Object == nil
}

// Sys return the underlying data source, represented by either an
// *s3.GetObjectOutput or an *s3.GetObject
func (i *S3FileInfo) Sys() interface{} {
	if i.s3ObjectOutput != nil {
		return i.s3ObjectOutput
	}
	return i.s3Object
}

// S3File implements afero.File
type S3File struct {
	m              sync.RWMutex
	s3Api          s3api
	bucket         string
	key            string
	s3ObjectOutput *s3.GetObjectOutput
}

// Close closes the underlying io.ReadCloser inside the *s3.GetObjectOutput,
// if present. Can return an error in case of already closed stream.
func (f *S3File) Close() error {
	if f.s3ObjectOutput != nil {
		return f.s3ObjectOutput.Body.Close()
	}
	return nil
}

// Read read contents from the underlying *s3.GetObjectOutput into a byte
// array, may return an error if no io.Reader is present.
func (f *S3File) Read(p []byte) (n int, err error) {
	if f.s3ObjectOutput == nil {
		return 0, fmt.Errorf("Cannot read")
	}
	f.m.RLock()
	defer f.m.RUnlock()
	return f.s3ObjectOutput.Body.Read(p)
}

// ReadAt unsupported
func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

// Seek unsupported
func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("Not supported")
}

// Readdir returns a slice of S3FileInfo limiting the number of results based
// on count value. Can return error if no underlying *s3.GetObjectOutput is set.
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

// Sync unsupported
func (f *S3File) Sync() error {
	return nil
}

// Write writes a slice of bytes into an S3 bucket, underlying it acts
// differently then an OS stream, basically it overwrites the remote object
// inside the S3 bucket by uploading the bytes over
func (f *S3File) Write(p []byte) (n int, err error) {
	buf := bytes.NewBuffer(p)
	_, err = f.s3Api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
		Body:   aws.ReadSeekCloser(buf),
	})
	if err != nil {
		return
	}
	n = len(p)
	return
}

// WriteAt unsupported
func (f *S3File) WriteAt(b []byte, off int64) (n int, err error) {
	return 0, nil
}

// WriteString convenient way to write a string using the Write function
func (f *S3File) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

// Truncate unsupported
func (f *S3File) Truncate(size int64) error {
	return nil
}
