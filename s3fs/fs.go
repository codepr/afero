package s3fs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// s3api is just a convenient private interface to improve testability and
// subsequent incremental changes.
//
// Could have just used s3iface.S3API but didn't feel useful to bring in such
// a big interface while only few methods were actually useful.
type s3api interface {
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	DeleteObject(*s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	DeleteObjects(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
	CopyObject(*s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
	WaitUntilObjectExists(*s3.HeadObjectInput) error
	ListObjectsV2(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

// S3Fs implements afero.Fs
type S3Fs struct {
	s3Api  s3api
	bucket string
}

func New(bucket string, api s3api) *S3Fs {
	return &S3Fs{
		s3Api:  api,
		bucket: bucket,
	}
}

func (s *S3Fs) Name() string {
	return "s3fs"
}

// Create create a new file into an S3 bucket, returning a *S3File, which
// implements afero.File or an error
func (s *S3Fs) Create(name string) (afero.File, error) {
	if strings.HasSuffix(name, "/") {
		// FIXME return err
		return &S3File{
			s3Api:  s.s3Api,
			bucket: s.bucket,
			key:    name,
		}, nil
	}

	_, err := s.s3Api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimLeft(name, "/")),
		Body:   aws.ReadSeekCloser(bytes.NewBuffer([]byte{})),
	})
	if err != nil {
		return nil, err
	}

	return s.Open(name)
}

// Open opens a file, returning it or an error, if any happens
func (s *S3Fs) Open(name string) (afero.File, error) {
	getObjectOutput, err := s.s3Api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimLeft(name, "/")),
	})
	if err != nil {
		return nil, err
	}
	if aws.BoolValue(getObjectOutput.DeleteMarker) {
		return nil, fmt.Errorf("File is marked as deleted")
	}

	return &S3File{
		s3Api:          s.s3Api,
		bucket:         s.bucket,
		key:            name,
		s3ObjectOutput: getObjectOutput,
	}, nil
}

// OpenFile opens a file using the given flags, returning it or an error, if
// any happens. Flags are unused as S3 doesn't really support OS-like
// permissions for its contents.
func (s *S3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return s.Open(name)
}

// Mkdir creates a directory in the filesystem, return an error if any
// happens.
func (s *S3Fs) Mkdir(name string, perm os.FileMode) error {
	return s.MkdirAll(name, perm)
}

// MkdirAll creates a directory path and all parents that does not exist
// yet.
func (s *S3Fs) MkdirAll(name string, perm os.FileMode) error {
	_, err := s.s3Api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	return err
}

// Remove removes a file identified by name, returning an error, if any
// happens.
func (s *S3Fs) Remove(name string) error {
	return s.RemoveAll(name)
}

// RemoveAll removes a directory path and any children it contains. It
// does not fail if the path does not exist (return nil).
func (s *S3Fs) RemoveAll(name string) error {
	listObject, err := s.s3Api.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(strings.TrimLeft(name, "/")),
	})
	if err != nil {
		return err
	}
	if len(listObject.Contents) == 0 {
		return fmt.Errorf("No objects")
	}
	fmt.Printf("#%v", listObject.Contents)
	objectIds := make([]*s3.ObjectIdentifier, len(listObject.Contents))
	for i, object := range listObject.Contents {
		objectIds[i] = &s3.ObjectIdentifier{Key: object.Key}
	}
	_, err = s.s3Api.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &s3.Delete{
			Objects: objectIds,
		},
	})
	return err
}

// Rename renames a file. Under the hood what it does is create a copy of the
// old file int othe s3 bucket with the new key represented by newname and then
// remove the old copied file.
func (s *S3Fs) Rename(oldname, newname string) error {
	source := filepath.Join(s.bucket, oldname)
	_, err := s.s3Api.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(source),
		Key:        aws.String(newname),
	})
	if err != nil {
		return err
	}
	_, err = s.s3Api.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(oldname),
	})
	if err != nil {
		return err
	}
	err = s.s3Api.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(newname),
	})
	return err
}

// Stat returns a FileInfo describing the named file, or an error, if any
// happens.
func (s *S3Fs) Stat(name string) (os.FileInfo, error) {
	getObjectOutput, err := s.s3Api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		return nil, err
	}
	file := &S3File{
		s3Api:          s.s3Api,
		bucket:         s.bucket,
		key:            name,
		s3ObjectOutput: getObjectOutput,
	}
	return file.Stat()
}

// Chmod unsupported
func (s *S3Fs) Chmod(name string, mode os.FileMode) error {
	return nil
}

// Chown unsupported
func (s *S3Fs) Chown(name string, mode os.FileMode) error {
	return nil
}

// Chtimes unsupported
func (s *S3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}
