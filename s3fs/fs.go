package s3fs

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type S3Fs struct {
	s3Api  s3iface.S3API
	bucket string
}

func New(bucket string, s3 s3iface.S3API) *S3Fs {
	return &S3Fs{
		s3Api:  s3,
		bucket: bucket,
	}
}

func (s *S3Fs) Name() string {
	return "s3fs"
}

func (s *S3Fs) Create(name string) (afero.File, error) {
	if strings.HasSuffix(name, "/") {
		return &S3File{
			s3Api:  s.s3Api,
			bucket: s.bucket,
			key:    name,
		}, nil
	}

	getObjectOutput, err := s.s3Api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimLeft(name, "/")),
	})
	if err != nil {
		return nil, err
	}

	return &S3File{
		s3Api:          s.s3Api,
		bucket:         s.bucket,
		key:            name,
		s3ObjectOutput: getObjectOutput,
	}, nil
}

func (s *S3Fs) Open(name string) (afero.File, error) {
	return s.Create(name)
}

func (s *S3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return s.Create(name)
}

func (s *S3Fs) Mkdir(name string, perm os.FileMode) error {
	return s.MkdirAll(name, perm)
}

func (s *S3Fs) MkdirAll(name string, perm os.FileMode) error {
	_, err := s.s3Api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	return err
}

func (s *S3Fs) Remove(name string) error {
	return s.RemoveAll(name)
}

func (s *S3Fs) RemoveAll(name string) error {
	_, err := s.s3Api.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	return err
}

func (s *S3Fs) Rename(oldname, newname string) error {
	source := fmt.Sprintf("%s/%s", s.bucket, oldname)
	_, err := s.s3Api.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(url.PathEscape(source)),
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

func (s *S3Fs) Chmod(name string, mode os.FileMode) error {
	return nil
}

func (s *S3Fs) Chown(name string, mode os.FileMode) error {
	return nil
}

func (s *S3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}
