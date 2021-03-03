package s3fs

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type fakes3lister struct {
	bucket string
	output []*s3.Object
}

func (f *fakes3lister) ListObjectsV2(v2input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	if aws.StringValue(v2input.Bucket) == f.bucket {
		return &s3.ListObjectsV2Output{Contents: f.output}, nil
	}
	return nil, nil
}

func TestRead(t *testing.T) {
	s3file := &S3File{
		s3Api:  &fakes3lister{},
		bucket: "test-bucket",
		key:    "/test/path",
		s3ObjectOutput: &s3.GetObjectOutput{
			Body:          ioutil.NopCloser(bytes.NewReader([]byte("test bin"))),
			ContentLength: aws.Int64(8),
		},
	}
	want := []byte("test bin")
	got := make([]byte, 8)
	n, err := s3file.Read(got)
	if err != nil {
		t.Errorf("Read failed: %s", err)
	}
	if n != 8 {
		t.Errorf("Read failed. Expected %d bytes read, got %d", 8, n)
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("Read failed. Expected %v got %v", want, got)
	}
}

func TestReaddir(t *testing.T) {
	bucket := "test-bucket"
	key := "/test/path"
	s3api := &fakes3lister{
		bucket: bucket,
		output: []*s3.Object{
			&s3.Object{
				Key:          aws.String(key),
				Size:         aws.Int64(8),
				LastModified: aws.Time(time.Time{}),
			},
			&s3.Object{
				Key:          aws.String(key),
				Size:         aws.Int64(10),
				LastModified: aws.Time(time.Time{}),
			},
		},
	}
	s3file := &S3File{
		s3Api:  s3api,
		bucket: bucket,
		key:    key,
		s3ObjectOutput: &s3.GetObjectOutput{
			Body:          ioutil.NopCloser(bytes.NewReader([]byte("test bin"))),
			ContentLength: aws.Int64(8),
		},
	}

	want := []*S3FileInfo{
		&S3FileInfo{
			key:            key,
			s3ObjectOutput: &s3.GetObjectOutput{},
		},
		&S3FileInfo{
			key:            key,
			s3ObjectOutput: &s3.GetObjectOutput{},
		},
	}
	infos, err := s3file.Readdir(2)
	if err != nil {
		t.Errorf("Readdir failed: %s", err)
	}
	if len(infos) != len(want) {
		t.Errorf("Readdir failed. Expected %v got %v", want, infos)
	}
}

func TestReaddirnames(t *testing.T) {
	bucket := "test-bucket"
	key := "/test/path"
	s3api := &fakes3lister{
		bucket: bucket,
		output: []*s3.Object{
			&s3.Object{
				Key:          aws.String(key),
				Size:         aws.Int64(8),
				LastModified: aws.Time(time.Time{}),
			},
			&s3.Object{
				Key:          aws.String(key),
				Size:         aws.Int64(10),
				LastModified: aws.Time(time.Time{}),
			},
		},
	}
	s3file := &S3File{
		s3Api:  s3api,
		bucket: bucket,
		key:    key,
		s3ObjectOutput: &s3.GetObjectOutput{
			Body:          ioutil.NopCloser(bytes.NewReader([]byte("test bin"))),
			ContentLength: aws.Int64(8),
		},
	}

	want := []string{"path", "path"}
	infos, err := s3file.Readdirnames(2)
	if err != nil {
		t.Errorf("Readdir failed: %s", err)
	}
	if len(infos) != len(want) || !reflect.DeepEqual(want, infos) {
		t.Errorf("Readdir failed. Expected %v got %v", want, infos)
	}
}
