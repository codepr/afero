package s3fs

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestRead(t *testing.T) {
	s3file := &S3File{
		s3Api:  &fakeS3Api{},
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
	tests := []struct {
		bucket string
		keys   []string
		want   []*S3FileInfo
		count  int
	}{
		{
			bucket: "test-bucket",
			keys:   []string{"/test/path"},
			want:   []*S3FileInfo{{key: "/test/path"}},
			count:  1,
		},
		{
			bucket: "test-bucket",
			keys:   []string{"/test/path/sub", "/test/alt", "/test/subtest/path"},
			want: []*S3FileInfo{
				{key: "/test/path"},
				{key: "/test/path/sub"},
				{key: "/test/alt"},
				{key: "/test/subtest/path"},
			},
			count: 4,
		},
		{
			bucket: "test-bucket",
			keys:   []string{"/test/alt"},
			want: []*S3FileInfo{
				{key: "/test/path"},
				{key: "/test/path/sub"},
				{key: "/test/alt"},
				{key: "/test/subtest/path"},
			},
			count: 4,
		},
	}
	s3api := newFakeS3Api()
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			for _, key := range tt.keys {
				s3api.PutObject(&s3.PutObjectInput{Bucket: aws.String(tt.bucket), Key: aws.String(key)})
			}

			s3file := &S3File{
				s3Api:  s3api,
				bucket: tt.bucket,
				key:    filepath.Dir(tt.keys[0]),
				s3ObjectOutput: &s3.GetObjectOutput{
					Body:          ioutil.NopCloser(bytes.NewReader([]byte("test bin"))),
					ContentLength: aws.Int64(8),
				},
			}

			infos, err := s3file.Readdir(tt.count)
			if err != nil {
				t.Errorf("Readdir failed: %s", err)
			}
			if len(infos) != len(tt.want) {
				t.Errorf("Readdir failed. Expected %#v got %#v", tt.want, infos)
			}

		})
	}
}

func sliceEquality(s1, s2 []string) bool {
	items := make(map[string]bool)
	for _, item := range s1 {
		items[item] = true
	}
	for _, item := range s2 {
		if !items[item] {
			return false
		}
	}
	return true
}

func TestReaddirnames(t *testing.T) {
	bucket := "test-bucket"
	key1 := "/test/path"
	key2 := "/test/path/sub"
	s3api := newFakeS3Api()
	s3api.PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key1)})
	s3api.PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key2)})
	s3file := &S3File{
		s3Api:  s3api,
		bucket: bucket,
		key:    key1,
		s3ObjectOutput: &s3.GetObjectOutput{
			Body:          ioutil.NopCloser(bytes.NewReader([]byte("test bin"))),
			ContentLength: aws.Int64(8),
		},
	}

	want := []string{"path", "sub"}
	infos, err := s3file.Readdirnames(2)
	if err != nil {
		t.Errorf("Readdir failed: %s", err)
	}
	if len(infos) != len(want) || !sliceEquality(want, infos) {
		t.Errorf("Readdir failed. Expected %v got %v", want, infos)
	}
}

func TestWrite(t *testing.T) {
	bucket := "test-bucket"
	key := "/test/path"
	payload := bytes.NewReader([]byte("Test-bin"))
	s3api := newFakeS3Api()
	s3api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(payload),
	})
	s3file := &S3File{
		s3Api:  s3api,
		bucket: bucket,
		key:    key,
		s3ObjectOutput: &s3.GetObjectOutput{
			Body:          ioutil.NopCloser(payload),
			ContentLength: aws.Int64(8),
		},
	}
	changed := []byte("Test-bin-changed")
	_, err := s3file.Write(changed)
	if err != nil {
		t.Errorf("Write failed: %s", err)
	}
	getObject, err := s3api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	body, err := ioutil.ReadAll(getObject.Body)
	if err != nil {
		t.Errorf("Write failed: %s", err)
	}
	if string(body) != string(changed) {
		t.Errorf("Write failed. Expected %s got %s", changed, body)
	}
}
