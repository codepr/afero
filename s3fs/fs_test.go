package s3fs

import (
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	errBucketNotFound = errors.New("bucket not found")
	errKeyNotFound    = errors.New("Key not found")
)

type fakeS3Api struct {
	content map[string]map[string]io.ReadCloser
}

func newFakeS3Api() *fakeS3Api {
	return &fakeS3Api{
		content: make(map[string]map[string]io.ReadCloser),
	}
}

func (f *fakeS3Api) GetObject(getObjectInput *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	bucket, ok := f.content[*getObjectInput.Bucket]
	if !ok {
		return nil, errBucketNotFound
	}
	object, ok := bucket[*getObjectInput.Key]
	if !ok {
		return nil, errKeyNotFound
	}
	return &s3.GetObjectOutput{Body: object}, nil
}

func (f *fakeS3Api) PutObject(putObjectInput *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	if bucket, ok := f.content[*putObjectInput.Bucket]; !ok {
		bucket = make(map[string]io.ReadCloser)
		f.content[*putObjectInput.Bucket] = bucket
	} else {
		bucket[*putObjectInput.Key] = aws.ReadSeekCloser(putObjectInput.Body)
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3Api) DeleteObject(deleteObjectInput *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	bucket, ok := f.content[*deleteObjectInput.Bucket]
	if !ok {
		return nil, errBucketNotFound
	}
	_, ok = bucket[*deleteObjectInput.Key]
	if !ok {
		return nil, errKeyNotFound
	}
	delete(bucket, *deleteObjectInput.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3Api) DeleteObjects(deleteObjectsInput *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	bucket, ok := f.content[*deleteObjectsInput.Bucket]
	if !ok {
		return nil, errBucketNotFound
	}
	for _, id := range deleteObjectsInput.Delete.Objects {
		delete(bucket, *id.Key)
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (f *fakeS3Api) CopyObject(copyObjectInput *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	bucket, ok := f.content[*copyObjectInput.Bucket]
	if !ok {
		return nil, errBucketNotFound
	}
	object, ok := bucket[*copyObjectInput.CopySource]
	if !ok {
		return nil, errKeyNotFound
	}
	bucket[*copyObjectInput.Key] = object
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeS3Api) WaitUntilObjectExists(headObjectInput *s3.HeadObjectInput) error {
	return nil
}

func (f *fakeS3Api) ListObjectsV2(v2input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	bucket, ok := f.content[*v2input.Bucket]
	if !ok {
		return nil, errBucketNotFound
	}
	fmt.Printf("%#v", bucket)
	var objects []*s3.Object
	for key, _ := range bucket {
		objects = append(objects, &s3.Object{Key: aws.String(key)})
	}
	return &s3.ListObjectsV2Output{Contents: objects, IsTruncated: aws.Bool(true)}, nil
}
