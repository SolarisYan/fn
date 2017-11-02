// package s3 implements an s3 api compatible log store
package s3

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"

	"github.com/fnproject/fn/api/models"
	"github.com/minio/minio-go"
)

// TODO we should encrypt these, user will have to supply a key though (or all
// OSS users logs will be encrypted with same key unless they change it which
// just seems mean...)

// TODO do we need to use the v2 API? can't find BMC object store docs :/

const (
	contentType = "text/plain"
)

type store struct {
	client *minio.Client
	bucket string
}

// s3://access_key_id:secret_access_key@host/location/bucket_name?ssl=true
func New(uri string) (models.LogStore, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	endpoint := u.Host
	accessKeyID := u.User.Username()
	secretAccessKey, _ := u.User.Password()
	useSSL := u.Query().Get("ssl") == "true"

	strs := strings.SplitN(u.Path, "/", 2)
	if len(strs) < 2 {
		return nil, errors.New("must provide bucket name and region in path of s3 api url. e.g. s3://s3.com/us-east-1/my_bucket")
	}
	location := strs[0]
	bucketName := strs[1]
	if location == "" {
		return nil, errors.New("must provide non-empty location in path of s3 api url. e.g. s3://s3.com/us-east-1/my_bucket")
	} else if bucketName == "" {
		return nil, errors.New("must provide non-empty bucket name in path of s3 api url. e.g. s3://s3.com/us-east-1/my_bucket")
	}

	// TODO not sure we _need_ validation, they could be running against an s3 api
	// that doesn't require auth...

	client, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		return nil, err
	}

	// ensure the bucket exists, creating if it does not
	err = client.MakeBucket(bucketName, location)
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, err := client.BucketExists(bucketName)
		if err != nil {
			return nil, err
		} else if !exists {
			return nil, errors.New("could not create bucket and bucket does not exist, please check permissions")
		}
	}

	return &store{
		client: client,
		bucket: bucketName,
	}, nil
}

func path(appName, callID string) string {
	return "/" + appName + "/" + callID
}

func (s *store) InsertLog(ctx context.Context, appName, callID string, callLog io.Reader) error {
	objectName := path(appName, callID)
	_, err := s.client.PutObjectWithContext(ctx, s.bucket, objectName, callLog, -1, minio.PutObjectOptions{ContentType: contentType})
	return err
}

func (s *store) GetLog(ctx context.Context, appName, callID string) (io.Reader, error) {
	objectName := path(appName, callID)
	return s.client.GetObjectWithContext(ctx, s.bucket, objectName, minio.GetObjectOptions{})
}
