package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var s3Svc *s3.S3

var downloadManager *s3manager.Downloader

func initializeS3DownloadManager() {
	if s3Svc == nil {
		initializes3Service()
	}
	downloadManager = s3manager.NewDownloaderWithClient(s3Svc)
}

func initializes3Service() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	}))
	s3Svc = s3.New(sess)
}

func GetDownloadStream(bucket string, keys []string, writer io.Writer) (err error) {
	if s3Svc == nil {
		initializes3Service()
	}
	chkFatal(err)
	for _, key := range keys {
		fmt.Println("adding " + key)
		result, err := s3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		chkFatal(err)
		unzippedFile, err := gzip.NewReader(result.Body)
		chkFatal(err)
		io.Copy(writer, unzippedFile)
		result.Body.Close()
	}
	return
}

func GetSingleFileInFolder(bucket string, keyPrefix string, suffix string) (file string) {
	if s3Svc == nil {
		initializes3Service()
	}
	keys := GetAllFilesWithSuffix(bucket, keyPrefix, suffix)
	if len(keys) == 1 {
		result, err := s3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(keys[0]),
		})
		chkFatal(err)
		file, err := ioutil.ReadAll(result.Body)
		chkFatal(err)
		return string(file)
	} else {
		fmt.Println("didn't get one key")
		return
	}
}

func SplitIntoBucketAndKey(fullPath string) (err error, bucket string, key string) {
	pathSlice := strings.Split(fullPath, ",")
	pathLength := len(pathSlice)
	if pathLength < 4 {
		return errors.New("invalid aws path"), "", ""
	}
	return nil, pathSlice[2], strings.Join(pathSlice[3:pathLength], "/")
}

func GetAllFilesWithSuffix(bucket string, keyPrefix string, suffix string) (keys []string) {
	if s3Svc == nil {
		initializes3Service()
	}
	fmt.Println("finding all keys for bucket: " + bucket + " , keyPrefix: " + keyPrefix + " ,suffix: " + suffix)
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(keyPrefix),
	}
	// result, err := s3Svc.ListObjectsV2(params)
	// chkFatal(err)
	// fmt.Println(result)
	// for _, element := range result.Contents {
	// 	if strings.HasSuffix(*element.Key, ".csv.gz") {
	// 		keys = append(keys, *element.Key)
	// 	}
	// }
	err := s3Svc.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, element := range page.Contents {
				if strings.HasSuffix(*element.Key, suffix) && *element.Size > int64(0) {
					keys = append(keys, *element.Key)
				}
			}
			return lastPage
		})
	chkFatal(err)
	return keys
}
