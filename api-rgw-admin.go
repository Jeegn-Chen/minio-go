package minio

import (
	"context"
	"github.com/minio/minio-go/pkg/credentials"
	"github.com/minio/minio-go/pkg/s3utils"
	"net/http"
	"net/url"
	"strings"
)

func NewV2PathStyle(endpoint string, accessKeyID, secretAccessKey string, secure bool) (*Client, error) {
	creds := credentials.NewStaticV2(accessKeyID, secretAccessKey, "")
	clnt, err := privateNew(endpoint, creds, secure, "", BucketLookupAuto)
	if err != nil {
		return nil, err
	}
	clnt.overrideSignerType = credentials.SignatureV2
	return clnt, nil
}

func NewV2PathStyleCore(endpoint string, accessKeyID, secretAccessKey string, secure bool) (*Core, error) {
	var s3Client Core
	client, err := NewV2PathStyle(endpoint, accessKeyID, secretAccessKey, secure)
	if err != nil {
		return nil, err
	}
	s3Client.Client = client
	return &s3Client, nil
}

func (c Client) GetRGWAdminInfo(
	bucketName string, objectName string,
	queryValues url.Values,
	customHeader http.Header) (res *http.Response, err error) {
	return c.executeMethod(context.Background(), "GET", requestMetadata{
		bucketName: bucketName,
		objectName: objectName,
		queryValues: queryValues,
		customHeader: customHeader,
		bucketLocation: "default",
	})
}

func (c Client) copyObjectDoQ(
	ctx context.Context,
	srcBucket, srcObject, destBucket, destObject string,
	metadata map[string]string,
	qValues map[string]string) (ObjectInfo, error) {

	// Build headers.
	headers := make(http.Header)

	// Set all the metadata headers.
	for k, v := range metadata {
		headers.Set(k, v)
	}

	queryValues := make(url.Values)
	for k, v := range qValues {
		queryValues.Set(k, v)
	}

	// Set the source header
	headers.Set("x-amz-copy-source", s3utils.EncodePath(srcBucket+"/"+srcObject))

	// Send upload-part-copy request
	resp, err := c.executeMethod(ctx, "PUT", requestMetadata{
		bucketName:   destBucket,
		objectName:   destObject,
		customHeader: headers,
		queryValues: queryValues,
		bucketLocation: "default",
	})
	defer closeResponse(resp)
	if err != nil {
		return ObjectInfo{}, err
	}

	// Check if we got an error response.
	if resp.StatusCode != http.StatusOK {
		return ObjectInfo{}, httpRespToErrorResponse(resp, srcBucket, srcObject)
	}

	cpObjRes := copyObjectResult{}
	err = xmlDecoder(resp.Body, &cpObjRes)
	if err != nil {
		return ObjectInfo{}, err
	}

	objInfo := ObjectInfo{
		Key:          destObject,
		ETag:         strings.Trim(cpObjRes.ETag, "\""),
		LastModified: cpObjRes.LastModified,
	}
	return objInfo, nil
}

func (c Client) CopyObjectQ(
	sourceBucket,sourceObject, destBucket, destObject string,
	metadata map[string]string,
	qValues map[string]string) (ObjectInfo, error) {
	return c.copyObjectDoQ(
		context.Background(),
		sourceBucket, sourceObject,
		destBucket, destObject,
		metadata, qValues)
}

func (c Client) RemoveObjectQ(bucketName, objectName string, qValues map[string]string) error {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return err
	}
	queryValues := make(url.Values)
	for k, v := range qValues {
		queryValues.Set(k, v)
	}
	// Execute DELETE on objectName.
	resp, err := c.executeMethod(context.Background(), "DELETE", requestMetadata{
		bucketName:       bucketName,
		objectName:       objectName,
		contentSHA256Hex: emptySHA256Hex,
		queryValues:      queryValues,
		bucketLocation: "default",
	})
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	if resp != nil {
		// if some unexpected error happened and max retry is reached, we want to let client know
		if resp.StatusCode != http.StatusNoContent {
			return httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}

	// DeleteObject always responds with http '204' even for
	// objects which do not exist. So no need to handle them
	// specifically.
	return nil
}
