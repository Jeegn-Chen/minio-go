package minio

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

func TestGetAdminLog1(t *testing.T) {
	c, err := NewV2PathStyle(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableSecurity)),
	)
	if err != nil {
		t.Fatal("Error:", err)
	}
	queryValues := make(url.Values)
	queryValues.Set("type", "data")
	customHeader := make(http.Header)
	resp, err := c.GetRGWAdminInfo(
		"admin", "log",
		queryValues,
		customHeader)
	if err != nil {
		t.Fatal("Error:", err, "/admin/log")
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatal("Error:", resp, "/admin/log")
	}
	infoBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal("Error:", err, "/admin/log")
	}
	infoStr := string(infoBuf)
	fmt.Printf("%v\n", infoStr)
}

func TestGetAdminLog2(t *testing.T) {
	c, err := NewV2PathStyle(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableSecurity)),
	)
	if err != nil {
		t.Fatal("Error:", err)
	}
	queryValues := make(url.Values)
	queryValues.Set("type", "data")
	queryValues.Set("id", "83")
	queryValues.Set("max-entries", "100")
	queryValues.Set("extra-info", "true")
	customHeader := make(http.Header)
	resp, err := c.GetRGWAdminInfo(
		"admin", "log",
		queryValues,
		customHeader)
	if err != nil {
		t.Fatal("Error:", err, "/admin/log")
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatal("Error:", resp, "/admin/log")
	}
	infoBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal("Error:", err, "/admin/log")
	}
	infoStr := string(infoBuf)
	fmt.Printf("%v\n", infoStr)
}

func TestGetAdminConfig(t *testing.T) {
	c, err := NewV2PathStyle(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableSecurity)),
	)
	if err != nil {
		t.Fatal("Error:", err)
	}
	queryValues := make(url.Values)
	queryValues.Set("type", "zone")
	customHeader := make(http.Header)
	resp, err := c.GetRGWAdminInfo(
		"admin", "config",
		queryValues,
		customHeader)
	if err != nil {
		t.Fatal("Error:", err, "/admin/config")
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatal("Error:", resp, "/admin/config")
	}
	infoBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal("Error:", err, "/admin/config")
	}
	infoStr := string(infoBuf)
	fmt.Printf("%v\n", infoStr)
}

func TestGetAdminLog3(t *testing.T) {
	c, err := NewV2PathStyle(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableSecurity)),
	)
	if err != nil {
		t.Fatal("Error:", err)
	}
	queryValues := make(url.Values)
	queryValues.Set("type", "bucket-index")
	queryValues.Set("bucket-instance", "synctest:a6e1c149-1afa-4694-b773-0675be9be6dd.6945.1:42")
	queryValues.Set("marker", "")
	queryValues.Set("max-entries", "100")
	customHeader := make(http.Header)
	resp, err := c.GetRGWAdminInfo(
		"admin", "log",
		queryValues,
		customHeader)
	if err != nil {
		t.Fatal("Error:", err, "/admin/log")
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatal("Error:", resp, "/admin/log")
	}
	infoBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal("Error:", err, "/admin/log")
	}
	infoStr := string(infoBuf)
	fmt.Printf("%v\n", infoStr)
}

func TestCopyRemote(t *testing.T)  {
	if testing.Short() {
		t.Skip("skipping functional tests for short runs")
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := NewCore(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableSecurity)),
	)
	if err != nil {
		t.Fatal("Error:", err)
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "default")
	if err != nil {
		t.Fatal("Error:", err, bucketName)
	}

	buf := bytes.Repeat([]byte("a"), 32*1024)

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	objInfo, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), "", "", map[string]string{
		"Content-Type": "binary/octet-stream",
	}, nil)
	if err != nil {
		t.Fatal("Error:", err, bucketName, objectName)
	}

	if objInfo.Size != int64(len(buf)) {
		t.Fatalf("Error: number of bytes does not match, want %v, got %v\n", len(buf), objInfo.Size)
	}

	destBucketName := bucketName
	destObjectName := objectName + "-dest"

	cobjInfo, err := c.CopyObjectQ(
		bucketName, objectName,
		destBucketName, destObjectName,
		map[string]string{},
		map[string]string {
			"rgwx-source-endpoint": "http://127.0.0.1:8000",
			"rgwx-client-id": "fake-client-id",
			"rgwx-op-id": "fake-op-id",
			"rgwx-copy-if-newer": "true",
			"rgwx-source-zone": "unknown",
		},
		)
	if err != nil {
		t.Fatal("Error:", err, bucketName, objectName, destBucketName, destObjectName)
	}
	if cobjInfo.ETag != objInfo.ETag {
		t.Fatalf("Error: expected etag to be same as source object %s, but found different etag :%s", objInfo.ETag, cobjInfo.ETag)
	}

	// Attempt to read from destBucketName and object name.
	r, err := c.Client.GetObject(destBucketName, destObjectName, GetObjectOptions{})
	if err != nil {
		t.Fatal("Error:", err, bucketName, objectName)
	}

	st, err := r.Stat()
	if err != nil {
		t.Fatal("Error:", err, bucketName, objectName)
	}

	if st.Size != int64(len(buf)) {
		t.Fatalf("Error: number of bytes in stat does not match, want %v, got %v\n",
			len(buf), st.Size)
	}

	if st.ContentType != "binary/octet-stream" {
		t.Fatalf("Error: Content types don't match, expected: binary/octet-stream, found: %+v\n", st.ContentType)
	}

	if st.ETag != objInfo.ETag {
		t.Fatalf("Error: expected etag to be same as source object %s, but found different etag :%s", objInfo.ETag, st.ETag)
	}

	if err := r.Close(); err != nil {
		t.Fatal("Error:", err)
	}

	if err := r.Close(); err == nil {
		t.Fatal("Error: object is already closed, should return error")
	}

	err = c.RemoveObjectQ(bucketName, objectName, map[string]string{})
	if err != nil {
		t.Fatal("Error: ", err)
	}

	err = c.RemoveObject(destBucketName, destObjectName)
	if err != nil {
		t.Fatal("Error: ", err)
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		t.Fatal("Error:", err)
	}

	// Do not need to remove destBucketName its same as bucketName.
}
