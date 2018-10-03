package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/minio/minio-go"
)

const (
	MiB int64 = 1024 * 1024
	GiB int64 = MiB * 1024
)

type configuration struct {
	N        int    //number of concurent upload
	FileSize int64  // size of the file to generate/upload
	Endpoint string //address of the minio
	Login    string
	Password string
	SSL      bool
}

var (
	config configuration
)

func init() {
	flag.IntVar(&config.N, "n", 1, "number of concurrent upload")
	flag.Int64Var(&config.FileSize, "s", 1, "size of the file to upload in GiB")
	flag.StringVar(&config.Endpoint, "endpoint", "127.0.0.1:9000", "endpoint of the minio to target")
	flag.StringVar(&config.Login, "l", "admin", "login of the target minio")
	flag.StringVar(&config.Password, "p", "adminadmin", "password of the target minio")
	flag.BoolVar(&config.SSL, "ssl", false, "use SSL for minio connection")
}

func main() {
	flag.Parse()

	// Initialize minio client object.
	minioClient, err := minio.New(config.Endpoint, config.Login, config.Password, config.SSL)
	if err != nil {
		log.Fatalln(err)
	}

	// Make a new bucket called.
	bucketName := "test"
	location := ""
	createBucket(minioClient, bucketName, location)
	log.Printf("Bucket successfully created %s\n", bucketName)

	// generate data file
	objectName := fmt.Sprintf("%d.dat", config.FileSize)
	filePath := objectName
	if err := generateFile(objectName, config.FileSize*GiB); err != nil {
		log.Fatalln(err)
	}

	// start the upload
	var (
		start     = time.Now()
		totalSize int64
	)

	c := make(chan int64)
	wg := sync.WaitGroup{}
	for i := 0; i < config.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Printf("Upload %s with FPutObject", filePath)
			start := time.Now()
			n, err := minioClient.FPutObject(bucketName, fmt.Sprintf("%d_%s", i, objectName), filePath, minio.PutObjectOptions{})
			if err != nil {
				log.Fatalln(err)
			}
			t := time.Now()
			elapsed := t.Sub(start)
			s := speed(elapsed, n) / float64(MiB)
			log.Printf("Successfully uploaded %s of size %.2fGiB in %s (%.2fMib/s)\n", objectName, float64(n/GiB), elapsed, s)
			c <- n
		}(i)
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	for size := range c {
		totalSize += size
	}
	end := time.Now()
	elapsed := end.Sub(start)
	log.Printf("Total uploaded %.2fGiB in %s (%.2fMib/s)\n", float64(totalSize/GiB), elapsed, speed(elapsed, totalSize)/float64(MiB))

}

func createBucket(client *minio.Client, bucketName, location string) error {
	exists, err := client.BucketExists(bucketName)
	if exists && err == nil {
		return nil
	}
	if err != nil {
		return err
	}
	return client.MakeBucket(bucketName, location)
}

func speed(duration time.Duration, size int64) float64 {
	return float64(size) / duration.Seconds()
}

func generateFile(name string, size int64) error {
	info, err := os.Stat(name)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if info.Size() == size {
		log.Printf("file already exists %s\n", name)
		return nil
	}

	log.Printf("Start creating file %s\n", name)
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.CopyN(f, rand.Reader, size); err != nil {
		return err
	}
	return nil
}
