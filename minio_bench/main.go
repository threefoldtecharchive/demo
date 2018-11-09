package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/urfave/cli"

	"github.com/minio/minio-go"
)

const (
	MiB int64 = 1024 * 1024
	GiB int64 = MiB * 1024
)

type configuration struct {
	N         int      //number of concurrent upload
	FileSize  int64    // size of the file to generate/upload
	Endpoints []string //address of the minios
	Login     string
	Password  string
	SSL       bool
}

type result struct {
	endpoint string
	size     int64
	speed    float64
	duration time.Duration
}

func (r result) String() string {
	return fmt.Sprintf("%s: Total uploaded %.2fGiB in %s (%.2fMib/s)", r.endpoint, float64(r.size/GiB), r.duration, r.speed)
}

var (
	config configuration
)

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:        "number, n",
			Value:       1,
			Usage:       "number of concurrent upload",
			Destination: &config.N,
		},
		cli.Int64Flag{
			Name:        "size, s",
			Value:       1,
			Usage:       "size of the file to upload in GiB",
			Destination: &config.FileSize,
		},
		cli.StringSliceFlag{
			Name:  "endpoints",
			Usage: "endpoint of the minio to targets",
		},
		cli.StringFlag{
			Name:        "login, l",
			Value:       "admin",
			Usage:       "minio login",
			Destination: &config.Login,
		},
		cli.StringFlag{
			Name:        "password, p",
			Value:       "adminadmin",
			Usage:       "minio password",
			Destination: &config.Password,
		},
		cli.BoolFlag{
			Name:        "ssl",
			Usage:       "use SSL for minio connection",
			Destination: &config.SSL,
		},
	}

	app.Action = func(ctx *cli.Context) error {
		log.Println("generate data files")
		objectName := fmt.Sprintf("%d.dat", config.FileSize)
		if err := generateFile(objectName, config.FileSize*GiB); err != nil {
			log.Fatalln(err)
		}

		config.Endpoints = ctx.StringSlice("endpoints")

		c := make(chan result)
		wg := sync.WaitGroup{}

		for _, endpoint := range config.Endpoints {
			wg.Add(1)
			go func(endpoint, login, password string, n int, size int64, ssl bool) {
				defer wg.Done()
				result, err := upload(endpoint, login, password, n, size, ssl)
				if err != nil {
					log.Printf("error uploading to %s", endpoint)
				}
				c <- result
			}(endpoint, config.Login, config.Password, config.N, config.FileSize, config.SSL)
		}

		go func() {
			wg.Wait()
			close(c)
		}()

		for result := range c {
			fmt.Println(result)
		}

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func upload(endpoint, login, password string, n int, size int64, ssl bool) (result, error) {
	output := result{
		endpoint: endpoint,
	}
	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, login, password, ssl)
	if err != nil {
		return output, err
	}

	// Make a new bucket
	bucketName := "test"
	location := ""
	log.Printf("%s: create bucket %s\n", endpoint, bucketName)
	createBucket(minioClient, bucketName, location)
	log.Printf("%s: Bucket successfully created %s\n", endpoint, bucketName)

	// start the upload
	var (
		start     = time.Now()
		totalSize int64
		filePath  = fmt.Sprintf("%d.dat", config.FileSize)
	)

	c := make(chan int64)
	wg := sync.WaitGroup{}
	for i := 0; i < config.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Printf("%s: Upload %s", endpoint, filePath)
			n, err := minioClient.FPutObject(bucketName, fmt.Sprintf("%d_%s", i, filePath), filePath, minio.PutObjectOptions{})
			if err != nil {
				log.Fatalln(err)
			}
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

	output.duration = elapsed
	output.size = totalSize
	output.speed = speed(elapsed, totalSize) / float64(MiB)

	return output, nil
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
