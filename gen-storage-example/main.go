package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/google/subcommands"

	// Import the genericstorage driver packages we want to be able to open.
	_ "github.com/swaraj1802/GenericStorageSDK/genericstorage/fileblob"
	_ "github.com/swaraj1802/GenericStorageSDK/genericstorage/gcsblob"
	_ "github.com/swaraj1802/GenericStorageSDK/genericstorage/s3blob"
)

var response string

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(ctx context.Context, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	fmt.Print("event.Body==> ")
	fmt.Println(event.Body)
	response = ""
	var params []string
	if strings.Contains(event.Body,"Content-Disposition: form-data; name="){
		split := strings.Split(event.Body,"----------------------------")
		fileName := ""
		fileContent := ""
		bucketName := ""
		for _,val := range split[1:] {
			if strings.Contains(val,"name=\"bucket\""){
				bucketName=val[strings.Index(val,"name=\"bucket\"")+14:]
				bucketName=strings.ReplaceAll(bucketName,"\n","")
				bucketName=strings.ReplaceAll(bucketName,"\r","")
				fmt.Println(bucketName)
			} else if strings.Contains(val,"Content-Type:") {
				re := regexp.MustCompile(`name="(.*?)"`)
				match := re.FindStringSubmatch(val)
				fileName = match[0]
				val = val[strings.Index(val,"Content-Type:"):]
				fileContent = val[strings.Index(val,"\n"):]
				fileContent=strings.Replace(fileContent,"\n","",1)
				fileContent=strings.Replace(fileContent,"\r","",1)
				fileName = strings.ReplaceAll(fileName,"name=","")
				fileName = strings.ReplaceAll(fileName,`"`,"")
			}
		}
		params = make([]string,3)
		params[0]="upload"
		params[1]=bucketName
		params[2]=fileName
		fmt.Print("Here2")
		os.Args = []string{"", "upload",bucketName, fileName}
		fmt.Println(fileName)
		fmt.Println(bucketName)
		file, err := os.Create("/tmp/"+fileName)
		if err != nil {
			fmt.Println(err.Error())
			return events.APIGatewayProxyResponse{
				StatusCode: 500,
				Body:       "Error occured wile creating temp file",
			}, fmt.Errorf("Error")
		}
		defer file.Close()
		_, err = file.WriteString(fileContent)
		if err != nil {
			fmt.Println(err.Error())
			return events.APIGatewayProxyResponse{
				StatusCode: 500,
				Body:       "Error occured wile creating writing string to the file",
			}, fmt.Errorf("Error")
		}
	} else {
		params = strings.Split(event.Body, ",")
		if len(params) == 2 {
			os.Args = []string{"", params[0], params[1]}
		} else if len(params) == 3 {
			os.Args = []string{"", params[0], params[1], params[2]}
		}
	}
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(&downloadCmd{}, "")
	subcommands.Register(&listCmd{}, "")
	subcommands.Register(&uploadCmd{}, "")
	log.SetFlags(0)
	log.SetPrefix("gen-storage: ")
	flag.Parse()
	status := subcommands.Execute(context.Background())

	if status == 1 {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Error occured check the logs",
		}, fmt.Errorf("Error")
	}

	if params[0]=="download" {
		resp := events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       response,
		}
		resp.Headers = make(map[string]string)
		resp.Headers["Content-Type"] = "application/octet-stream"
		resp.Headers["Content-Disposition"] = "attachment; filename=" + params[2]
		resp.Headers["Access-Control-Allow-Origin"]="*"
		resp.Headers["Access-Control-Expose-Headers"]="Content-Disposition"
		return resp, nil
	}

	resp := events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       response,
	}
	resp.Headers["Content-Type"] = "text/plain"
	resp.Headers["Access-Control-Allow-Origin"]="*"
	return resp,nil
}

type downloadCmd struct{}

func (*downloadCmd) Name() string     { return "download" }
func (*downloadCmd) Synopsis() string { return "Output a genericstorage to stdout" }
func (*downloadCmd) Usage() string {
	return `download <bucket URL> <key>

  Read the genericstorage <key> from <bucket URL> and write it to stdout.

  Example:
    gen-storage download gs://mybucket my/gcs/file`
}

func (*downloadCmd) SetFlags(_ *flag.FlagSet) {}

func (*downloadCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() != 2 {
		f.Usage()
		return subcommands.ExitUsageError
	}
	bucketURL := f.Arg(0)
	blobKey := f.Arg(1)
	// Open a *genericstorage.Bucket using the bucketURL.
	bucket, err := genericstorage.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Printf("Failed to open bucket: %v\n", err)
		return subcommands.ExitFailure
	}
	defer bucket.Close()

	// Open a *genericstorage.Reader for the genericstorage at blobKey.
	reader, err := bucket.NewReader(ctx, blobKey, nil)
	if err != nil {
		log.Printf("Failed to read %q: %v\n", blobKey, err)
		return subcommands.ExitFailure
	}
	defer reader.Close()

	buf := new(bytes.Buffer)
	_, err = reader.WriteTo(buf)
	if err != nil {
		log.Printf("Failed to copy data: %v\n", err)
		return subcommands.ExitFailure
	}
	response = buf.String()
	return subcommands.ExitSuccess
}

type listCmd struct {
	prefix    string
	delimiter string
}

func (*listCmd) Name() string     { return "ls" }
func (*listCmd) Synopsis() string { return "List blobs in a bucket" }
func (*listCmd) Usage() string {
	return `ls [-p <prefix>] [d <delimiter>] <bucket URL>

  List the blobs in <bucket URL>.

  Example:
    gen-storage ls -p "subdir/" gs://mybucket`
}

func (cmd *listCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.prefix, "p", "", "prefix to match")
	f.StringVar(&cmd.delimiter, "d", "/", "directory delimiter; empty string returns flattened listing")
}

func (cmd *listCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() != 1 {
		f.Usage()
		return subcommands.ExitUsageError
	}
	bucketURL := f.Arg(0)

	// Open a *genericstorage.Bucket using the bucketURL.
	bucket, err := genericstorage.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Printf("Failed to open bucket: %v\n", err)
		return subcommands.ExitFailure
	}
	defer bucket.Close()

	opts := genericstorage.ListOptions{
		Prefix:    cmd.prefix,
		Delimiter: cmd.delimiter,
	}
	iter := bucket.List(&opts)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Failed to list: %v", err)
			return subcommands.ExitFailure
		}
		response += obj.Key + "\n"
	}
	return subcommands.ExitSuccess
}

type uploadCmd struct{}

func (*uploadCmd) Name() string     { return "upload" }
func (*uploadCmd) Synopsis() string { return "Upload a genericstorage from stdin" }
func (*uploadCmd) Usage() string {
	return `Multipart form submission`
}

func (*uploadCmd) SetFlags(_ *flag.FlagSet) {}

func (*uploadCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) (status subcommands.ExitStatus) {
	fmt.Print("Uploading file")
	if f.NArg() != 2 {
		f.Usage()
		return subcommands.ExitUsageError
	}
	bucketURL := f.Arg(0)
	blobKey := f.Arg(1)

	// Open a *genericstorage.Bucket using the bucketURL.
	bucket, err := genericstorage.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Printf("Failed to open bucket: %v\n", err)
		return subcommands.ExitFailure
	}
	defer bucket.Close()

	// Open a *genericstorage.Writer for the genericstorage at blobKey.
	writer, err := bucket.NewWriter(ctx, blobKey, nil)
	if err != nil {
		log.Printf("Failed to write %q: %v\n", blobKey, err)
		return subcommands.ExitFailure
	}
	defer func() {
		if err := writer.Close(); err != nil && status == subcommands.ExitSuccess {
			log.Printf("closing the writer: %v", err)
			status = subcommands.ExitFailure
		}
	}()
	file, err := os.Open("/tmp/"+blobKey)
	if err != nil {
		fmt.Printf("unable to open the file: %v", err)
		status = subcommands.ExitFailure
	}
	defer file.Close()
	// Copy the data.
	_, err = io.Copy(writer, file)
	if err != nil {
		fmt.Printf("Failed to copy data: %v\n", err)
		return subcommands.ExitFailure
	}
	response = "Uploaded successfully to " + bucketURL
	return subcommands.ExitSuccess
}
