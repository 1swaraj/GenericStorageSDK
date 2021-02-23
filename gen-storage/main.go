package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage"
	"io"
	"log"
	"os"

	"github.com/google/subcommands"

	// Import the genericstorage driver packages we want to be able to open.
	_ "github.com/swaraj1802/GenericStorageSDK/genericstorage/fileblob"
	_ "github.com/swaraj1802/GenericStorageSDK/genericstorage/gcsblob"
	_ "github.com/swaraj1802/GenericStorageSDK/genericstorage/s3blob"
)

func main() {
	os.Exit(run())
}

func run() int {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(&downloadCmd{}, "")
	subcommands.Register(&listCmd{}, "")
	subcommands.Register(&uploadCmd{}, "")
	log.SetFlags(0)
	log.SetPrefix("gen-storage: ")
	flag.Parse()
	return int(subcommands.Execute(context.Background()))
}

type downloadCmd struct{}

func (*downloadCmd) Name() string     { return "download" }
func (*downloadCmd) Synopsis() string { return "Output a genericstorage to stdout" }
func (*downloadCmd) Usage() string {
	return `download <bucket URL> <key>

  Read the genericstorage <key> from <bucket URL> and write it to stdout.

  Example:
    gen-storage download gs://mybucket my/gcs/file > foo.txt`
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

	// Copy the data.
	_, err = io.Copy(os.Stdout, reader)
	if err != nil {
		log.Printf("Failed to copy data: %v\n", err)
		return subcommands.ExitFailure
	}
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
		fmt.Println(obj.Key)
	}
	return subcommands.ExitSuccess
}

type uploadCmd struct{}

func (*uploadCmd) Name() string     { return "upload" }
func (*uploadCmd) Synopsis() string { return "Upload a genericstorage from stdin" }
func (*uploadCmd) Usage() string {
	return `upload <bucket URL> <key>

  Read from stdin and write to the genericstorage <key> in <bucket URL>.

  Example:
    cat foo.txt | gen-storage upload gs://mybucket my/gcs/file`
}

func (*uploadCmd) SetFlags(_ *flag.FlagSet) {}

func (*uploadCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) (status subcommands.ExitStatus) {
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

	// Copy the data.
	_, err = io.Copy(writer, os.Stdin)
	if err != nil {
		log.Printf("Failed to copy data: %v\n", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
