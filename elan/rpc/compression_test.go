package rpc

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thought-machine/please-servers/grpcutil"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

var bsClient bs.ByteStreamClient

const (
	// These are determined empirically from the generated test files.
	hash = "5201263d9a7365629360c09a9ab780a1c15f94aaf3b38874d41500df3e0be87f"
	size = 517788
	name = "blobs/5201263d9a7365629360c09a9ab780a1c15f94aaf3b38874d41500df3e0be87f/517788"
)

var expectedData, compressedData []byte

func TestStreamRead(t *testing.T) {
	client, err := bsClient.Read(context.Background(), &bs.ReadRequest{
		ResourceName: name,
	})
	require.NoError(t, err)
	buf := []byte{}
	for {
		msg, err := client.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		buf = append(buf, msg.Data...)
	}
	assert.Equal(t, expectedData, buf)
}

func testMain(m *testing.M) int {
	storage := "file://" + os.Getenv("TEST_DIR") + "/elan/rpc"
	lis, s := startServer(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 7777,
	}, storage, 5, 1000, 1000)
	go grpcutil.ServeForever(lis, s)
	defer s.Stop()

	conn, err := grpcutil.Dial("127.0.0.1:7777", false, "", "")
	if err != nil {
		log.Fatalf("%s", err)
	}
	bsClient = bs.NewByteStreamClient(conn)
	defer conn.Close()

	expectedData, err = ioutil.ReadFile(path.Join("elan/rpc/cas", hash[:2], hash))
	if err != nil {
		log.Fatalf("%s", err)
	}
	compressedData, err = ioutil.ReadFile(path.Join("elan/rpc/zstd_cas", hash[:2], hash))
	if err != nil {
		log.Fatalf("%s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}
