package gzfile

import (
	"compress/gzip"
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"

	"github.com/thought-machine/please-servers/grpcutil"
)

func TestWriteAndReadUncompressed(t *testing.T) {
	testWriteAndRead(grpcutil.SkipCompression(context.Background()), t, "test-bucket-1")
}

func TestWriteAndReadCompressed(t *testing.T) {
	testWriteAndRead(context.Background(), t, "test-bucket-2")
}

func TestIsReallyCompressed(t *testing.T) {
	testWrite(context.Background(), t, "test-bucket-3")

	// This sucks a bit because we're assuming the bucket's internal file layout.
	// However without this test you could implement gzfile with fileblob and they'd still all pass.
	f, err := os.Open(path.Join("test-bucket-2", key))
	require.NoError(t, err)
	defer f.Close()
	gzr, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gzr.Close()
	b, err := ioutil.ReadAll(gzr)
	assert.NoError(t, err)
	assert.Equal(t, testContents, string(b))
}

func TestPartialRead(t *testing.T) {
	bucket := testWrite(context.Background(), t, "test-bucket-4")

	r, err := bucket.NewRangeReader(context.Background(), key, 500, 500, nil)
	require.NoError(t, err)
	defer r.Close()
	b, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	// N.B. The arguments to NewRangeReader are offset/length, whereas this is offset:offset+length
	assert.Equal(t, string(b), testContents[500:1000])
}

func testWriteAndRead(ctx context.Context, t *testing.T, root string) {
	bucket := testWrite(ctx, t, root)
	defer bucket.Close()
	b, err := bucket.ReadAll(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, testContents, string(b))
}

func testWrite(ctx context.Context, t *testing.T, root string) *blob.Bucket {
	bucket, err := blob.OpenBucket(ctx, "gzfile://"+root)
	require.NoError(t, err)

	err = bucket.WriteAll(ctx, key, []byte(testContents), nil)
	require.NoError(t, err)
	return bucket
}

const key = "test/file/cthulhu.txt"

const testContents = `
The most merciful thing in the world, I think, is the inability of the human mind to correlate all its contents. We live on a placid island of ignorance in the midst of black seas of infinity, and it was not meant that we should voyage far. The sciences, each straining in its own direction, have hitherto harmed us little; but some day the piecing together of dissociated knowledge will open up such terrifying vistas of reality, and of our frightful position therein, that we shall either go mad from the revelation or flee from the deadly light into the peace and safety of a new dark age.
Theosophists have guessed at the awesome grandeur of the cosmic cycle wherein our world and human race form transient incidents. They have hinted at strange survivals in terms which would freeze the blood if not masked by a bland optimism. But it is not from them that there came the single glimpse of forbidden aeons which chills me when I think of it and maddens me when I dream of it. That glimpse, like all dread glimpses of truth, flashed out from an accidental piecing together of separated things—in this case an old newspaper item and the notes of a dead professor. I hope that no one else will accomplish this piecing out; certainly, if I live, I shall never knowingly supply a link in so hideous a chain. I think that the professor, too, intended to keep silent regarding the part he knew, and that he would have destroyed his notes had not sudden death seized him.
`
