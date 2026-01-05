package abssas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("abssas", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "abssas"

// MetadataKeyTimestamp is the metadata key for storing LTX file timestamps in Azure Blob Storage.
// Azure metadata keys cannot contain hyphens, so we use litestreamtimestamp (C# identifier rules).
const MetadataKeyTimestamp = "litestreamtimestamp"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to Azure Blob Storage using SAS URL authentication.
// SAS (Shared Access Signature) URLs allow granular access control to specific containers or paths
// without requiring full account credentials.
type ReplicaClient struct {
	mu     sync.Mutex
	client *azblob.Client
	logger *slog.Logger

	// SASURL is the full Azure Blob Storage SAS URL with authentication token
	// Example: https://account.blob.core.windows.net/container/path?sv=...&sig=...
	SASURL string

	// Container is the Azure Blob Storage container name (extracted from SASURL)
	Container string

	// Path is the prefix path within the container (extracted from SASURL)
	Path string
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient(sasURL string) (*ReplicaClient, error) {
	// Parse the SAS URL
	u, err := url.Parse(sasURL)
	if err != nil {
		return nil, fmt.Errorf("abssas: cannot parse SAS URL: %w", err)
	}

	// Extract container name and path from URL path
	// Path format: /container/optional/path
	pathParts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(pathParts) < 1 || pathParts[0] == "" {
		return nil, fmt.Errorf("abssas: invalid SAS URL format: missing container name")
	}

	// First part is the container name
	container := pathParts[0]

	// The rest is the path (if any)
	var pathPrefix string
	if len(pathParts) > 1 {
		pathPrefix = strings.Join(pathParts[1:], "/")
	}

	return &ReplicaClient{
		logger:    slog.Default().WithGroup(ReplicaClientType),
		SASURL:    sasURL,
		Container: container,
		Path:      pathPrefix,
	}, nil
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
// URL format: abssas://storageaccount.blob.core.windows.net/container/path?sv=...&sig=...
// The entire URL (excluding the abssas:// scheme) is used as the SAS URL with https:// scheme.
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	// Reconstruct the full SAS URL with https:// scheme
	sasURL := "https://" + host + urlPath
	if len(query) > 0 {
		sasURL += "?" + query.Encode()
	}
	return NewReplicaClient(sasURL)
}

// Type returns "abssas" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to Azure. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return nil
	}

	// Validate required configuration
	if c.SASURL == "" {
		return fmt.Errorf("abssas: SAS URL is required")
	}
	if c.Container == "" {
		return fmt.Errorf("abssas: container name is required")
	}

	// Parse the SAS URL to extract the service endpoint
	u, err := url.Parse(c.SASURL)
	if err != nil {
		return fmt.Errorf("abssas: cannot parse SAS URL: %w", err)
	}

	// Build the service URL (scheme + host + query params with SAS token)
	// The azblob client needs the service URL, not the full blob URL
	serviceURL := fmt.Sprintf("%s://%s/?%s", u.Scheme, u.Host, u.RawQuery)

	// Configure client options with retry policy
	clientOptions := &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    10,
				RetryDelay:    time.Second,
				MaxRetryDelay: 30 * time.Second,
				TryTimeout:    15 * time.Minute, // Reasonable timeout for blob operations
				StatusCodes: []int{
					http.StatusRequestTimeout,
					http.StatusTooManyRequests,
					http.StatusInternalServerError,
					http.StatusBadGateway,
					http.StatusServiceUnavailable,
					http.StatusGatewayTimeout,
				},
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "litestream",
			},
		},
	}

	// Use SAS URL authentication (no credentials needed, auth is in the URL)
	slog.Debug("abssas: using SAS URL authentication")
	c.client, err = azblob.NewClientWithNoCredential(serviceURL, clientOptions)
	if err != nil {
		return fmt.Errorf("abssas: cannot create azure blob client with SAS URL: %w", err)
	}

	return nil
}

// LTXFiles returns an iterator over all available LTX files.
// Azure always uses accurate timestamps from metadata since they're included in LIST operations at zero cost.
// The useMetadata parameter is ignored.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	return newLTXFileIterator(ctx, c, level, seek), nil
}

// WriteLTXFile writes an LTX file to remote storage.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

	// Use TeeReader to peek at LTX header while preserving data for upload
	var buf bytes.Buffer
	teeReader := io.TeeReader(rd, &buf)

	// Extract timestamp from LTX header
	hdr, _, err := ltx.PeekHeader(teeReader)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()

	// Combine buffered data with rest of reader
	rc := internal.NewReadCounter(io.MultiReader(&buf, rd))

	// Upload blob with proper content type, access tier, and metadata
	// Azure metadata keys cannot contain hyphens, so use litestreamtimestamp
	_, err = c.client.UploadStream(ctx, c.Container, key, rc, &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr("application/octet-stream"),
		},
		AccessTier: to.Ptr(blob.AccessTierHot), // Use Hot tier as default
		Metadata: map[string]*string{
			MetadataKeyTimestamp: to.Ptr(timestamp.Format(time.RFC3339Nano)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("abssas: cannot upload ltx file %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      rc.N(),
		CreatedAt: timestamp,
	}, nil
}

// OpenLTXFile returns a reader for an LTX file.
// Returns os.ErrNotExist if no matching min/max TXID is not found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	resp, err := c.client.DownloadStream(ctx, c.Container, key, &azblob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: offset,
			Count:  size,
		},
	})

	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, fmt.Errorf("abssas: cannot start new reader for %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(*resp.ContentLength))

	return resp.Body, nil
}

// DeleteLTXFiles deletes LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	for _, info := range a {
		key := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "key", key)

		_, err := c.client.DeleteBlob(ctx, c.Container, key, nil)
		if isNotExists(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("abssas: cannot delete ltx file %q: %w", key, err)
		}

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	// List all blobs with the configured path prefix
	prefix := "/"
	if c.Path != "" {
		prefix = strings.TrimSuffix(c.Path, "/") + "/"
	}

	pager := c.client.NewListBlobsFlatPager(c.Container, &azblob.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: azblob.ListBlobsInclude{Metadata: true},
	})

	for pager.More() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("abssas: cannot list blobs: %w", err)
		}

		for _, item := range resp.Segment.BlobItems {
			internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

			_, err := c.client.DeleteBlob(ctx, c.Container, *item.Name, nil)
			if isNotExists(err) {
				continue
			} else if err != nil {
				return fmt.Errorf("abssas: cannot delete blob %q: %w", *item.Name, err)
			}
		}
	}

	return nil
}

type ltxFileIterator struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *ReplicaClient
	level  int
	seek   ltx.TXID

	pager     *runtime.Pager[azblob.ListBlobsFlatResponse]
	pageItems []*ltx.FileInfo
	pageIndex int

	closed bool
	err    error
	info   *ltx.FileInfo
}

func newLTXFileIterator(ctx context.Context, client *ReplicaClient, level int, seek ltx.TXID) *ltxFileIterator {
	ctx, cancel := context.WithCancel(ctx)

	itr := &ltxFileIterator{
		ctx:    ctx,
		cancel: cancel,
		client: client,
		level:  level,
		seek:   seek,
	}

	// Create paginator for listing blobs with level prefix
	dir := litestream.LTXLevelDir(client.Path, level)
	prefix := dir + "/"
	if seek != 0 {
		prefix += seek.String()
	}

	itr.pager = client.client.NewListBlobsFlatPager(client.Container, &azblob.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: azblob.ListBlobsInclude{Metadata: true},
	})

	return itr
}

func (itr *ltxFileIterator) Close() (err error) {
	itr.closed = true
	itr.cancel()
	return nil
}

func (itr *ltxFileIterator) Next() bool {
	if itr.closed || itr.err != nil {
		return false
	}

	// Process blobs until we find a valid LTX file
	for {
		// Load next page if needed
		if itr.pageItems == nil || itr.pageIndex >= len(itr.pageItems) {
			if !itr.loadNextPage() {
				return false
			}
		}

		// Process current item from page
		if itr.pageIndex < len(itr.pageItems) {
			itr.info = itr.pageItems[itr.pageIndex]
			itr.pageIndex++
			return true
		}
	}
}

// loadNextPage loads the next page of blobs and extracts valid LTX files
func (itr *ltxFileIterator) loadNextPage() bool {
	if !itr.pager.More() {
		return false
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

	resp, err := itr.pager.NextPage(itr.ctx)
	if err != nil {
		itr.err = fmt.Errorf("abssas: cannot list blobs: %w", err)
		return false
	}

	// Extract blob items directly from the response
	itr.pageItems = nil
	itr.pageIndex = 0

	for _, item := range resp.Segment.BlobItems {
		key := path.Base(*item.Name)
		minTXID, maxTXID, err := ltx.ParseFilename(key)
		if err != nil {
			continue // Skip non-LTX files
		}

		// Build file info
		info := &ltx.FileInfo{
			Level:   itr.level,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
			Size:    *item.Properties.ContentLength,
		}

		// Skip if below seek TXID
		if info.MinTXID < itr.seek {
			continue
		}

		// Skip if wrong level
		if info.Level != itr.level {
			continue
		}

		// Always use accurate timestamp from metadata since it's zero-cost
		// Azure includes metadata in LIST operations, so no extra API call needed
		info.CreatedAt = item.Properties.CreationTime.UTC()
		if item.Metadata != nil {
			if ts, ok := item.Metadata[MetadataKeyTimestamp]; ok && ts != nil {
				if parsed, err := time.Parse(time.RFC3339Nano, *ts); err == nil {
					info.CreatedAt = parsed
				}
			}
		}

		itr.pageItems = append(itr.pageItems, info)
	}

	return len(itr.pageItems) > 0 || itr.pager.More()
}

func (itr *ltxFileIterator) Err() error { return itr.err }

func (itr *ltxFileIterator) Item() *ltx.FileInfo {
	return itr.info
}

func isNotExists(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.ErrorCode == string(bloberror.BlobNotFound) || respErr.ErrorCode == string(bloberror.ContainerNotFound)
	}
	return false
}
