package stripchat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/grafov/m3u8"
	"github.com/teacat/chaturbate-dvr/internal"
)

const apiBase = "https://stripchat.com/api/front/v2/models/username/%s/cam"

// Client represents an API client for interacting with Stripchat.
type Client struct {
	Req *internal.Req
}

// NewClient initializes and returns a new Client instance.
func NewClient() *Client {
	return &Client{
		Req: internal.NewReq(),
	}
}

// GetStream fetches the stream information for a given username.
func (c *Client) GetStream(ctx context.Context, username string) (*Stream, error) {
	return FetchStream(ctx, c.Req, username)
}

// FetchStream retrieves the HLS stream URL from the Stripchat public API.
func FetchStream(ctx context.Context, client *internal.Req, username string) (*Stream, error) {
	url := fmt.Sprintf(apiBase, username)
	body, err := client.Get(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info: %w", err)
	}

	var resp struct {
		Cam struct {
			IsBroadcasting bool   `json:"isBroadcasting"`
			StreamName     string `json:"streamName"`
		} `json:"cam"`
		User struct {
			ServerConfig struct {
				HlsPlaybackHost string `json:"hlsPlaybackHost"`
			} `json:"serverConfig"`
		} `json:"user"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, fmt.Errorf("failed to parse stream JSON: %w", err)
	}

	if !resp.Cam.IsBroadcasting {
		return nil, internal.ErrChannelOffline
	}

	hlsHost := resp.User.ServerConfig.HlsPlaybackHost
	streamName := resp.Cam.StreamName
	if hlsHost == "" || streamName == "" {
		return nil, errors.New("missing HLS host or stream name in API response")
	}

	// Build HLS master playlist URL
	// e.g. https://<hlsHost>/hls/<streamName>/master/index.m3u8
	hlsSource := fmt.Sprintf("https://%s/hls/%s/master/index.m3u8", hlsHost, streamName)

	return &Stream{HLSSource: hlsSource}, nil
}

// Stream represents an HLS stream source.
type Stream struct {
	HLSSource string
}

// GetPlaylist retrieves the playlist corresponding to the given resolution and framerate.
func (s *Stream) GetPlaylist(ctx context.Context, resolution, framerate int) (*Playlist, error) {
	return FetchPlaylist(ctx, s.HLSSource, resolution, framerate)
}

// FetchPlaylist fetches and decodes the HLS playlist file.
func FetchPlaylist(ctx context.Context, hlsSource string, resolution, framerate int) (*Playlist, error) {
	if hlsSource == "" {
		return nil, errors.New("HLS source is empty")
	}

	resp, err := internal.NewReq().Get(ctx, hlsSource)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch HLS source: %w", err)
	}

	p, _, err := m3u8.DecodeFrom(strings.NewReader(resp), true)
	if err != nil {
		return nil, fmt.Errorf("failed to decode m3u8 playlist: %w", err)
	}

	masterPlaylist, ok := p.(*m3u8.MasterPlaylist)
	if !ok {
		return nil, errors.New("invalid master playlist format")
	}

	return pickPlaylist(masterPlaylist, hlsSource, resolution, framerate)
}

// Playlist represents an HLS playlist containing variant streams.
type Playlist struct {
	PlaylistURL string
	RootURL     string
	Resolution  int
	Framerate   int
}

type resolution struct {
	framerate map[int]string
	width     int
}

func pickPlaylist(masterPlaylist *m3u8.MasterPlaylist, baseURL string, res, fps int) (*Playlist, error) {
	resolutions := map[int]*resolution{}

	for _, v := range masterPlaylist.Variants {
		if v == nil {
			continue
		}
		parts := strings.Split(v.Resolution, "x")
		if len(parts) != 2 {
			continue
		}
		width := 0
		fmt.Sscanf(parts[1], "%d", &width)
		if width == 0 {
			continue
		}
		framerateVal := 30
		if v.VariantParams.FrameRate != 0 && v.VariantParams.FrameRate >= 55 {
			framerateVal = 60
		}
		if _, exists := resolutions[width]; !exists {
			resolutions[width] = &resolution{framerate: map[int]string{}, width: width}
		}
		resolutions[width].framerate[framerateVal] = v.URI
	}

	if len(resolutions) == 0 {
		return nil, errors.New("no valid resolutions found in playlist")
	}

	variant, exists := resolutions[res]
	if !exists {
		// Pick the closest resolution below the requested one
		best := 0
		for w := range resolutions {
			if w <= res && w > best {
				best = w
			}
		}
		if best == 0 {
			// fallback: pick highest available
			for w := range resolutions {
				if w > best {
					best = w
				}
			}
		}
		variant = resolutions[best]
	}

	finalFPS := fps
	playlistURL, ok := variant.framerate[fps]
	if !ok {
		for fr, url := range variant.framerate {
			playlistURL = url
			finalFPS = fr
			break
		}
	}

	base := strings.TrimSuffix(baseURL, "index.m3u8")
	if !strings.HasPrefix(playlistURL, "http") {
		playlistURL = base + playlistURL
	}

	return &Playlist{
		PlaylistURL: playlistURL,
		RootURL:     base,
		Resolution:  variant.width,
		Framerate:   finalFPS,
	}, nil
}

// WatchHandler is a function type that processes video segments.
type WatchHandler func(b []byte, duration float64) error

// WatchSegments continuously fetches and processes video segments.
func (p *Playlist) WatchSegments(ctx context.Context, handler WatchHandler) error {
	var (
		client  = internal.NewReq()
		lastSeq = -1
	)

	for {
		resp, err := client.Get(ctx, p.PlaylistURL)
		if err != nil {
			return fmt.Errorf("get playlist: %w", err)
		}
		pl, _, err := m3u8.DecodeFrom(strings.NewReader(resp), true)
		if err != nil {
			return fmt.Errorf("decode from: %w", err)
		}
		playlist, ok := pl.(*m3u8.MediaPlaylist)
		if !ok {
			return fmt.Errorf("cast to media playlist")
		}

		for _, v := range playlist.Segments {
			if v == nil {
				continue
			}
			seq := internal.SegmentSeq(v.URI)
			if seq == -1 || seq <= lastSeq {
				continue
			}
			lastSeq = seq

			segURL := v.URI
			if !strings.HasPrefix(segURL, "http") {
				segURL = p.RootURL + segURL
			}

			pipeline := func() ([]byte, error) {
				return client.GetBytes(ctx, segURL)
			}
			data, err := retry.DoWithData(
				pipeline,
				retry.Context(ctx),
				retry.Attempts(3),
				retry.Delay(600*time.Millisecond),
				retry.DelayType(retry.FixedDelay),
			)
			if err != nil {
				break
			}
			if err := handler(data, v.Duration); err != nil {
				return fmt.Errorf("handler: %w", err)
			}
		}

		<-time.After(1 * time.Second)
	}
}
