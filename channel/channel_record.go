package channel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/stripchat"
)

// streamClient is an interface satisfied by both chaturbate.Client and stripchat.Client.
type streamClient interface {
	GetStream(ctx context.Context, username string) (streamResult, error)
}

// streamResult is an interface satisfied by both *chaturbate.Stream and *stripchat.Stream.
type streamResult interface {
	GetPlaylist(ctx context.Context, resolution, framerate int) (watchable, error)
}

// watchable is an interface for any playlist that can watch segments.
type watchable interface {
	WatchSegments(ctx context.Context, handler func([]byte, float64) error) error
	GetResolution() int
	GetFramerate() int
}

// cbStreamAdapter wraps *chaturbate.Stream to satisfy streamResult.
type cbStreamAdapter struct{ s *chaturbate.Stream }

func (a *cbStreamAdapter) GetPlaylist(ctx context.Context, resolution, framerate int) (watchable, error) {
	pl, err := a.s.GetPlaylist(ctx, resolution, framerate)
	if err != nil {
		return nil, err
	}
	return &cbPlaylistAdapter{pl}, nil
}

type cbPlaylistAdapter struct{ p *chaturbate.Playlist }

func (a *cbPlaylistAdapter) WatchSegments(ctx context.Context, handler func([]byte, float64) error) error {
	return a.p.WatchSegments(ctx, handler)
}
func (a *cbPlaylistAdapter) GetResolution() int { return a.p.Resolution }
func (a *cbPlaylistAdapter) GetFramerate() int  { return a.p.Framerate }

// cbClientAdapter wraps *chaturbate.Client to satisfy streamClient.
type cbClientAdapter struct{ c *chaturbate.Client }

func (a *cbClientAdapter) GetStream(ctx context.Context, username string) (streamResult, error) {
	s, err := a.c.GetStream(ctx, username)
	if err != nil {
		return nil, err
	}
	return &cbStreamAdapter{s}, nil
}

// scStreamAdapter wraps *stripchat.Stream to satisfy streamResult.
type scStreamAdapter struct{ s *stripchat.Stream }

func (a *scStreamAdapter) GetPlaylist(ctx context.Context, resolution, framerate int) (watchable, error) {
	pl, err := a.s.GetPlaylist(ctx, resolution, framerate)
	if err != nil {
		return nil, err
	}
	return &scPlaylistAdapter{pl}, nil
}

type scPlaylistAdapter struct{ p *stripchat.Playlist }

func (a *scPlaylistAdapter) WatchSegments(ctx context.Context, handler func([]byte, float64) error) error {
	return a.p.WatchSegments(ctx, handler)
}
func (a *scPlaylistAdapter) GetResolution() int { return a.p.Resolution }
func (a *scPlaylistAdapter) GetFramerate() int  { return a.p.Framerate }

// scClientAdapter wraps *stripchat.Client to satisfy streamClient.
type scClientAdapter struct{ c *stripchat.Client }

func (a *scClientAdapter) GetStream(ctx context.Context, username string) (streamResult, error) {
	s, err := a.c.GetStream(ctx, username)
	if err != nil {
		return nil, err
	}
	return &scStreamAdapter{s}, nil
}

// newStreamClient returns the correct client based on platform config.
func newStreamClient() streamClient {
	if server.Config.Platform == "stripchat" {
		return &scClientAdapter{c: stripchat.NewClient()}
	}
	return &cbClientAdapter{c: chaturbate.NewClient()}
}

// Monitor starts monitoring the channel for live streams and records them.
func (ch *Channel) Monitor() {
	client := newStreamClient()
	ch.Info("starting to record `%s`", ch.Config.Username)

	ctx, _ := ch.WithCancel(context.Background())

	var err error
	for {
		if err = ctx.Err(); err != nil {
			break
		}

		pipeline := func() error {
			return ch.RecordStream(ctx, client)
		}
		onRetry := func(_ uint, err error) {
			ch.UpdateOnlineStatus(false)

			if errors.Is(err, internal.ErrChannelOffline) || errors.Is(err, internal.ErrPrivateStream) {
				ch.Info("channel is offline or private, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrCloudflareBlocked) {
				ch.Info("channel was blocked by Cloudflare; try with `-cookies` and `-user-agent`? try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, context.Canceled) {
				// ...
			} else {
				ch.Error("on retry: %s: retrying in %d min(s)", err.Error(), server.Config.Interval)
			}
		}
		if err = retry.Do(
			pipeline,
			retry.Context(ctx),
			retry.Attempts(0),
			retry.Delay(time.Duration(server.Config.Interval)*time.Minute),
			retry.DelayType(retry.FixedDelay),
			retry.OnRetry(onRetry),
		); err != nil {
			break
		}
	}

	if err := ch.Cleanup(); err != nil {
		ch.Error("cleanup on monitor exit: %s", err.Error())
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		ch.Error("record stream: %s", err.Error())
	}
}

// Update sends an update signal to the channel's update channel.
func (ch *Channel) Update() {
	ch.UpdateCh <- true
}

// RecordStream records the stream of the channel using the provided client.
func (ch *Channel) RecordStream(ctx context.Context, client streamClient) error {
	stream, err := client.GetStream(ctx, ch.Config.Username)
	if err != nil {
		return fmt.Errorf("get stream: %w", err)
	}
	ch.StreamedAt = time.Now().Unix()
	ch.Sequence = 0

	if err := ch.NextFile(); err != nil {
		return fmt.Errorf("next file: %w", err)
	}

	defer func() {
		if err := ch.Cleanup(); err != nil {
			ch.Error("cleanup on record stream exit: %s", err.Error())
		}
	}()

	playlist, err := stream.GetPlaylist(ctx, ch.Config.Resolution, ch.Config.Framerate)
	if err != nil {
		return fmt.Errorf("get playlist: %w", err)
	}
	ch.UpdateOnlineStatus(true)

	ch.Info("stream quality - resolution %dp (target: %dp), framerate %dfps (target: %dfps)",
		playlist.GetResolution(), ch.Config.Resolution, playlist.GetFramerate(), ch.Config.Framerate)

	return playlist.WatchSegments(ctx, ch.HandleSegment)
}

// HandleSegment processes and writes segment data to a file.
func (ch *Channel) HandleSegment(b []byte, duration float64) error {
	if ch.Config.IsPaused {
		return retry.Unrecoverable(internal.ErrPaused)
	}

	n, err := ch.File.Write(b)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	ch.Filesize += n
	ch.Duration += duration
	ch.Info("duration: %s, filesize: %s", internal.FormatDuration(ch.Duration), internal.FormatFilesize(ch.Filesize))

	ch.Update()

	if ch.ShouldSwitchFile() {
		if err := ch.NextFile(); err != nil {
			return fmt.Errorf("next file: %w", err)
		}
		ch.Info("max filesize or duration exceeded, new file created: %s", ch.File.Name())
		return nil
	}
	return nil
}
