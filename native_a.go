package radio

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// NativeClient stays connected to SRS and streams voice packets itself.
// It does not launch DCS-SR-ExternalAudio.exe.
type NativeClient struct {
	cfg        Config
	log        *slog.Logger
	speakerTTS func(string) (string, error)
	transcribe func(string) (string, error)

	mu        sync.Mutex
	tcp       net.Conn
	udp       *net.UDPConn
	guid      string
	name      string
	coalition int
	freq      Frequency
	packetID  uint64
	busy      func(bool)
	txQueue   chan Transmission
	rxChan    chan ReceivedCall
	freqs     []Frequency
	peers     map[string]string
	listenOnly bool
	connected  atomic.Bool
	pos        srsPosition

	rxMu     sync.Mutex
	rxGUID   string
	rxName   string
	rxFreq   Frequency
	rxFrames [][]byte
	rxLast   time.Time
	rxPktID  uint64
	rxBusy   atomic.Bool
}

func NewNativeClient(cfg Config, textToWav func(string) (string, error)) *NativeClient {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	coalition := cfg.Coalition
	if coalition == 0 {
		coalition = 2
	}
	name := cfg.ClientName
	if name == "" {
		name = "Sky Control"
	}
	f := Frequency{Hz: 305_000_000, Modulation: "AM"}
	if len(cfg.Frequencies) > 0 {
		f = cfg.Frequencies[0]
	}
	return &NativeClient{
		cfg:        cfg,
		log:        log,
		speakerTTS: textToWav,
		guid:       newSRSGUID(),
		name:       name,
		coalition:  coalition,
		freq:       f,
		packetID:   1,
		txQueue:    make(chan Transmission, 16),
		rxChan:     make(chan ReceivedCall, 8),
		freqs:      append([]Frequency(nil), cfg.Frequencies...),
		peers:      make(map[string]string),
	}
}

func NewListenClient(cfg Config) *NativeClient {
	if cfg.ClientName == "" {
		cfg.ClientName = "Sky Control"
	}
	c := NewNativeClient(cfg, nil)
	c.listenOnly = true
	return c
}

func (c *NativeClient) SetPosition(lat, lon, altFt float64) {
	c.mu.Lock()
	c.pos = srsPosition{Latitude: lat, Longitude: lon, Altitude: altFt * 0.3048}
	c.mu.Unlock()
}

func (c *NativeClient) SetTranscriber(fn func(wavPath string) (string, error)) {
	c.mu.Lock()
	c.transcribe = fn
	c.mu.Unlock()
}

func (c *NativeClient) Connected() bool {
	return c != nil && c.connected.Load()
}

func (c *NativeClient) Run(ctx context.Context) error {
	kind := "native"
	if c.listenOnly {
		kind = "listen"
	}
	c.log.Info("SRS native client starting", "mode", kind, "address", c.cfg.Address, "guid", c.guid, "name", c.name)
	if c.listenOnly {
		fmt.Printf("  SRS listen: %s @ %s\n", c.name, c.cfg.Address)
	} else {
		fmt.Println("  ATC radio: SRS native (stays connected)")
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := c.connect(ctx); err != nil {
			c.connected.Store(false)
			c.log.Warn("SRS native connect failed, retrying", "error", err)
			fmt.Printf("  SRS %s: connect failed (%v) — retry in %s\n", kind, err, backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		c.connected.Store(true)
		fmt.Printf("  SRS %s: connected (%d freqs)\n", kind, len(c.Frequencies()))
		err := c.serve(ctx)
		c.connected.Store(false)
		c.closeConn()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.Warn("SRS native disconnected, reconnecting", "error", err)
		fmt.Printf("  SRS %s: dropped — reconnecting\n", kind)
	}
}

func (c *NativeClient) connect(ctx context.Context) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	tcp, err := d.DialContext(ctx, "tcp", c.cfg.Address)
	if err != nil {
		return fmt.Errorf("tcp: %w", err)
	}
	udpAddr, err := net.ResolveUDPAddr("udp", c.cfg.Address)
	if err != nil {
		_ = tcp.Close()
		return err
	}
	udp, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		_ = tcp.Close()
		return fmt.Errorf("udp: %w", err)
	}
	c.mu.Lock()
	c.tcp = tcp
	c.udp = udp
	c.mu.Unlock()

	if err := c.sendJSON(c.syncMessage()); err != nil {
		c.closeConn()
		return fmt.Errorf("sync: %w", err)
	}
	if pw := strings.TrimSpace(c.cfg.EAMPassword); pw != "" {
		msg := c.syncMessage()
		msg.Type = 7
		msg.ExternalAWACSModePassword = pw
		_ = c.sendJSON(msg)
	}
	_ = c.sendJSON(c.radioUpdateMessage())
	c.udpPing()
	return nil
}
