func (c *NativeClient) serve(ctx context.Context) error {
	errCh := make(chan error, 4)
	go func() { errCh <- c.readTCP(ctx) }()
	go func() { errCh <- c.readUDP(ctx) }()
	go func() { errCh <- c.pingLoop(ctx) }()
	go func() { errCh <- c.rxFlushLoop(ctx) }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			if err != nil && ctx.Err() == nil {
				return err
			}
		case tx := <-c.txQueue:
			if c.listenOnly {
				continue
			}
			if err := c.sendTX(tx); err != nil {
				c.log.Error("native TX failed", "error", err)
				fmt.Printf("  SRS native TX failed: %v\n", err)
			}
		}
	}
}

func (c *NativeClient) sendTX(tx Transmission) error {
	if c.listenOnly {
		return nil
	}
	if c.busy != nil && tx.Priority >= 0 {
		c.busy(true)
		defer c.busy(false)
	}
	c.rxBusy.Store(true)
	defer c.rxBusy.Store(false)

	freq := pickNativeFreq(tx)
	name := tx.Callsign
	if name == "" {
		name = c.cfg.ClientName
	}
	if name == "" {
		name = "Sky Control"
	}

	c.mu.Lock()
	needUpdate := c.name != name || absHz(c.freq.Hz-freq.Hz) > 500
	c.name = name
	c.freq = freq
	c.mu.Unlock()
	if needUpdate {
		if err := c.sendJSON(c.radioUpdateMessage()); err != nil {
			return fmt.Errorf("radio update: %w", err)
		}
		time.Sleep(80 * time.Millisecond)
	}

	src := strings.TrimSpace(tx.Spoken)
	if src == "" {
		src = tx.Text
	}
	if src == "" && len(tx.OpusFrames) == 0 {
		return fmt.Errorf("no text")
	}
	if c.speakerTTS == nil && len(tx.OpusFrames) == 0 {
		return fmt.Errorf("no piper")
	}
	var frames [][]byte
	if len(tx.OpusFrames) > 0 {
		frames = tx.OpusFrames
	} else {
		wav, err := c.speakerTTS(src)
		if err != nil || wav == "" {
			if err == nil {
				err = fmt.Errorf("empty wav")
			}
			return err
		}
		frames, err = wavToOpusFrames(wav)
		if err != nil {
			return err
		}
	}
	dur := time.Duration(len(frames)) * 40 * time.Millisecond
	if tx.Priority >= 0 {
		fmt.Printf("  SRS native TX %.3f %s as %s — %.1fs (%d frames)\n",
			freq.Hz/1_000_000, orAM(freq.Modulation), name, dur.Seconds(), len(frames))
	}

	c.mu.Lock()
	udp := c.udp
	guid := c.guid
	startID := c.packetID
	c.packetID += uint64(len(frames))
	c.mu.Unlock()
	if udp == nil {
		return fmt.Errorf("no udp")
	}

	mod := byte(0)
	if strings.EqualFold(freq.Modulation, "FM") {
		mod = 1
	}
	freqs := []srsFreq{{Hz: freq.Hz, Mod: mod}}
	start := time.Now()
	for i, opus := range frames {
		pkt := encodeVoicePacket(opus, freqs, 100000002, startID+uint64(i), []byte(guid))
		delay := time.Until(start.Add(time.Duration(i)*40*time.Millisecond - 20*time.Millisecond))
		if delay > 0 {
			time.Sleep(delay)
		}
		if _, err := udp.Write(pkt); err != nil {
			return err
		}
	}
	if tx.Priority >= 0 {
		fmt.Printf("  SRS native done %.1fs\n", time.Since(start).Seconds())
	}

	c.mu.Lock()
	c.freq = Frequency{Hz: 305_000_000, Modulation: "AM"}
	if n := strings.TrimSpace(c.cfg.ClientName); n != "" {
		c.name = n
	} else {
		c.name = "Sky Control"
	}
	c.mu.Unlock()
	_ = c.sendJSON(c.radioUpdateMessage())
	return nil
}

func pickNativeFreq(tx Transmission) Frequency {
	uhf := func(list []Frequency) Frequency {
		var vhf Frequency
		for _, f := range list {
			if f.Hz >= 200_000_000 {
				return f
			}
			if vhf.Hz == 0 && f.Hz >= 1_000_000 {
				vhf = f
			}
		}
		return vhf
	}
	if tx.Frequency.Hz >= 1_000_000 {
		return tx.Frequency
	}
	if f := uhf(tx.ExtraFreqs); f.Hz > 0 {
		return f
	}
	return Frequency{Hz: 305_000_000, Modulation: "AM"}
}

func orAM(m string) string {
	if m == "" {
		return "AM"
	}
	return m
}

func absHz(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
