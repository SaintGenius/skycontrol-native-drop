func (c *NativeClient) pingLoop(ctx context.Context) error {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	c.udpPing()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_ = c.sendJSON(c.typedMessage(1))
			c.udpPing()
		}
	}
}

func (c *NativeClient) udpPing() {
	c.mu.Lock()
	udp, guid := c.udp, c.guid
	c.mu.Unlock()
	if udp != nil {
		_, _ = udp.Write([]byte(guid))
	}
}

func (c *NativeClient) readTCP(ctx context.Context) error {
	c.mu.Lock()
	tcp := c.tcp
	c.mu.Unlock()
	if tcp == nil {
		return fmt.Errorf("no tcp")
	}
	r := bufio.NewReader(tcp)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = tcp.SetReadDeadline(time.Now().Add(30 * time.Second))
		line, err := r.ReadBytes('\n')
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		var msg srsMessage
		if json.Unmarshal(line, &msg) != nil {
			continue
		}
		c.noteClients(msg)
		if msg.Type == 6 {
			c.log.Warn("SRS version mismatch", "server", msg.Version)
		}
	}
}

func (c *NativeClient) noteClients(msg srsMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.peers == nil {
		c.peers = make(map[string]string)
	}
	add := func(info srsClientInfo) {
		if info.GUID == "" {
			return
		}
		if info.GUID == c.guid {
			return
		}
		n := strings.TrimSpace(info.Name)
		if n == "" {
			n = "Pilot"
		}
		c.peers[info.GUID] = n
	}
	add(msg.Client)
	for _, p := range msg.Clients {
		add(p)
	}
}

func (c *NativeClient) peerName(guid string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := c.peers[guid]; n != "" {
		return n
	}
	return "Pilot"
}

func (c *NativeClient) readUDP(ctx context.Context) error {
	buf := make([]byte, 4096)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.mu.Lock()
		udp := c.udp
		c.mu.Unlock()
		if udp == nil {
			return fmt.Errorf("no udp")
		}
		_ = udp.SetReadDeadline(time.Now().Add(20 * time.Second))
		n, err := udp.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		if n == 22 {
			continue
		}
		c.handleVoice(buf[:n])
	}
}

func (c *NativeClient) rxFlushLoop(ctx context.Context) error {
	t := time.NewTicker(80 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.flushRX(false)
		}
	}
}

func (c *NativeClient) closeConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tcp != nil {
		_ = c.tcp.Close()
		c.tcp = nil
	}
	if c.udp != nil {
		_ = c.udp.Close()
		c.udp = nil
	}
}
