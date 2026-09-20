func (c *NativeClient) clientInfo() srsClientInfo {
	c.mu.Lock()
	name, freq, coal, guid, freqs, pos := c.name, c.freq, c.coalition, c.guid, append([]Frequency(nil), c.freqs...), c.pos
	c.mu.Unlock()
	radios := make([]srsRadio, 0, 11)
	seen := map[int64]bool{}
	add := func(f Frequency) {
		if f.Hz < 1_000_000 {
			return
		}
		key := int64(f.Hz / 1000)
		if seen[key] {
			return
		}
		seen[key] = true
		mod := byte(0)
		if strings.EqualFold(f.Modulation, "FM") {
			mod = 1
		}
		radios = append(radios, srsRadio{
			Frequency:      f.Hz,
			Modulation:     mod,
			GuardFrequency: -1,
		})
	}
	add(Frequency{Hz: 305_000_000, Modulation: "AM"})
	add(freq)
	for _, f := range freqs {
		add(f)
	}
	if len(radios) == 0 {
		add(Frequency{Hz: 305_000_000, Modulation: "AM"})
	}
	if len(radios) > 11 {
		radios = radios[:11]
	}
	return srsClientInfo{
		GUID:           guid,
		Name:           name,
		Seat:           0,
		Coalition:      coal,
		AllowRecording: true,
		RadioInfo: srsRadioInfo{
			Radios: radios,
			Unit:   name,
			UnitID: 100000002,
			IFF: srsIFF{
				Control: 2, Status: 0,
				Mode1: -1, Mode2: -1, Mode3: -1, Mic: -1,
			},
			Ambient: srsAmbient{Volume: 1},
		},
		Position: pos,
	}
}

func (c *NativeClient) typedMessage(t int) srsMessage {
	return srsMessage{
		Version: "2.4.0.0",
		Type:    t,
		Client:  c.clientInfo(),
	}
}

func (c *NativeClient) syncMessage() srsMessage        { return c.typedMessage(2) }
func (c *NativeClient) radioUpdateMessage() srsMessage { return c.typedMessage(3) }

func (c *NativeClient) sendJSON(msg srsMessage) error {
	c.mu.Lock()
	tcp := c.tcp
	c.mu.Unlock()
	if tcp == nil {
		return fmt.Errorf("no tcp")
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_ = tcp.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = tcp.Write(b)
	return err
}

func newSRSGUID() string {
	const alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	b := make([]byte, 22)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			b[i] = alphabet[i%len(alphabet)]
			continue
		}
		b[i] = alphabet[n.Int64()]
	}
	return string(b)
}
