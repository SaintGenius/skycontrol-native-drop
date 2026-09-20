func (c *NativeClient) Transmit(tx Transmission) {
	if c.listenOnly {
		return
	}
	tx.CreatedAt = time.Now()
	select {
	case c.txQueue <- tx:
	default:
		c.log.Warn("TX queue full, dropping transmission")
	}
}

func (c *NativeClient) Received() <-chan ReceivedCall { return c.rxChan }

func (c *NativeClient) Frequencies() []Frequency {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Frequency, len(c.freqs))
	copy(out, c.freqs)
	return out
}

func (c *NativeClient) SetFrequencies(freqs []Frequency) {
	c.mu.Lock()
	same := len(freqs) == len(c.freqs)
	if same {
		for i := range freqs {
			if absHz(freqs[i].Hz-c.freqs[i].Hz) > 500 {
				same = false
				break
			}
		}
	}
	c.freqs = append([]Frequency(nil), freqs...)
	if len(c.freqs) > 0 && c.freq.Hz < 1_000_000 {
		c.freq = c.freqs[0]
	}
	need := !same && c.tcp != nil
	c.mu.Unlock()
	if need {
		_ = c.sendJSON(c.radioUpdateMessage())
	}
}

func (c *NativeClient) SetBusy(fn func(bool)) {
	if c != nil {
		c.busy = fn
	}
}

type srsMessage struct {
	Version                   string            `json:"Version"`
	Client                    srsClientInfo     `json:"Client"`
	Clients                   []srsClientInfo   `json:"Clients,omitempty"`
	ServerSettings            map[string]string `json:"ServerSettings,omitempty"`
	ExternalAWACSModePassword string            `json:"ExternalAWACSModePassword,omitempty"`
	Type                      int               `json:"MsgType"`
}

type srsClientInfo struct {
	GUID           string       `json:"ClientGuid"`
	Name           string       `json:"Name"`
	Seat           int          `json:"Seat"`
	Coalition      int          `json:"Coalition"`
	AllowRecording bool         `json:"AllowRecord"`
	RadioInfo      srsRadioInfo `json:"RadioInfo"`
	Position       srsPosition  `json:"LatLngPosition"`
}

type srsRadioInfo struct {
	Radios  []srsRadio `json:"radios"`
	Unit    string     `json:"unit"`
	UnitID  uint64     `json:"unitId"`
	IFF     srsIFF     `json:"iff"`
	Ambient srsAmbient `json:"ambient"`
}

type srsRadio struct {
	Frequency        float64 `json:"freq"`
	Modulation       byte    `json:"modulation"`
	IsEncrypted      bool    `json:"enc"`
	EncryptionKey    byte    `json:"encKey"`
	GuardFrequency   float64 `json:"secFreq"`
	ShouldRetransmit bool    `json:"retransmit"`
}

type srsIFF struct {
	Control int  `json:"control"`
	Status  int  `json:"status"`
	Mode1   int  `json:"mode1"`
	Mode2   int  `json:"mode2"`
	Mode3   int  `json:"mode3"`
	Mode4   bool `json:"mode4"`
	Mic     int  `json:"mic"`
}

type srsAmbient struct {
	Volume float64 `json:"vol"`
	Type   string  `json:"abType"`
}

type srsPosition struct {
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lng"`
	Altitude  float64 `json:"alt"`
}
