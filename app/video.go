package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// VideoBridge liga a câmera/tela do navegador à ligação de vídeo. O meowcaller não
// codifica pixels: ele só transporta H.264 (Annex-B). Quem codifica a câmera e decodifica
// o vídeo da outra pessoa é o navegador, via WebCodecs.
//
//   - navegador -> servidor: mensagem binária = 1 access unit H.264 Annex-B da câmera
//   - servidor -> navegador: mensagem binária = 1 access unit H.264 da outra pessoa
//     mensagem texto (JSON)  = {"t":"orient","v":0..3} ou {"t":"keyframe"}
//
// Só um navegador por vez; uma conexão nova substitui a anterior.
type VideoBridge struct {
	mu      sync.Mutex
	out     chan vbOut
	cancel  context.CancelFunc
	conns   int
	onFrame func([]byte)
}

type vbOut struct {
	text bool
	data []byte
}

func NewVideoBridge() *VideoBridge {
	return &VideoBridge{out: make(chan vbOut, 32)}
}

func (b *VideoBridge) Connected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conns > 0
}

// OnFrame registra quem recebe os frames da câmera do navegador.
func (b *VideoBridge) OnFrame(fn func([]byte)) {
	b.mu.Lock()
	b.onFrame = fn
	b.mu.Unlock()
}

func (b *VideoBridge) send(m vbOut) {
	select {
	case b.out <- m:
	default: // navegador atrasado: descarta (recupera no próximo keyframe)
	}
}

// WriteFrame manda um access unit da outra pessoa para o navegador.
func (b *VideoBridge) WriteFrame(au []byte) {
	if len(au) == 0 {
		return
	}
	f := make([]byte, len(au))
	copy(f, au)
	b.send(vbOut{data: f})
}

func (b *VideoBridge) sendJSON(v any) {
	data, err := json.Marshal(v)
	if err == nil {
		b.send(vbOut{text: true, data: data})
	}
}

// SetOrientation avisa o navegador para girar o vídeo da outra pessoa (quartos de volta).
func (b *VideoBridge) SetOrientation(o int) { b.sendJSON(map[string]any{"t": "orient", "v": o}) }

// RequestKeyframe pede ao codificador do navegador que o próximo frame seja um IDR.
func (b *VideoBridge) RequestKeyframe() { b.sendJSON(map[string]any{"t": "keyframe"}) }

func (b *VideoBridge) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(4 << 20)
	ctx, cancel := context.WithCancel(r.Context())

	b.mu.Lock()
	if b.cancel != nil {
		b.cancel()
	}
	b.cancel = cancel
	b.conns++
	b.mu.Unlock()
	b.drain()

	defer func() {
		cancel()
		b.mu.Lock()
		b.conns--
		b.mu.Unlock()
		_ = conn.CloseNow()
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-b.out:
				typ := websocket.MessageBinary
				if m.text {
					typ = websocket.MessageText
				}
				if err := conn.Write(ctx, typ, m.data); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary || len(data) == 0 {
			continue
		}
		b.mu.Lock()
		fn := b.onFrame
		b.mu.Unlock()
		if fn != nil {
			fn(data)
		}
	}
}

func (b *VideoBridge) drain() {
	for {
		select {
		case <-b.out:
		default:
			return
		}
	}
}
