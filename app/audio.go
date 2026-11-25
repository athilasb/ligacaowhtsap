package main

import (
	"context"
	"encoding/binary"
	"math"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	meowcaller "github.com/purpshell/meowcaller"
)

// AudioBridge liga o navegador à ligação:
//   - navegador -> servidor: PCM int16 LE, 16 kHz mono (microfone)
//   - servidor -> navegador: PCM int16 LE, 16 kHz mono (voz da outra pessoa)
//
// Só um navegador fica conectado por vez; uma conexão nova substitui a anterior.
type AudioBridge struct {
	mu      sync.Mutex
	pending []float32      // amostras do microfone ainda sem formar um frame completo
	mic     chan []float32 // frames prontos (FrameSamples) para enviar na ligação
	out     chan []byte    // frames da outra pessoa para mandar ao navegador
	cancel  context.CancelFunc
	conns   int
}

const micQueueFrames = 6 // ~360 ms de buffer no máximo

func NewAudioBridge() *AudioBridge {
	return &AudioBridge{
		mic: make(chan []float32, micQueueFrames),
		out: make(chan []byte, 32),
	}
}

func (b *AudioBridge) Connected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conns > 0
}

func (b *AudioBridge) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(1 << 20)
	ctx, cancel := context.WithCancel(r.Context())

	b.mu.Lock()
	if b.cancel != nil {
		b.cancel() // derruba o navegador anterior
	}
	b.cancel = cancel
	b.conns++
	b.pending = nil
	b.mu.Unlock()
	b.drain()

	defer func() {
		cancel()
		b.mu.Lock()
		b.conns--
		b.pending = nil
		b.mu.Unlock()
		b.drain()
		_ = conn.CloseNow()
	}()

	// escritor: voz da outra pessoa -> navegador
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case pkt := <-b.out:
				if err := conn.Write(ctx, websocket.MessageBinary, pkt); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// leitor: microfone do navegador -> fila da ligação
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ == websocket.MessageBinary {
			b.pushMic(data)
		}
	}
}

func (b *AudioBridge) drain() {
	for {
		select {
		case <-b.mic:
		case <-b.out:
		default:
			return
		}
	}
}

func (b *AudioBridge) pushMic(data []byte) {
	n := len(data) / 2
	b.mu.Lock()
	for i := 0; i < n; i++ {
		b.pending = append(b.pending, float32(int16(binary.LittleEndian.Uint16(data[i*2:])))/32768)
	}
	var frames [][]float32
	for len(b.pending) >= meowcaller.FrameSamples {
		frame := make([]float32, meowcaller.FrameSamples)
		copy(frame, b.pending)
		b.pending = b.pending[meowcaller.FrameSamples:]
		frames = append(frames, frame)
	}
	b.mu.Unlock()
	for _, f := range frames {
		select {
		case b.mic <- f:
		default: // fila cheia: descarta o mais antigo para não acumular atraso
			select {
			case <-b.mic:
			default:
			}
			select {
			case b.mic <- f:
			default:
			}
		}
	}
}

// Source devolve um AudioSource que nunca bloqueia nem termina: se não há áudio do
// microfone, manda silêncio (o loop de envio do meowcaller lê um frame a cada 60 ms).
func (b *AudioBridge) Source() meowcaller.AudioSource { return &micSource{b: b} }

type micSource struct{ b *AudioBridge }

func (s *micSource) ReadFrame() ([]float32, error) {
	select {
	case f := <-s.b.mic:
		return f, nil
	default:
		return make([]float32, meowcaller.FrameSamples), nil
	}
}

func (s *micSource) Close() error { return nil }

// Sink converte os frames recebidos para int16 e manda ao navegador (descarta se não
// houver navegador ou se ele estiver atrasado).
func (b *AudioBridge) Sink() meowcaller.AudioSink {
	return meowcaller.SinkFunc(func(frame []float32) {
		pkt := make([]byte, len(frame)*2)
		for i, v := range frame {
			v = float32(math.Max(-1, math.Min(1, float64(v))))
			binary.LittleEndian.PutUint16(pkt[i*2:], uint16(int16(v*32767)))
		}
		select {
		case b.out <- pkt:
		default:
		}
	})
}
