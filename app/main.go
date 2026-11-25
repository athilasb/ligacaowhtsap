// Servidor de teste local: pareia um WhatsApp por QR code e faz/recebe ligações de voz
// usando o meowcaller. O áudio vem do microfone do navegador e volta para o alto-falante
// do navegador via WebSocket (PCM 16 kHz mono, int16 little-endian).
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/polymorfa/hypermeow"
	"github.com/polymorfa/hypermeow/proto/waCompanionReg"
	"github.com/polymorfa/hypermeow/store"
	"github.com/polymorfa/hypermeow/store/sqlstore"
	"github.com/polymorfa/hypermeow/types"
	"github.com/polymorfa/hypermeow/types/events"
	waLog "github.com/polymorfa/hypermeow/util/log"
	meowcaller "github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"github.com/skip2/go-qrcode"
	"google.golang.org/protobuf/proto"

	_ "modernc.org/sqlite"
)

//go:embed web
var webFiles embed.FS

var errLoggedOut = errors.New("sessão do WhatsApp desconectada")

type App struct {
	ctx       context.Context
	log       zerolog.Logger
	container *sqlstore.Container
	bridge    *AudioBridge
	video     *VideoBridge

	mu        sync.Mutex
	wa        *whatsmeow.Client
	mc        *meowcaller.Client
	status    string // starting | qr | connecting | connected | disconnected
	qr        string
	self      string
	logoutReq chan struct{}

	call      *meowcaller.Call
	callPeer  string
	callDir   string // outgoing | incoming
	callState string
	callSince time.Time
	lastEnd   string
	// vídeo da outra pessoa (último <video> recebido)
	peerVideoOn  bool
	peerUpgrade  bool // a outra pessoa pediu para virar vídeo e ainda não aceitamos
	videoSentErr bool

	events []string
}

func main() {
	dataDir := envOr("DATA_DIR", "./data")
	addr := envOr("ADDR", ":8090")

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"}).
		Level(zerolog.InfoLevel).With().Timestamp().Logger()
	ctx, stop := signal.NotifyContext(logger.WithContext(context.Background()), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		logger.Fatal().Err(err).Msg("criar pasta de dados")
	}
	store.DeviceProps.Os = proto.String("Mac OS")
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()

	dsn := "file:" + filepath.ToSlash(filepath.Join(dataDir, "whatsapp.db")) + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	container, err := sqlstore.New(ctx, "sqlite", dsn, waLog.Zerolog(logger.With().Str("mod", "db").Logger()))
	if err != nil {
		logger.Fatal().Err(err).Msg("abrir banco sqlite")
	}

	app := &App{ctx: ctx, log: logger, container: container, bridge: NewAudioBridge(), video: NewVideoBridge(), status: "starting"}
	app.video.OnFrame(app.sendVideoFrame)
	go app.runWhatsApp()

	mux := http.NewServeMux()
	static, _ := fs.Sub(webFiles, "web")
	mux.Handle("/", http.FileServer(http.FS(static)))
	mux.HandleFunc("GET /api/status", app.handleStatus)
	mux.HandleFunc("GET /qr.png", app.handleQR)
	mux.HandleFunc("POST /api/call", app.handleCall)
	mux.HandleFunc("POST /api/answer", app.handleAnswer)
	mux.HandleFunc("POST /api/reject", app.handleReject)
	mux.HandleFunc("POST /api/hangup", app.handleHangup)
	mux.HandleFunc("POST /api/logout", app.handleLogout)
	mux.HandleFunc("POST /api/video/{action}", app.handleVideo)
	mux.HandleFunc("/ws/audio", app.bridge.HandleWS)
	mux.HandleFunc("/ws/video", app.video.HandleWS)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	logger.Info().Str("addr", addr).Msg("abra no navegador: http://localhost" + addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Err(err).Msg("servidor http")
	}
	app.mu.Lock()
	if app.wa != nil {
		app.wa.Disconnect()
	}
	app.mu.Unlock()
}

// ---------- WhatsApp (pareamento e conexão) ----------

func (a *App) runWhatsApp() {
	for a.ctx.Err() == nil {
		err := a.session()
		if a.ctx.Err() != nil {
			return
		}
		a.event("WhatsApp: %v — reiniciando em 3s", err)
		select {
		case <-a.ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (a *App) session() error {
	ctx := a.ctx
	device, err := a.container.GetFirstDevice(ctx)
	if err != nil {
		return fmt.Errorf("carregar device: %w", err)
	}
	wa := whatsmeow.NewClient(device, waLog.Zerolog(a.log.With().Str("mod", "wa").Logger().Level(zerolog.WarnLevel)))
	// O meowcaller precisa ser criado ANTES de conectar o whatsmeow.
	mc := meowcaller.NewClient(wa, meowcaller.WithLogger(a.log.With().Str("mod", "call").Logger()))
	mc.OnIncomingCall(a.onIncomingCall)

	loggedOut := make(chan struct{}, 1)
	wa.AddEventHandler(func(evt any) {
		switch e := evt.(type) {
		case *events.Connected:
			a.setConnected(wa)
		case *events.Disconnected:
			a.setStatus("disconnected")
		case *events.LoggedOut:
			a.event("WhatsApp desconectou este aparelho (%v)", e.Reason)
			select {
			case loggedOut <- struct{}{}:
			default:
			}
		case *events.PairSuccess:
			a.event("QR lido com sucesso: %s", e.ID.User)
			a.setStatus("connecting")
		}
	})

	a.mu.Lock()
	a.wa, a.mc, a.qr, a.self = wa, mc, "", ""
	a.logoutReq = loggedOut
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.wa, a.mc = nil, nil
		a.mu.Unlock()
		wa.Disconnect()
	}()

	if wa.Store.ID == nil {
		qrCh, err := wa.GetQRChannel(ctx)
		if err != nil {
			return fmt.Errorf("abrir canal de QR: %w", err)
		}
		if err := wa.Connect(); err != nil {
			return fmt.Errorf("conectar: %w", err)
		}
		a.event("Aguardando leitura do QR code")
		for item := range qrCh {
			switch item.Event {
			case "code":
				a.mu.Lock()
				a.qr, a.status = item.Code, "qr"
				a.mu.Unlock()
			case "success":
				a.mu.Lock()
				a.qr = ""
				a.mu.Unlock()
			default: // timeout, err-*
				a.mu.Lock()
				a.qr = ""
				a.mu.Unlock()
				if item.Event != "success" {
					return fmt.Errorf("pareamento: %s", item.Event)
				}
			}
		}
	} else {
		a.setStatus("connecting")
		if err := wa.Connect(); err != nil {
			return fmt.Errorf("conectar: %w", err)
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-loggedOut:
		a.setStatus("disconnected")
		if wa.Store.ID != nil {
			_ = wa.Store.Delete(context.Background())
		}
		return errLoggedOut
	}
}

func (a *App) setConnected(wa *whatsmeow.Client) {
	self := ""
	if wa.Store.ID != nil {
		self = wa.Store.ID.User
	}
	if wa.Store.PushName == "" {
		wa.Store.PushName = "meowcaller teste"
	}
	_ = wa.SendPresence(a.ctx, types.PresenceAvailable)
	a.mu.Lock()
	a.status, a.self, a.qr = "connected", self, ""
	a.mu.Unlock()
	a.event("Conectado como +%s", self)
}

func (a *App) setStatus(s string) {
	a.mu.Lock()
	a.status = s
	a.mu.Unlock()
}

// ---------- Ligações ----------

func (a *App) attachCall(call *meowcaller.Call, peer, dir string) {
	a.mu.Lock()
	a.call, a.callPeer, a.callDir = call, peer, dir
	a.callState, a.callSince, a.lastEnd = phaseName(call.State()), time.Time{}, ""
	a.peerVideoOn, a.peerUpgrade, a.videoSentErr = false, false, false
	a.mu.Unlock()

	// Vídeo: H.264 da outra pessoa -> navegador decodifica; pedidos de keyframe e
	// orientação também vão para o navegador.
	call.ReceiveVideo(meowcaller.VideoSinkFunc(a.video.WriteFrame))
	call.OnVideoKeyframeRequest(a.video.RequestKeyframe)
	call.OnPeerAccept(a.video.RequestKeyframe)
	call.OnVideoState(func(s meowcaller.VideoState) {
		a.video.SetOrientation(s.Orientation)
		a.mu.Lock()
		if a.call == call {
			switch {
			case s.Upgrade:
				a.peerUpgrade = true
			case s.Active:
				a.peerVideoOn = true
			case s.Raw == 0 || s.Raw == 6:
				a.peerVideoOn = false
				a.peerUpgrade = false
			}
		}
		a.mu.Unlock()
		switch {
		case s.Upgrade:
			a.event("A outra pessoa quer ligar o vídeo")
		case s.Active:
			a.event("Vídeo da outra pessoa ligado")
		case s.Raw == 0 || s.Raw == 6:
			a.event("Vídeo da outra pessoa desligado")
		}
	})

	call.OnStateChange(func(p meowcaller.CallPhase) {
		a.mu.Lock()
		if a.call == call {
			a.callState = phaseName(p)
			if p == meowcaller.CallPhaseActive && a.callSince.IsZero() {
				a.callSince = time.Now()
			}
		}
		a.mu.Unlock()
		a.event("Ligação: %s", phaseName(p))
	})
	call.OnReady(func() {
		a.mu.Lock()
		if a.call == call && a.callSince.IsZero() {
			a.callSince = time.Now()
			a.callState = "active"
		}
		a.mu.Unlock()
		a.event("Áudio conectado")
	})
	call.OnEnd(func(reason string) {
		a.mu.Lock()
		if a.call == call {
			a.call, a.callState, a.lastEnd = nil, "", reason
		}
		a.mu.Unlock()
		a.event("Ligação encerrada (%s)", reason)
	})
	// Microfone do navegador -> ligação; ligação -> alto-falante do navegador.
	call.Play(a.bridge.Source())
	call.Receive(a.bridge.Sink())
}

func (a *App) onIncomingCall(call *meowcaller.Call) {
	a.mu.Lock()
	busy := a.call != nil
	a.mu.Unlock()
	peer := call.Peer().User
	if busy {
		a.event("Recusando ligação de %s (já em outra ligação)", peer)
		_ = call.Reject()
		return
	}
	kind := "voz"
	if call.IsVideo() {
		kind = "vídeo"
	}
	a.event("Ligação de %s recebida de %s", kind, peer)
	a.attachCall(call, peer, "incoming")
	a.mu.Lock()
	if a.call == call {
		a.callState = "incoming"
	}
	a.mu.Unlock()
}

func (a *App) handleCall(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Number string `json:"number"`
		Video  bool   `json:"video"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpErr(w, http.StatusBadRequest, "JSON inválido")
		return
	}
	number := onlyDigits(body.Number)
	if len(number) < 10 {
		httpErr(w, http.StatusBadRequest, "Número inválido. Use DDI + DDD + número, ex.: 5511999999999")
		return
	}
	a.mu.Lock()
	wa, mc, status, busy := a.wa, a.mc, a.status, a.call != nil
	a.mu.Unlock()
	if mc == nil || wa == nil || status != "connected" {
		httpErr(w, http.StatusConflict, "WhatsApp ainda não está conectado")
		return
	}
	if busy {
		httpErr(w, http.StatusConflict, "Já existe uma ligação em andamento")
		return
	}
	resolved, target, err := a.resolveNumber(wa, number)
	if err != nil {
		a.event("%v", err)
		httpErr(w, http.StatusNotFound, err.Error())
		return
	}
	if resolved != number {
		a.event("Número no WhatsApp é +%s (digitado: +%s)", resolved, number)
		number = resolved
	}
	if body.Video {
		a.event("Ligando (vídeo) para +%s…", number)
	} else {
		a.event("Ligando para +%s…", number)
	}
	// Usa o contexto da aplicação: a ligação continua depois que a requisição HTTP termina.
	call, err := mc.CallWithOptions(a.ctx, target, meowcaller.CallOptions{Video: body.Video})
	if err != nil {
		a.event("Falha ao ligar: %v", err)
		httpErr(w, http.StatusBadGateway, err.Error())
		return
	}
	a.attachCall(call, number, "outgoing")
	writeJSON(w, map[string]any{"ok": true, "call_id": call.ID()})
}

// resolveNumber pergunta ao WhatsApp qual é o número canônico. No Brasil, muitos
// celulares continuam registrados SEM o nono dígito (ex.: 55 62 9 9918-1775 vira
// 556299181775), então testamos as duas formas.
//
// Devolve o telefone (só dígitos) e o alvo da ligação. Atenção: hoje o WhatsApp
// responde o JID canônico como LID (ex.: 31151464915162@lid), que NÃO é telefone —
// usar o JID.User como número fazia a ligação ir para "+31151464915162" e falhar.
// O telefone vem de PhoneNumber (ou da própria query); o LID vira o alvo direto.
func (a *App) resolveNumber(wa *whatsmeow.Client, number string) (phone, target string, err error) {
	candidates := []string{number}
	if strings.HasPrefix(number, "55") {
		switch len(number) {
		case 13: // 55 + DDD + 9XXXXXXXX -> tenta sem o 9
			if number[4] == '9' {
				candidates = append(candidates, number[:4]+number[5:])
			}
		case 12: // 55 + DDD + XXXXXXXX -> tenta com o 9
			candidates = append(candidates, number[:4]+"9"+number[4:])
		}
	}
	queries := make([]string, len(candidates))
	for i, c := range candidates {
		queries[i] = "+" + c
	}
	ctx, cancel := context.WithTimeout(a.ctx, 15*time.Second)
	defer cancel()
	results, err := wa.IsOnWhatsApp(ctx, queries)
	if err != nil {
		return "", "", fmt.Errorf("falha ao consultar o número no WhatsApp: %w", err)
	}
	pick := func(r types.IsOnWhatsAppResponse) (string, string) {
		p := onlyDigits(r.Query)
		if r.PhoneNumber.User != "" {
			p = r.PhoneNumber.User
		} else if r.JID.Server == types.DefaultUserServer && r.JID.User != "" {
			p = r.JID.User
		}
		if r.JID.Server == types.HiddenUserServer && r.JID.User != "" {
			return p, r.JID.String() // liga direto pelo LID
		}
		return p, "+" + p
	}
	for _, c := range candidates { // respeita a ordem de preferência
		for _, r := range results {
			if r.IsIn && onlyDigits(r.Query) == c {
				phone, target = pick(r)
				return phone, target, nil
			}
		}
	}
	for _, r := range results {
		if r.IsIn {
			phone, target = pick(r)
			return phone, target, nil
		}
	}
	return "", "", fmt.Errorf("O número +%s não tem WhatsApp (testei: %s)", number, strings.Join(candidates, ", "))
}

func (a *App) currentCall() *meowcaller.Call {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.call
}

func (a *App) handleAnswer(w http.ResponseWriter, _ *http.Request) {
	call := a.currentCall()
	if call == nil {
		httpErr(w, http.StatusConflict, "Nenhuma ligação para atender")
		return
	}
	if err := call.Answer(); err != nil {
		httpErr(w, http.StatusBadGateway, err.Error())
		return
	}
	a.event("Ligação atendida")
	writeJSON(w, map[string]any{"ok": true})
}

func (a *App) handleReject(w http.ResponseWriter, _ *http.Request) {
	call := a.currentCall()
	if call == nil {
		httpErr(w, http.StatusConflict, "Nenhuma ligação")
		return
	}
	err := call.Reject()
	a.clearCall(call, "recusada")
	if err != nil {
		httpErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (a *App) handleHangup(w http.ResponseWriter, _ *http.Request) {
	call := a.currentCall()
	if call == nil {
		writeJSON(w, map[string]any{"ok": true})
		return
	}
	err := call.Hangup()
	a.clearCall(call, "desligada por você")
	if err != nil {
		httpErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// sendVideoFrame recebe um frame H.264 da câmera do navegador e manda na ligação.
func (a *App) sendVideoFrame(au []byte) {
	call := a.currentCall()
	if call == nil || !call.IsVideo() {
		return
	}
	if err := call.SendVideoWithDuration(au, time.Second/15); err != nil {
		a.mu.Lock()
		first := !a.videoSentErr && a.call == call
		a.videoSentErr = true
		a.mu.Unlock()
		if first { // loga só o primeiro, senão são 15 por segundo
			a.log.Warn().Err(err).Msg("frame de vídeo não enviado")
		}
	}
}

// handleVideo: start = pedir para virar vídeo (ou ligar a câmera de novo), accept =
// aceitar o pedido da outra pessoa, stop = desligar a minha câmera.
func (a *App) handleVideo(w http.ResponseWriter, r *http.Request) {
	call := a.currentCall()
	if call == nil {
		httpErr(w, http.StatusConflict, "Nenhuma ligação em andamento")
		return
	}
	var err error
	switch r.PathValue("action") {
	case "start":
		if call.IsSendingVideo() {
			break // ligação já é de vídeo: a câmera só volta a mandar frames
		}
		err = call.StartVideo()
		a.event("Pedindo para ligar o vídeo…")
	case "accept":
		err = call.AcceptVideo()
		a.mu.Lock()
		a.peerUpgrade = false
		a.mu.Unlock()
		a.event("Vídeo aceito")
	case "stop":
		err = call.StopVideo()
		a.event("Sua câmera foi desligada")
	default:
		httpErr(w, http.StatusNotFound, "ação desconhecida")
		return
	}
	if err != nil {
		a.event("Vídeo: %v", err)
		httpErr(w, http.StatusBadGateway, err.Error())
		return
	}
	a.video.RequestKeyframe()
	writeJSON(w, map[string]any{"ok": true})
}

func (a *App) clearCall(call *meowcaller.Call, reason string) {
	a.mu.Lock()
	if a.call == call {
		a.call, a.callState, a.lastEnd = nil, "", reason
	}
	a.mu.Unlock()
}

func (a *App) handleLogout(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	wa, ch := a.wa, a.logoutReq
	a.mu.Unlock()
	if wa == nil {
		httpErr(w, http.StatusConflict, "Sem sessão")
		return
	}
	if wa.Store.ID != nil && wa.IsLoggedIn() {
		if err := wa.Logout(a.ctx); err != nil {
			a.event("Logout: %v", err)
		}
	}
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	writeJSON(w, map[string]any{"ok": true})
}

// ---------- Status / QR ----------

func (a *App) handleStatus(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	resp := map[string]any{
		"status":     a.status,
		"has_qr":     a.qr != "",
		"qr_version": hash(a.qr),
		"self":       a.self,
		"audio_ws":   a.bridge.Connected(),
		"video_ws":   a.video.Connected(),
		"last_end":   a.lastEnd,
		"events":     append([]string(nil), a.events...),
		"call":       nil,
	}
	call := a.call
	var callInfo map[string]any
	if call != nil {
		var secs int
		if !a.callSince.IsZero() {
			secs = int(time.Since(a.callSince).Seconds())
		}
		callInfo = map[string]any{
			"peer": a.callPeer, "direction": a.callDir, "state": a.callState, "seconds": secs,
			"peer_video": a.peerVideoOn, "peer_upgrade": a.peerUpgrade,
		}
	}
	a.mu.Unlock()
	if callInfo != nil { // fora do lock: esses métodos travam o engine
		callInfo["video"] = call.IsVideo()
		callInfo["sending_video"] = call.IsSendingVideo()
		callInfo["receiving_video"] = call.IsReceivingVideo()
		resp["call"] = callInfo
	}
	writeJSON(w, resp)
}

func (a *App) handleQR(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	code := a.qr
	a.mu.Unlock()
	if code == "" {
		http.NotFound(w, nil)
		return
	}
	png, err := qrcode.Encode(code, qrcode.Medium, 320)
	if err != nil {
		httpErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(png)
}

func (a *App) event(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	a.log.Info().Msg(msg)
	a.mu.Lock()
	a.events = append(a.events, time.Now().Format("15:04:05")+"  "+msg)
	if len(a.events) > 40 {
		a.events = a.events[len(a.events)-40:]
	}
	a.mu.Unlock()
}

// ---------- utilidades ----------

func phaseName(p meowcaller.CallPhase) string {
	switch p {
	case meowcaller.CallPhaseCalling:
		return "calling"
	case meowcaller.CallPhaseRinging:
		return "ringing"
	case meowcaller.CallPhaseConnecting:
		return "connecting"
	case meowcaller.CallPhaseActive:
		return "active"
	case meowcaller.CallPhaseEnded:
		return "ended"
	case meowcaller.CallPhaseWaitingRoom:
		return "waiting_room"
	default:
		return "idle"
	}
}

func onlyDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func hash(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h = (h ^ uint32(s[i])) * 16777619
	}
	return h
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func httpErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}
