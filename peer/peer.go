// Package peer implementa um peer da rede P2P estilo BitTorrent simplificado.
//
// Cada peer tem três responsabilidades:
//
//  1. Falar com o tracker (conexão TCP persistente): registrar-se, listar
//     peers, anunciar arquivos, buscar arquivos.
//
//  2. Servir requisições de OUTROS peers (servidor TCP local): listar
//     arquivos que possui, dar info de tamanho de arquivo, enviar chunks
//     (pedaços) de arquivo, receber mensagem de chat.
//
//  3. Baixar arquivos de outros peers em PARALELO, dividindo em chunks:
//     - Pergunta ao tracker quem tem o arquivo
//     - Pega FILE_INFO de um peer (tamanho + nº de chunks)
//     - Distribui os chunks entre os peers usando um pool de goroutines
//     - Cada goroutine escreve seu chunk direto no arquivo via WriteAt
//       (offsets não se sobrepõem, então não há necessidade de lock)
//     - Reassembla o arquivo na ordem correta automaticamente
package peer

import (
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"trabalholp/protocol"
)

// RemotePeer representa as informações de um peer retornadas pelo tracker.
type RemotePeer struct {
	Host  string
	Port  int
	Files []string
}

// Endpoint é um par host:porta de um peer remoto.
type Endpoint struct {
	Host string
	Port int
}

// Peer representa um peer participante da rede P2P.
type Peer struct {
	Host   string
	Port   int
	PeerID string

	// ShareDir é o diretório opcional cujo conteúdo é compartilhado
	// automaticamente no Start().
	ShareDir string

	filesMu sync.Mutex
	files   map[string]string // nome do arquivo -> caminho local

	trackerMu   sync.Mutex
	trackerConn net.Conn

	chatMu  sync.Mutex
	chatLog []ChatMessage

	listener net.Listener
	running  bool
}

// ChatMessage representa uma mensagem de chat recebida de outro peer.
type ChatMessage struct {
	From string
	Text string
	Time time.Time
}

// New cria um novo Peer.
func New(host string, port int, shareDir string) *Peer {
	return &Peer{
		Host:     host,
		Port:     port,
		PeerID:   fmt.Sprintf("%s:%d", host, port),
		ShareDir: shareDir,
		files:    make(map[string]string),
	}
}

// ------------------------------------------------------------------
// Ciclo de vida
// ------------------------------------------------------------------

// Start sobe o servidor local, conecta no tracker e registra-se.
func (p *Peer) Start(trackerHost string, trackerPort int) bool {
	if !p.startServer() {
		return false
	}

	if !p.connectTracker(trackerHost, trackerPort) {
		p.stopServer()
		return false
	}

	if p.ShareDir != "" {
		p.autoShareDir()
	}

	return true
}

// Stop desregistra do tracker, fecha sockets e para o servidor.
func (p *Peer) Stop() {
	p.unregisterFromTracker()
	p.stopServer()

	p.trackerMu.Lock()
	if p.trackerConn != nil {
		p.trackerConn.Close()
		p.trackerConn = nil
	}
	p.trackerMu.Unlock()
}

// startServer cria o listener que recebe conexões de outros peers.
func (p *Peer) startServer() bool {
	ln, err := net.Listen("tcp", net.JoinHostPort(p.Host, strconv.Itoa(p.Port)))
	if err != nil {
		fmt.Printf("[Peer] Falha ao subir servidor em %s:%d: %v\n", p.Host, p.Port, err)
		return false
	}
	p.listener = ln
	p.running = true

	go p.serverLoop()
	fmt.Printf("[Peer] Servidor escutando em %s:%d\n", p.Host, p.Port)
	return true
}

func (p *Peer) stopServer() {
	p.running = false
	if p.listener != nil {
		p.listener.Close()
		p.listener = nil
	}
}

// serverLoop aceita conexões de outros peers; cada uma vai para uma goroutine.
func (p *Peer) serverLoop() {
	for p.running {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handlePeerRequest(conn)
	}
}

// ------------------------------------------------------------------
// Lado servidor: respondendo OUTROS peers
// ------------------------------------------------------------------

// handlePeerRequest atende UMA requisição de outro peer e fecha a conexão.
func (p *Peer) handlePeerRequest(conn net.Conn) {
	defer conn.Close()

	message, err := protocol.RecvMsg(conn)
	if err != nil {
		return
	}

	command, _ := message["command"].(string)

	switch command {
	case protocol.CmdListFiles:
		p.filesMu.Lock()
		files := make([]string, 0, len(p.files))
		for name := range p.files {
			files = append(files, name)
		}
		p.filesMu.Unlock()
		sort.Strings(files)
		protocol.SendMsg(conn, protocol.Message{"status": protocol.StatusOK, "files": files})

	case protocol.CmdFileInfo:
		p.replyFileInfo(conn, message)

	case protocol.CmdGetChunk:
		p.replyChunk(conn, message)

	case protocol.CmdChat:
		sender, _ := message["from"].(string)
		if sender == "" {
			sender = "?"
		}
		text, _ := message["message"].(string)
		p.chatMu.Lock()
		p.chatLog = append(p.chatLog, ChatMessage{From: sender, Text: text, Time: time.Now()})
		p.chatMu.Unlock()
		fmt.Printf("\n[Chat] %s: %s\n> ", sender, text)
		protocol.SendMsg(conn, protocol.Message{"status": protocol.StatusOK})

	default:
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "Comando desconhecido",
		})
	}
}

func (p *Peer) replyFileInfo(conn net.Conn, message protocol.Message) {
	filename, _ := message["filename"].(string)

	p.filesMu.Lock()
	path, ok := p.files[filename]
	p.filesMu.Unlock()

	info, statErr := os.Stat(path)
	if !ok || statErr != nil {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "Arquivo não disponível",
		})
		return
	}

	size := info.Size()
	totalChunks := int(math.Ceil(float64(size) / float64(protocol.ChunkSize)))
	if totalChunks < 1 {
		totalChunks = 1
	}

	protocol.SendMsg(conn, protocol.Message{
		"status":       protocol.StatusOK,
		"filename":     filename,
		"size":         size,
		"chunk_size":   protocol.ChunkSize,
		"total_chunks": totalChunks,
	})
}

func (p *Peer) replyChunk(conn net.Conn, message protocol.Message) {
	filename, _ := message["filename"].(string)
	chunkIndex, ok := asInt(message["chunk_index"])

	p.filesMu.Lock()
	path, hasFile := p.files[filename]
	p.filesMu.Unlock()

	if !hasFile || !ok {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "Arquivo/chunk inválido",
		})
		return
	}

	f, err := os.Open(path)
	if err != nil {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": err.Error(),
		})
		return
	}
	defer f.Close()

	offset := int64(chunkIndex) * int64(protocol.ChunkSize)
	buf := make([]byte, protocol.ChunkSize)
	n, err := f.ReadAt(buf, offset)
	if err != nil && n == 0 {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": err.Error(),
		})
		return
	}
	data := buf[:n]

	protocol.SendMsg(conn, protocol.Message{
		"status":      protocol.StatusOK,
		"chunk_index": chunkIndex,
		"size":        len(data),
	})
	protocol.SendBytes(conn, data)
}

// ------------------------------------------------------------------
// Lado cliente: falando com o tracker
// ------------------------------------------------------------------

func (p *Peer) connectTracker(trackerHost string, trackerPort int) bool {
	conn, err := net.Dial("tcp", net.JoinHostPort(trackerHost, strconv.Itoa(trackerPort)))
	if err != nil {
		fmt.Printf("[Peer] Não foi possível conectar ao tracker: %v\n", err)
		return false
	}

	p.trackerMu.Lock()
	p.trackerConn = conn
	p.trackerMu.Unlock()

	response, err := p.trackerCall(protocol.Message{
		"command": protocol.CmdRegister,
		"peer_id": p.PeerID,
		"host":    p.Host,
		"port":    p.Port,
	})
	if err != nil {
		fmt.Printf("[Peer] Erro durante registro no tracker: %v\n", err)
		return false
	}

	if status, _ := response["status"].(string); status != protocol.StatusOK {
		fmt.Printf("[Peer] Tracker rejeitou registro: %v\n", response["message"])
		return false
	}

	fmt.Printf("[Peer] Registrado no tracker como %s\n", p.PeerID)
	return true
}

func (p *Peer) trackerCall(message protocol.Message) (protocol.Message, error) {
	p.trackerMu.Lock()
	defer p.trackerMu.Unlock()

	if p.trackerConn == nil {
		return nil, fmt.Errorf("não conectado ao tracker")
	}
	if err := protocol.SendMsg(p.trackerConn, message); err != nil {
		return nil, err
	}
	return protocol.RecvMsg(p.trackerConn)
}

func (p *Peer) unregisterFromTracker() {
	p.trackerMu.Lock()
	hasConn := p.trackerConn != nil
	p.trackerMu.Unlock()

	if !hasConn {
		return
	}
	p.trackerCall(protocol.Message{
		"command": protocol.CmdUnregister,
		"peer_id": p.PeerID,
	})
}

// ------------------------------------------------------------------
// Operações do usuário (chamadas pela CLI)
// ------------------------------------------------------------------

// ChatLog retorna uma cópia das mensagens de chat recebidas até agora.
func (p *Peer) ChatLog() []ChatMessage {
	p.chatMu.Lock()
	defer p.chatMu.Unlock()
	log := make([]ChatMessage, len(p.chatLog))
	copy(log, p.chatLog)
	return log
}

// SharedFiles retorna a lista (ordenada) dos arquivos compartilhados por
// este peer.
func (p *Peer) SharedFiles() []string {
	p.filesMu.Lock()
	defer p.filesMu.Unlock()
	files := make([]string, 0, len(p.files))
	for name := range p.files {
		files = append(files, name)
	}
	sort.Strings(files)
	return files
}

// SharedFilePath retorna o caminho absoluto do arquivo compartilhado com o
// nome informado, e um booleano indicando se ele existe.
func (p *Peer) SharedFilePath(filename string) (string, bool) {
	p.filesMu.Lock()
	defer p.filesMu.Unlock()
	path, ok := p.files[filename]
	return path, ok
}

// ListPeers retorna os peers conectados ao tracker (exceto este).
func (p *Peer) ListPeers() map[string]RemotePeer {
	response, err := p.trackerCall(protocol.Message{"command": protocol.CmdListPeers})
	if err != nil {
		return map[string]RemotePeer{}
	}
	peers := parseRemotePeers(response["peers"])
	delete(peers, p.PeerID)
	return peers
}

// AddSharedFile compartilha um arquivo local, anunciando-o ao tracker.
func (p *Peer) AddSharedFile(filePath string) bool {
	info, err := os.Stat(filePath)
	if err != nil || info.IsDir() {
		fmt.Printf("[Peer] Arquivo não encontrado: %s\n", filePath)
		return false
	}

	filename := filepath.Base(filePath)
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		absPath = filePath
	}

	p.filesMu.Lock()
	p.files[filename] = absPath
	p.filesMu.Unlock()

	response, err := p.trackerCall(protocol.Message{
		"command":  protocol.CmdAddFile,
		"peer_id":  p.PeerID,
		"filename": filename,
	})
	if err != nil {
		fmt.Printf("[Peer] Erro ao avisar tracker sobre arquivo: %v\n", err)
		return false
	}
	if status, _ := response["status"].(string); status != protocol.StatusOK {
		fmt.Printf("[Peer] Tracker recusou ADD_FILE: %v\n", response["message"])
		return false
	}

	fmt.Printf("[Peer] Compartilhando '%s'\n", filename)
	return true
}

// SearchFile pergunta ao tracker quem possui um arquivo.
func (p *Peer) SearchFile(filename string) map[string]RemotePeer {
	response, err := p.trackerCall(protocol.Message{
		"command":  protocol.CmdSearch,
		"filename": filename,
	})
	if err != nil {
		return map[string]RemotePeer{}
	}
	return parseRemotePeers(response["peers"])
}

// ListRemoteFiles lista os arquivos de um peer remoto.
func (p *Peer) ListRemoteFiles(peerID string) []string {
	peers := p.ListPeers()
	info, ok := peers[peerID]
	if !ok {
		fmt.Printf("[Peer] Peer %s não está online\n", peerID)
		return nil
	}

	conn, err := p.openPeerSocket(info.Host, info.Port)
	if err != nil {
		fmt.Printf("[Peer] Falha ao consultar %s: %v\n", peerID, err)
		return nil
	}
	defer conn.Close()

	if err := protocol.SendMsg(conn, protocol.Message{"command": protocol.CmdListFiles}); err != nil {
		fmt.Printf("[Peer] Falha ao consultar %s: %v\n", peerID, err)
		return nil
	}
	response, err := protocol.RecvMsg(conn)
	if err != nil {
		fmt.Printf("[Peer] Falha ao consultar %s: %v\n", peerID, err)
		return nil
	}

	filesRaw, _ := response["files"].([]interface{})
	files := make([]string, 0, len(filesRaw))
	for _, f := range filesRaw {
		if s, ok := f.(string); ok {
			files = append(files, s)
		}
	}
	return files
}

// Chat envia uma mensagem de texto curta para outro peer.
func (p *Peer) Chat(recipientID, text string) bool {
	peers := p.ListPeers()
	info, ok := peers[recipientID]
	if !ok {
		fmt.Printf("[Peer] %s não está online\n", recipientID)
		return false
	}

	conn, err := p.openPeerSocket(info.Host, info.Port)
	if err != nil {
		fmt.Printf("[Peer] Falha ao enviar chat: %v\n", err)
		return false
	}
	defer conn.Close()

	if err := protocol.SendMsg(conn, protocol.Message{
		"command": protocol.CmdChat,
		"from":    p.PeerID,
		"message": text,
	}); err != nil {
		fmt.Printf("[Peer] Falha ao enviar chat: %v\n", err)
		return false
	}
	if _, err := protocol.RecvMsg(conn); err != nil {
		fmt.Printf("[Peer] Falha ao enviar chat: %v\n", err)
		return false
	}

	return true
}

// ------------------------------------------------------------------
// Download paralelo
// ------------------------------------------------------------------

// DownloadFile baixa um arquivo de múltiplos peers em paralelo.
//
// Cada goroutine baixa um chunk e escreve direto no offset correto do
// arquivo de destino via WriteAt. Como chunks diferentes ocupam regiões
// de bytes distintas e não se sobrepõem, não é necessário nenhum lock de
// escrita entre as goroutines.
func (p *Peer) DownloadFile(filename, saveDir string, maxWorkers int) bool {
	peersWithFile := p.SearchFile(filename)
	delete(peersWithFile, p.PeerID)

	if len(peersWithFile) == 0 {
		fmt.Printf("[Peer] Nenhum peer possui '%s'\n", filename)
		return false
	}

	peerEndpoints := make([]Endpoint, 0, len(peersWithFile))
	peerIDs := make([]string, 0, len(peersWithFile))
	for pid, info := range peersWithFile {
		peerEndpoints = append(peerEndpoints, Endpoint{Host: info.Host, Port: info.Port})
		peerIDs = append(peerIDs, pid)
	}
	fmt.Printf("[Peer] Arquivo '%s' encontrado em: %s\n", filename, strings.Join(peerIDs, ", "))

	fileInfo, ok := p.fetchFileInfo(filename, peerEndpoints)
	if !ok {
		fmt.Printf("[Peer] Não foi possível obter info de '%s'\n", filename)
		return false
	}

	totalChunks, _ := asInt(fileInfo["total_chunks"])
	size, _ := asInt64(fileInfo["size"])

	if err := os.MkdirAll(saveDir, 0o755); err != nil {
		fmt.Printf("[Peer] Falha ao criar diretório '%s': %v\n", saveDir, err)
		return false
	}
	destPath := filepath.Join(saveDir, filename)

	if size == 0 {
		if f, err := os.Create(destPath); err == nil {
			f.Close()
		}
		fmt.Printf("[Peer] '%s' está vazio, nada a baixar.\n", filename)
		return true
	}

	// Cria o arquivo com tamanho final pré-alocado.
	f, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Printf("[Peer] Falha ao criar '%s': %v\n", destPath, err)
		return false
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		fmt.Printf("[Peer] Falha ao alocar '%s': %v\n", destPath, err)
		return false
	}

	startTime := time.Now()

	type chunkResult struct {
		index int
		ok    bool
	}

	results := make(chan chunkResult, totalChunks)
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	for chunkIndex := 0; chunkIndex < totalChunks; chunkIndex++ {
		primary := peerEndpoints[chunkIndex%len(peerEndpoints)]
		candidates := []Endpoint{primary}
		for _, ep := range peerEndpoints {
			if ep != primary {
				candidates = append(candidates, ep)
			}
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, candidates []Endpoint) {
			defer wg.Done()
			defer func() { <-sem }()
			ok := p.downloadChunkToFile(filename, idx, candidates, f)
			results <- chunkResult{index: idx, ok: ok}
		}(chunkIndex, candidates)
	}

	wg.Wait()
	close(results)

	f.Sync()
	f.Close()

	successes := 0
	var failures []int
	for r := range results {
		if r.ok {
			successes++
			fmt.Printf("[Peer] Chunk %d/%d OK\n", r.index+1, totalChunks)
		} else {
			failures = append(failures, r.index)
		}
	}

	duration := time.Since(startTime)

	if len(failures) > 0 {
		sort.Ints(failures)
		fmt.Printf("[Peer] Falha em %d chunks: %v\n", len(failures), failures)
		fmt.Printf("[Peer] Download de '%s' incompleto (%d/%d chunks) — duração: %.2fs\n",
			filename, successes, totalChunks, duration.Seconds())
		return false
	}

	fmt.Printf("[Peer] Download de '%s' concluído (%d/%d chunks) — duração: %.2fs\n",
		filename, successes, totalChunks, duration.Seconds())
	return true
}

// downloadChunkToFile baixa um chunk e escreve direto no offset correto do
// arquivo de destino, sem lock — cada chunk ocupa um intervalo de bytes
// distinto e nenhuma outra goroutine escreve nesse intervalo.
func (p *Peer) downloadChunkToFile(filename string, chunkIndex int, candidates []Endpoint, f *os.File) bool {
	for _, ep := range candidates {
		data, err := p.downloadOneChunk(filename, chunkIndex, ep.Host, ep.Port)
		if err != nil {
			continue
		}

		offset := int64(chunkIndex) * int64(protocol.ChunkSize)
		if _, err := f.WriteAt(data, offset); err != nil {
			continue
		}
		return true
	}
	return false
}

// fetchFileInfo tenta obter FILE_INFO de qualquer peer da lista.
func (p *Peer) fetchFileInfo(filename string, peerEndpoints []Endpoint) (protocol.Message, bool) {
	for _, ep := range peerEndpoints {
		conn, err := p.openPeerSocket(ep.Host, ep.Port)
		if err != nil {
			continue
		}

		err = protocol.SendMsg(conn, protocol.Message{
			"command":  protocol.CmdFileInfo,
			"filename": filename,
		})
		if err != nil {
			conn.Close()
			continue
		}
		response, err := protocol.RecvMsg(conn)
		conn.Close()
		if err != nil {
			continue
		}
		if status, _ := response["status"].(string); status == protocol.StatusOK {
			return response, true
		}
	}
	return nil, false
}

// downloadOneChunk baixa exatamente um chunk de um peer específico.
func (p *Peer) downloadOneChunk(filename string, chunkIndex int, host string, port int) ([]byte, error) {
	conn, err := p.openPeerSocket(host, port)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	err = protocol.SendMsg(conn, protocol.Message{
		"command":     protocol.CmdGetChunk,
		"filename":    filename,
		"chunk_index": chunkIndex,
	})
	if err != nil {
		return nil, err
	}

	meta, err := protocol.RecvMsg(conn)
	if err != nil {
		return nil, err
	}
	if status, _ := meta["status"].(string); status != protocol.StatusOK {
		return nil, fmt.Errorf("status != ok")
	}

	return protocol.RecvBytes(conn)
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

// openPeerSocket abre uma conexão TCP curta para outro peer.
func (p *Peer) openPeerSocket(host string, port int) (net.Conn, error) {
	return net.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
}

// autoShareDir compartilha automaticamente todos os arquivos de p.ShareDir.
func (p *Peer) autoShareDir() {
	entries, err := os.ReadDir(p.ShareDir)
	if err != nil {
		fmt.Printf("[Peer] share_dir não existe: %s\n", p.ShareDir)
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			p.AddSharedFile(filepath.Join(p.ShareDir, entry.Name()))
		}
	}
}

// parseRemotePeers converte o campo "peers" de uma resposta JSON do
// tracker em um map[string]RemotePeer.
func parseRemotePeers(raw interface{}) map[string]RemotePeer {
	result := make(map[string]RemotePeer)
	m, ok := raw.(map[string]interface{})
	if !ok {
		return result
	}
	for pid, v := range m {
		info, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		rp := RemotePeer{}
		if h, ok := info["host"].(string); ok {
			rp.Host = h
		}
		if port, ok := asInt(info["port"]); ok {
			rp.Port = port
		}
		if filesRaw, ok := info["files"].([]interface{}); ok {
			for _, f := range filesRaw {
				if s, ok := f.(string); ok {
					rp.Files = append(rp.Files, s)
				}
			}
		}
		result[pid] = rp
	}
	return result
}

// asInt converte um valor decodificado de JSON (float64) para int.
func asInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	default:
		return 0, false
	}
}

// asInt64 converte um valor decodificado de JSON (float64) para int64.
func asInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
