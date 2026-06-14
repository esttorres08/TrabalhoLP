// Package tracker implementa o tracker centralizado do sistema P2P.
//
// O tracker NÃO transfere arquivos. Ele apenas:
//
//   - Aceita conexões TCP de peers e mantém uma conexão persistente com cada um.
//   - Guarda em memória qual peer está vivo (host, porta) e quais arquivos
//     cada peer está compartilhando.
//   - Responde consultas: lista de peers, busca de arquivos por nome.
//
// Quando um peer fecha a conexão (ou cai), seu registro é removido
// automaticamente, junto com a lista de arquivos dele.
package tracker

import (
	"fmt"
	"net"
	"sort"
	"sync"

	"trabalholp/protocol"
)

// peerInfo guarda os dados de um peer registrado.
type peerInfo struct {
	host  string
	port  int
	files map[string]struct{}
	conn  net.Conn
}

// Tracker é o servidor central que indexa peers e arquivos compartilhados.
type Tracker struct {
	host string
	port int

	mu    sync.Mutex
	peers map[string]*peerInfo

	listener net.Listener
}

// New cria um novo Tracker no host/porta informados.
func New(host string, port int) *Tracker {
	return &Tracker{
		host:  host,
		port:  port,
		peers: make(map[string]*peerInfo),
	}
}

// Start inicia o tracker no host/porta configurados.
//
// Bloqueia a goroutine atual no Accept(). Cada novo peer é tratado em sua
// própria goroutine, para que múltiplos peers possam estar conectados
// simultaneamente.
func (t *Tracker) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", t.host, t.port))
	if err != nil {
		return err
	}
	t.listener = ln

	fmt.Printf("[Tracker] Escutando em %s:%d\n", t.host, t.port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		fmt.Printf("[Tracker] Nova conexão de %s\n", conn.RemoteAddr())
		go t.handlePeerConnection(conn)
	}
}

// Stop fecha o listener do servidor para parar o Accept().
func (t *Tracker) Stop() {
	if t.listener != nil {
		t.listener.Close()
	}
}

// handlePeerConnection é o loop de mensagens com um peer enquanto a conexão
// estiver viva. Cada iteração lê uma mensagem JSON via protocol.RecvMsg,
// decide qual comando é, e responde. Quando o peer desconecta (ou erra),
// removemos ele do índice.
func (t *Tracker) handlePeerConnection(conn net.Conn) {
	addr := conn.RemoteAddr()
	var peerID string

	defer func() {
		if peerID != "" {
			t.removePeerByID(peerID)
		}
		conn.Close()
		fmt.Printf("[Tracker] Conexão encerrada com %s\n", addr)
	}()

	for {
		message, err := protocol.RecvMsg(conn)
		if err != nil {
			// Peer desconectou de forma abrupta - comportamento esperado.
			return
		}

		command, _ := message["command"].(string)

		switch command {
		case protocol.CmdRegister:
			peerID = t.handleRegister(conn, message)

		case protocol.CmdUnregister:
			t.handleUnregister(conn, message)
			return

		case protocol.CmdListPeers:
			t.handleListPeers(conn)

		case protocol.CmdAddFile:
			t.handleAddFile(conn, message)

		case protocol.CmdRemoveFile:
			t.handleRemoveFile(conn, message)

		case protocol.CmdSearch:
			t.handleSearch(conn, message)

		default:
			protocol.SendMsg(conn, protocol.Message{
				"status":  protocol.StatusError,
				"message": fmt.Sprintf("Comando desconhecido: %v", command),
			})
		}
	}
}

// handleRegister registra um peer novo. Retorna o peer_id em caso de sucesso.
func (t *Tracker) handleRegister(conn net.Conn, message protocol.Message) string {
	peerID, _ := message["peer_id"].(string)
	peerHost, _ := message["host"].(string)
	peerPort, ok := toInt(message["port"])

	if peerID == "" || peerHost == "" || !ok || peerPort == 0 {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "Dados de registro incompletos",
		})
		return ""
	}

	t.mu.Lock()
	t.peers[peerID] = &peerInfo{
		host:  peerHost,
		port:  peerPort,
		files: make(map[string]struct{}),
		conn:  conn,
	}
	t.mu.Unlock()

	fmt.Printf("[Tracker] Peer registrado: %s (%s:%d)\n", peerID, peerHost, peerPort)
	protocol.SendMsg(conn, protocol.Message{
		"status":  protocol.StatusOK,
		"message": "Registrado com sucesso",
	})
	return peerID
}

// handleUnregister remove o peer da lista a pedido dele mesmo (saída limpa).
func (t *Tracker) handleUnregister(conn net.Conn, message protocol.Message) {
	peerID, _ := message["peer_id"].(string)
	if peerID != "" && t.removePeerByID(peerID) {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusOK,
			"message": fmt.Sprintf("Peer %s removido", peerID),
		})
	} else {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "Peer não encontrado",
		})
	}
}

// handleListPeers envia o dicionário de peers atualmente registrados.
func (t *Tracker) handleListPeers(conn net.Conn) {
	t.mu.Lock()
	peersList := make(protocol.Message, len(t.peers))
	for pid, info := range t.peers {
		files := make([]string, 0, len(info.files))
		for f := range info.files {
			files = append(files, f)
		}
		sort.Strings(files)
		peersList[pid] = protocol.Message{
			"host":  info.host,
			"port":  info.port,
			"files": files,
		}
	}
	t.mu.Unlock()

	protocol.SendMsg(conn, protocol.Message{
		"status": protocol.StatusOK,
		"peers":  peersList,
	})
}

// handleAddFile anota que um peer passou a compartilhar um arquivo.
func (t *Tracker) handleAddFile(conn net.Conn, message protocol.Message) {
	peerID, _ := message["peer_id"].(string)
	filename, _ := message["filename"].(string)

	if peerID == "" || filename == "" {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "peer_id e filename são obrigatórios",
		})
		return
	}

	t.mu.Lock()
	info, exists := t.peers[peerID]
	if exists {
		info.files[filename] = struct{}{}
	}
	t.mu.Unlock()

	if !exists {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "Peer não registrado",
		})
		return
	}

	fmt.Printf("[Tracker] Peer %s agora compartilha '%s'\n", peerID, filename)
	protocol.SendMsg(conn, protocol.Message{
		"status":  protocol.StatusOK,
		"message": fmt.Sprintf("Arquivo '%s' indexado", filename),
	})
}

// handleRemoveFile remove um arquivo do índice de um peer.
func (t *Tracker) handleRemoveFile(conn net.Conn, message protocol.Message) {
	peerID, _ := message["peer_id"].(string)
	filename, _ := message["filename"].(string)

	t.mu.Lock()
	if info, exists := t.peers[peerID]; exists {
		delete(info.files, filename)
	}
	t.mu.Unlock()

	protocol.SendMsg(conn, protocol.Message{
		"status":  protocol.StatusOK,
		"message": fmt.Sprintf("Arquivo '%s' removido do índice", filename),
	})
}

// handleSearch busca quais peers anunciaram ter um determinado arquivo.
//
// Retorna a lista de peers (host/porta) que possuem o arquivo, para o
// cliente poder fazer download direto deles.
func (t *Tracker) handleSearch(conn net.Conn, message protocol.Message) {
	filename, _ := message["filename"].(string)
	if filename == "" {
		protocol.SendMsg(conn, protocol.Message{
			"status":  protocol.StatusError,
			"message": "filename é obrigatório",
		})
		return
	}

	t.mu.Lock()
	matches := make(protocol.Message)
	for pid, info := range t.peers {
		if _, ok := info.files[filename]; ok {
			matches[pid] = protocol.Message{
				"host": info.host,
				"port": info.port,
			}
		}
	}
	t.mu.Unlock()

	protocol.SendMsg(conn, protocol.Message{
		"status":   protocol.StatusOK,
		"filename": filename,
		"peers":    matches,
	})
}

// removePeerByID remove um peer do índice. Retorna true se ele existia.
func (t *Tracker) removePeerByID(peerID string) bool {
	t.mu.Lock()
	_, exists := t.peers[peerID]
	if exists {
		delete(t.peers, peerID)
	}
	t.mu.Unlock()

	if exists {
		fmt.Printf("[Tracker] Peer removido: %s\n", peerID)
	}
	return exists
}

// toInt converte um valor decodificado de JSON (float64, int ou string)
// para int. Usado porque o campo "port" pode chegar como número JSON.
func toInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	default:
		return 0, false
	}
}
