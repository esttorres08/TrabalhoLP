// Package web fornece uma interface web simples (somente net/http e
// html/template da stdlib) para operar um Peer: ver peers online, buscar e
// baixar arquivos, compartilhar arquivos locais e trocar mensagens de chat.
package web

import (
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"trabalholp/peer"
)

// Server expõe um *peer.Peer como um pequeno servidor HTTP.
type Server struct {
	peer *peer.Peer
}

// New cria um novo Server para o peer informado.
func New(p *peer.Peer) *Server {
	return &Server{peer: p}
}

// Handler monta as rotas HTTP da interface web.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/share", s.handleShare)
	mux.HandleFunc("/search", s.handleSearch)
	mux.HandleFunc("/download", s.handleDownload)
	mux.HandleFunc("/chat", s.handleChat)
	mux.HandleFunc("/files", s.handleFiles)
	mux.HandleFunc("/view", s.handleView)
	return mux
}

// pageData é o conjunto de dados passado ao template HTML.
type pageData struct {
	PeerID string

	Peers       map[string]peer.RemotePeer
	SharedFiles []string
	ChatLog     []peer.ChatMessage

	Message string

	SearchQuery   string
	SearchResults map[string]peer.RemotePeer

	FilesPeerID string
	RemoteFiles []string
}

func (s *Server) baseData() pageData {
	return pageData{
		PeerID:      s.peer.PeerID,
		Peers:       s.peer.ListPeers(),
		SharedFiles: s.peer.SharedFiles(),
		ChatLog:     s.peer.ChatLog(),
	}
}

func (s *Server) render(w http.ResponseWriter, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pageTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	s.render(w, s.baseData())
}

func (s *Server) handleShare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	path := r.FormValue("path")
	data := s.baseData()
	if s.peer.AddSharedFile(path) {
		data.Message = fmt.Sprintf("Arquivo '%s' compartilhado com sucesso.", path)
		data.SharedFiles = s.peer.SharedFiles()
	} else {
		data.Message = fmt.Sprintf("Falha ao compartilhar '%s'.", path)
	}
	s.render(w, data)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	filename := r.FormValue("filename")
	data := s.baseData()
	data.SearchQuery = filename

	results := s.peer.SearchFile(filename)
	delete(results, s.peer.PeerID)
	data.SearchResults = results
	if len(results) == 0 {
		data.Message = fmt.Sprintf("Ninguém anunciou o arquivo '%s'.", filename)
	}
	s.render(w, data)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	filename := r.FormValue("filename")
	saveDir := r.FormValue("save_dir")
	if saveDir == "" {
		saveDir = "downloads"
	}
	workers := 4
	if raw := r.FormValue("workers"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			workers = n
		}
	}

	data := s.baseData()
	if s.peer.DownloadFile(filename, saveDir, workers) {
		data.Message = fmt.Sprintf("Download de '%s' concluído em '%s/'.", filename, saveDir)
		data.SharedFiles = s.peer.SharedFiles()
	} else {
		data.Message = fmt.Sprintf("Falha ao baixar '%s'.", filename)
	}
	s.render(w, data)
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	peerID := r.FormValue("peer_id")
	text := r.FormValue("message")

	data := s.baseData()
	if s.peer.Chat(peerID, text) {
		data.Message = fmt.Sprintf("Mensagem enviada para %s.", peerID)
	} else {
		data.Message = fmt.Sprintf("Falha ao enviar mensagem para %s.", peerID)
	}
	s.render(w, data)
}

func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	peerID := r.URL.Query().Get("peer_id")

	data := s.baseData()
	data.FilesPeerID = peerID
	if peerID != "" {
		data.RemoteFiles = s.peer.ListRemoteFiles(peerID)
		if len(data.RemoteFiles) == 0 {
			data.Message = "Nenhum arquivo encontrado (ou peer offline)."
		}
	}
	s.render(w, data)
}

// handleView exibe (ou faz o download, dependendo do tipo) o conteúdo de um
// arquivo compartilhado por este peer.
func (s *Server) handleView(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Query().Get("file")
	path, ok := s.peer.SharedFilePath(filename)
	if !ok {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, path)
}

// isImageFile indica se o arquivo deve ter uma miniatura de imagem exibida
// na lista de arquivos compartilhados.
func isImageFile(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg":
		return true
	default:
		return false
	}
}

var pageTemplate = template.Must(template.New("index").Funcs(template.FuncMap{
	"isImage": isImageFile,
}).Parse(`<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>Peer {{.PeerID}}</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; color: #222; }
  h1 { font-size: 1.4rem; }
  h2 { font-size: 1.1rem; margin-top: 2rem; border-bottom: 1px solid #ddd; padding-bottom: 0.3rem; }
  .card { background: #f7f7f9; border: 1px solid #e0e0e0; border-radius: 8px; padding: 1rem; margin-bottom: 1rem; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #eee; font-size: 0.9rem; }
  form { display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: center; }
  input { padding: 0.4rem; border: 1px solid #ccc; border-radius: 4px; }
  button { padding: 0.4rem 0.8rem; border: none; border-radius: 4px; background: #2563eb; color: #fff; cursor: pointer; }
  button:hover { background: #1d4ed8; }
  .msg { background: #fff7d6; border: 1px solid #f0e0a0; border-radius: 6px; padding: 0.5rem 0.8rem; margin-bottom: 1rem; }
  .muted { color: #777; font-size: 0.85rem; }
  code { background: #eee; padding: 0.1rem 0.3rem; border-radius: 3px; }
  .file-list { list-style: none; padding: 0; margin: 0 0 0.8rem; }
  .file-list li { padding: 0.4rem 0; border-bottom: 1px solid #eee; }
  .thumb { max-width: 200px; max-height: 150px; margin-top: 0.4rem; border-radius: 6px; border: 1px solid #ddd; display: block; }
</style>
</head>
<body>
  <h1>Peer <code>{{.PeerID}}</code> <a href="/" style="font-size:0.7em;">(atualizar)</a></h1>

  {{if .Message}}<div class="msg">{{.Message}}</div>{{end}}

  <h2>Peers online</h2>
  <div class="card">
    {{if .Peers}}
    <table>
      <tr><th>Peer ID</th><th>Endereço</th><th>Arquivos</th><th></th></tr>
      {{range $id, $info := .Peers}}
      <tr>
        <td>{{$id}}</td>
        <td>{{$info.Host}}:{{$info.Port}}</td>
        <td>{{range $f := $info.Files}}{{$f}} {{end}}</td>
        <td><a href="/files?peer_id={{$id}}">ver arquivos</a></td>
      </tr>
      {{end}}
    </table>
    {{else}}
    <p class="muted">Nenhum outro peer online.</p>
    {{end}}
  </div>

  {{if .FilesPeerID}}
  <h2>Arquivos de {{.FilesPeerID}}</h2>
  <div class="card">
    {{if .RemoteFiles}}
    <ul>{{range .RemoteFiles}}<li>{{.}}</li>{{end}}</ul>
    {{else}}
    <p class="muted">Nenhum arquivo encontrado.</p>
    {{end}}
  </div>
  {{end}}

  <h2>Meus arquivos compartilhados</h2>
  <div class="card">
    {{if .SharedFiles}}
    <ul class="file-list">
      {{range .SharedFiles}}
      <li>
        {{.}} — <a href="/view?file={{.}}" target="_blank">ver / baixar</a>
        {{if isImage .}}
        <div><img src="/view?file={{.}}" alt="{{.}}" class="thumb"></div>
        {{end}}
      </li>
      {{end}}
    </ul>
    {{else}}
    <p class="muted">Nenhum arquivo compartilhado ainda.</p>
    {{end}}
    <form method="post" action="/share">
      <input type="text" name="path" placeholder="caminho do arquivo" size="40" required>
      <button type="submit">Compartilhar</button>
    </form>
  </div>

  <h2>Buscar e baixar arquivo</h2>
  <div class="card">
    <form method="post" action="/search">
      <input type="text" name="filename" placeholder="nome do arquivo" value="{{.SearchQuery}}" required>
      <button type="submit">Buscar</button>
    </form>

    {{if .SearchResults}}
    <table>
      <tr><th>Peer ID</th><th>Endereço</th></tr>
      {{range $id, $info := .SearchResults}}
      <tr><td>{{$id}}</td><td>{{$info.Host}}:{{$info.Port}}</td></tr>
      {{end}}
    </table>
    {{end}}

    <form method="post" action="/download" style="margin-top:0.8rem;">
      <input type="text" name="filename" placeholder="nome do arquivo" value="{{.SearchQuery}}" required>
      <input type="text" name="save_dir" placeholder="pasta destino (downloads)" size="20">
      <input type="number" name="workers" placeholder="workers (4)" min="1" size="6">
      <button type="submit">Baixar</button>
    </form>
  </div>

  <h2>Chat</h2>
  <div class="card">
    <form method="post" action="/chat">
      <input type="text" name="peer_id" placeholder="peer_id (host:porta)" size="20" required>
      <input type="text" name="message" placeholder="mensagem" size="30" required>
      <button type="submit">Enviar</button>
    </form>

    {{if .ChatLog}}
    <table style="margin-top:0.8rem;">
      <tr><th>Hora</th><th>De</th><th>Mensagem</th></tr>
      {{range .ChatLog}}
      <tr><td class="muted">{{.Time.Format "15:04:05"}}</td><td>{{.From}}</td><td>{{.Text}}</td></tr>
      {{end}}
    </table>
    {{else}}
    <p class="muted">Nenhuma mensagem recebida ainda.</p>
    {{end}}
  </div>
</body>
</html>
`))
