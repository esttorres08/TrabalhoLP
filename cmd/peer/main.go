// Interface de linha de comando (CLI) interativa para um peer.
//
// Uso típico:
//
//	go run ./cmd/peer 6001 [--tracker host:porta] [--share pasta]
//
// Após iniciado, digite `help` para ver os comandos disponíveis.
package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	"trabalholp/peer"
)

const helpText = `
Comandos disponíveis:

  peers                              Lista os peers online no tracker
  arquivos <peer_id>                 Lista arquivos de um peer remoto
  adicionar <caminho>                Compartilha um arquivo local
  buscar <nome>                      Pergunta ao tracker quem tem o arquivo
  baixar <nome> [pasta] [workers]    Baixa um arquivo em paralelo
                                     (padrão: pasta=downloads, workers=4)
  chat <peer_id> <mensagem>          Envia uma mensagem de chat curta
  help                               Mostra esta ajuda
  sair | quit | exit                 Desregistra do tracker e encerra

Dica: o peer_id é "<host>:<porta>" (ex.: 127.0.0.1:6001).
`

func cmdPeers(p *peer.Peer, args []string) {
	peers := p.ListPeers()
	if len(peers) == 0 {
		fmt.Println("Nenhum outro peer online.")
		return
	}
	fmt.Println("Peers online:")
	for pid, info := range peers {
		filesRepr := ""
		if len(info.Files) > 0 {
			filesRepr = fmt.Sprintf(" arquivos=%v", info.Files)
		}
		fmt.Printf("  - %s (%s:%d)%s\n", pid, info.Host, info.Port, filesRepr)
	}
}

func cmdArquivos(p *peer.Peer, args []string) {
	if len(args) == 0 {
		fmt.Println("Uso: arquivos <peer_id>")
		return
	}
	files := p.ListRemoteFiles(args[0])
	if len(files) == 0 {
		fmt.Println("Nenhum arquivo encontrado (ou peer offline).")
		return
	}
	fmt.Printf("Arquivos de %s:\n", args[0])
	for _, f := range files {
		fmt.Printf("  - %s\n", f)
	}
}

func cmdAdicionar(p *peer.Peer, args []string) {
	if len(args) == 0 {
		fmt.Println("Uso: adicionar <caminho_do_arquivo>")
		return
	}
	p.AddSharedFile(args[0])
}

func cmdBuscar(p *peer.Peer, args []string) {
	if len(args) == 0 {
		fmt.Println("Uso: buscar <nome_do_arquivo>")
		return
	}
	matches := p.SearchFile(args[0])
	delete(matches, p.PeerID)
	if len(matches) == 0 {
		fmt.Printf("Ninguém anunciou o arquivo '%s'.\n", args[0])
		return
	}
	fmt.Printf("Peers que possuem '%s':\n", args[0])
	for pid, info := range matches {
		fmt.Printf("  - %s (%s:%d)\n", pid, info.Host, info.Port)
	}
}

func cmdBaixar(p *peer.Peer, args []string) {
	if len(args) == 0 {
		fmt.Println("Uso: baixar <nome_do_arquivo> [pasta_destino=downloads] [workers=4]")
		return
	}
	filename := args[0]
	saveDir := "downloads"
	if len(args) > 1 {
		saveDir = args[1]
	}
	workers := 4
	if len(args) > 2 {
		w, err := strconv.Atoi(args[2])
		if err != nil {
			fmt.Printf("Workers inválido: %v\n", err)
			return
		}
		workers = w
	}
	p.DownloadFile(filename, saveDir, workers)
}

func cmdChat(p *peer.Peer, args []string) {
	if len(args) < 2 {
		fmt.Println("Uso: chat <peer_id> <mensagem>")
		return
	}
	recipient := args[0]
	text := strings.Join(args[1:], " ")
	if p.Chat(recipient, text) {
		fmt.Printf("Mensagem enviada para %s.\n", recipient)
	}
}

func cmdHelp(p *peer.Peer, args []string) {
	fmt.Print(helpText)
}

func buildDispatch() map[string]func(*peer.Peer, []string) {
	return map[string]func(*peer.Peer, []string){
		"peers":     cmdPeers,
		"arquivos":  cmdArquivos,
		"adicionar": cmdAdicionar,
		"buscar":    cmdBuscar,
		"baixar":    cmdBaixar,
		"chat":      cmdChat,
		"help":      cmdHelp,
	}
}

// tokenize divide uma linha em tokens respeitando aspas simples e duplas
// (equivalente simplificado de shlex.split).
func tokenize(s string) ([]string, error) {
	var tokens []string
	var current strings.Builder
	var inQuote rune
	hasToken := false

	for _, r := range s {
		switch {
		case inQuote != 0:
			if r == inQuote {
				inQuote = 0
			} else {
				current.WriteRune(r)
			}
		case r == '"' || r == '\'':
			inQuote = r
			hasToken = true
		case unicode.IsSpace(r):
			if hasToken {
				tokens = append(tokens, current.String())
				current.Reset()
				hasToken = false
			}
		default:
			current.WriteRune(r)
			hasToken = true
		}
	}

	if inQuote != 0 {
		return nil, fmt.Errorf("aspas não fechadas")
	}
	if hasToken {
		tokens = append(tokens, current.String())
	}
	return tokens, nil
}

// parseTracker faz o parsing de --tracker host:porta.
func parseTracker(value string) (string, int, error) {
	idx := strings.LastIndex(value, ":")
	if idx == -1 {
		return "", 0, fmt.Errorf("use o formato host:porta (ex.: 127.0.0.1:5000)")
	}
	host := value[:idx]
	port, err := strconv.Atoi(value[idx+1:])
	if err != nil {
		return "", 0, fmt.Errorf("use o formato host:porta (ex.: 127.0.0.1:5000)")
	}
	return host, port, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Uso: peer <porta> [--host 127.0.0.1] [--tracker host:porta] [--share pasta]")
		os.Exit(1)
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Porta inválida: %v\n", err)
		os.Exit(1)
	}

	host := "127.0.0.1"
	trackerHost := "127.0.0.1"
	trackerPort := 5000
	shareDir := ""

	rest := os.Args[2:]
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case "--host":
			i++
			if i >= len(rest) {
				fmt.Fprintln(os.Stderr, "--host requer um valor")
				os.Exit(1)
			}
			host = rest[i]
		case "--tracker":
			i++
			if i >= len(rest) {
				fmt.Fprintln(os.Stderr, "--tracker requer um valor")
				os.Exit(1)
			}
			h, p, err := parseTracker(rest[i])
			if err != nil {
				fmt.Fprintf(os.Stderr, "--tracker inválido: %v\n", err)
				os.Exit(1)
			}
			trackerHost, trackerPort = h, p
		case "--share":
			i++
			if i >= len(rest) {
				fmt.Fprintln(os.Stderr, "--share requer um valor")
				os.Exit(1)
			}
			shareDir = rest[i]
		default:
			fmt.Fprintf(os.Stderr, "Argumento desconhecido: %s\n", rest[i])
			os.Exit(1)
		}
	}

	p := peer.New(host, port, shareDir)

	if !p.Start(trackerHost, trackerPort) {
		fmt.Println("[CLI] Falha ao iniciar o peer. Verifique se o tracker está rodando.")
		os.Exit(1)
	}

	// Garante que o peer se desregistre do tracker ao receber Ctrl+C.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println()
		fmt.Println("[CLI] Encerrando peer...")
		p.Stop()
		os.Exit(0)
	}()

	fmt.Print(helpText)
	dispatch := buildDispatch()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		raw, err := reader.ReadString('\n')
		raw = strings.TrimSpace(raw)

		if raw == "" {
			if err != nil {
				// EOF (Ctrl+D)
				fmt.Println()
				break
			}
			continue
		}

		tokens, tokErr := tokenize(raw)
		if tokErr != nil {
			fmt.Printf("Comando mal formado: %v\n", tokErr)
			if err != nil {
				break
			}
			continue
		}

		cmd := strings.ToLower(tokens[0])
		cmdArgs := tokens[1:]

		if cmd == "sair" || cmd == "quit" || cmd == "exit" {
			break
		}

		handler, ok := dispatch[cmd]
		if !ok {
			fmt.Printf("Comando desconhecido: %s. Digite 'help' para ver a lista.\n", cmd)
		} else {
			handler(p, cmdArgs)
		}

		if err != nil {
			break
		}
	}

	fmt.Println("[CLI] Encerrando peer...")
	p.Stop()
}
