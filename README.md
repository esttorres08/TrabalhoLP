# TrabalhoLP — Compartilhamento P2P de Arquivos

Sistema de compartilhamento de arquivos ponto-a-ponto (P2P) com rastreador centralizado, escrito em Go. Cada peer pode compartilhar arquivos, buscar e baixar arquivos de outros peers, e trocar mensagens de chat — tudo via linha de comando ou interface web no navegador.

## Pré-requisitos

- [Go 1.21+](https://go.dev/dl/) instalado na máquina

Verifique com:

```bash
go version
```

Não há dependências externas — tudo usa somente a biblioteca padrão do Go.

---

## Estrutura do projeto

```
TrabalhoLP/
├── cmd/
│   ├── tracker/    # Executável do tracker (servidor central)
│   └── peer/       # Executável do peer (cliente P2P)
├── protocol/       # Protocolo de comunicação (framing TCP)
├── tracker/        # Lógica do tracker
├── peer/           # Lógica do peer
└── web/            # Interface web (opcional)
```

---

## Como rodar

### 1. Clonar o repositório

```bash
git clone https://github.com/esttorres08/TrabalhoLP.git
cd TrabalhoLP
```

### 2. Iniciar o Tracker

O tracker é o servidor central que registra quais peers estão online e quais arquivos cada um possui. **Deve ser iniciado primeiro.**

```bash
go run ./cmd/tracker
```

Por padrão escuta em `0.0.0.0:5000`. Para mudar:

```bash
go run ./cmd/tracker --host 0.0.0.0 --port 5000
```

---

### 3. Iniciar um Peer

Abra um **novo terminal** para cada peer. O primeiro argumento é a porta TCP do peer.

```bash
go run ./cmd/peer 6001 --tracker 127.0.0.1:5000
```

**Opções disponíveis:**

| Flag | Padrão | Descrição |
|------|--------|-----------|
| `--host` | `127.0.0.1` | Endereço de bind do peer |
| `--tracker` | `127.0.0.1:5000` | Endereço do tracker |
| `--share <pasta>` | — | Compartilha todos os arquivos de uma pasta automaticamente ao iniciar |
| `--web <porta>` | — | Ativa a interface web no navegador |

**Exemplo com interface web:**

```bash
go run ./cmd/peer 6001 --tracker 127.0.0.1:5000 --web 8081
```

Acesse `http://127.0.0.1:8081` no navegador.

---

## Exemplo completo (3 terminais)

**Terminal 1 — Tracker:**
```bash
go run ./cmd/tracker
```

**Terminal 2 — Peer A (quem compartilha):**
```bash
go run ./cmd/peer 6001 --tracker 127.0.0.1:5000 --web 8081
```

**Terminal 3 — Peer B (quem baixa):**
```bash
go run ./cmd/peer 6002 --tracker 127.0.0.1:5000 --web 8082
```

Acesse `http://127.0.0.1:8081` (peer A) e `http://127.0.0.1:8082` (peer B) no navegador.

---

## Comandos da CLI

Após iniciar um peer, os seguintes comandos estão disponíveis no terminal:

| Comando | Descrição |
|---------|-----------|
| `peers` | Lista os peers online |
| `arquivos <peer_id>` | Lista os arquivos de um peer remoto |
| `adicionar <caminho>` | Compartilha um arquivo local |
| `buscar <nome>` | Busca quem tem determinado arquivo |
| `baixar <nome> [pasta] [workers]` | Baixa um arquivo em paralelo (padrão: `downloads/`, 4 workers) |
| `chat <peer_id> <mensagem>` | Envia uma mensagem de chat |
| `help` | Mostra a ajuda |
| `sair` | Encerra o peer |

O `peer_id` é sempre `host:porta` (ex.: `127.0.0.1:6001`).

---

## Interface Web

Quando iniciado com `--web <porta>`, o peer expõe uma interface no navegador com:

- **Peers online** — lista de peers conectados com link para ver arquivos de cada um
- **Meus arquivos compartilhados** — lista com pré-visualização de imagens e link para abrir/baixar
- **Compartilhar arquivo** — formulário para compartilhar um arquivo pelo caminho local
- **Buscar e baixar** — busca um arquivo na rede e inicia o download
- **Chat** — envia e recebe mensagens dos outros peers

> Para compartilhar uma imagem pela web, digite o caminho completo do arquivo no campo "caminho do arquivo" (ex.: `C:\Users\Seu Nome\Pictures\foto.jpg`) e clique em **Compartilhar**.

---

## Compilar os executáveis (opcional)

Para gerar os binários em vez de usar `go run`:

```bash
go build -o bin/tracker ./cmd/tracker
go build -o bin/peer   ./cmd/peer
```

Depois basta executar diretamente:

```bash
./bin/tracker
./bin/peer 6001 --tracker 127.0.0.1:5000 --web 8081
```
