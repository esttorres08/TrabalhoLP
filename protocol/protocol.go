// Package protocol define o protocolo de comunicação do sistema P2P.
//
// Framing: cada mensagem (JSON ou bytes crus) é precedida por um header
// de 4 bytes big-endian (uint32) com o tamanho do payload:
//
//	+--------------------+----------------------------+
//	|  4 bytes (uint32)  |   payload de N bytes        |
//	|  tamanho do JSON   |   JSON em UTF-8 (ou bytes)  |
//	+--------------------+----------------------------+
//
// Para chunks binários (download de arquivos) usamos SendBytes/RecvBytes,
// que seguem a mesma ideia mas carregam bytes crus em vez de JSON.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderSize     = 4
	MaxMessageSize = 64 * 1024 * 1024

	CmdRegister   = "REGISTER"
	CmdUnregister = "UNREGISTER"
	CmdListPeers  = "LIST_PEERS"
	CmdAddFile    = "ADD_FILE"
	CmdRemoveFile = "REMOVE_FILE"
	CmdSearch     = "SEARCH"

	CmdListFiles = "LIST_FILES"
	CmdFileInfo  = "FILE_INFO"
	CmdGetChunk  = "GET_CHUNK"
	CmdChat      = "CHAT"

	StatusOK    = "ok"
	StatusError = "error"

	ChunkSize = 256 * 1024
)

// Message é o tipo usado para os payloads JSON trocados entre peers/tracker.
type Message map[string]interface{}

// recvExact recebe exatamente n bytes de r, ou retorna erro se a conexão
// fechar antes do esperado.
func recvExact(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, errors.New("conexão fechada antes do esperado")
		}
		return nil, err
	}
	return buf, nil
}

// SendMsg envia um Message como JSON com prefixo de tamanho.
func SendMsg(w io.Writer, message Message) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if len(payload) > MaxMessageSize {
		return fmt.Errorf("mensagem maior que o limite de %d bytes", MaxMessageSize)
	}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := w.Write(append(header, payload...)); err != nil {
		return err
	}
	return nil
}

// RecvMsg recebe uma mensagem JSON enviada por SendMsg.
func RecvMsg(r io.Reader) (Message, error) {
	header, err := recvExact(r, HeaderSize)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > MaxMessageSize {
		return nil, fmt.Errorf("mensagem anunciada (%d bytes) excede o limite", length)
	}
	payload, err := recvExact(r, int(length))
	if err != nil {
		return nil, err
	}
	var message Message
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, err
	}
	return message, nil
}

// SendBytes envia bytes crus com prefixo de tamanho (para chunks de arquivo).
func SendBytes(w io.Writer, data []byte) error {
	if len(data) > MaxMessageSize {
		return fmt.Errorf("bloco maior que o limite de %d bytes", MaxMessageSize)
	}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	if _, err := w.Write(append(header, data...)); err != nil {
		return err
	}
	return nil
}

// RecvBytes recebe bytes crus enviados por SendBytes.
func RecvBytes(r io.Reader) ([]byte, error) {
	header, err := recvExact(r, HeaderSize)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > MaxMessageSize {
		return nil, fmt.Errorf("bloco anunciado (%d bytes) excede o limite", length)
	}
	return recvExact(r, int(length))
}
