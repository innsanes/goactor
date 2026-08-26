package core

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash"
)

func NewRootMessageID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(id[:]), nil
}

func GenerateMessageID(parentID, senderID, receiverID string, seq uint64) string {
	h := sha256.New()
	writeMessageIDString(h, parentID)
	writeMessageIDString(h, senderID)
	writeMessageIDString(h, receiverID)
	writeMessageIDInt(h, seq)
	digest := h.Sum(nil)
	return base64.URLEncoding.EncodeToString(digest)
}

func writeMessageIDString(h hash.Hash, value string) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], uint64(len(value)))
	_, _ = h.Write(buf[:n])
	_, _ = h.Write([]byte(value))
}

func writeMessageIDInt(h hash.Hash, value uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], value)
	_, _ = h.Write(buf[:n])
}
