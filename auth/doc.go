// Package auth implements the password / token encryption schemes the
// IBM i sign-on server demands.
//
// The server announces a [password level] in its
// SignonExchangeAttributesReply (see
// [github.com/complacentsee/go-db2i/hostserver]); each level selects a
// different algorithm:
//
//	Level 0, 1: DES-CBC  (legacy; not implemented yet)
//	Level 2, 3: SHA-1    -> [EncryptPasswordSHA1]
//	Level 4:    PBKDF2-HMAC-SHA-512 + SHA-256 salt (not implemented yet)
//
// All variants take the userID, password, client seed (random 8 bytes
// the client picked for this connection), and server seed (random 8
// bytes returned by the server), and produce 20 (SHA-1) or 8 (DES) or
// up to 1024 (PBKDF2) bytes the [hostserver.SignonInfoRequest] sends
// to the server.
//
// [password level]: https://www.ibm.com/docs/en/i/7.5?topic=values-password-level-system-value-qpwdlvl
package auth
