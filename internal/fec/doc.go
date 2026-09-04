// Package fec implements bounded Reed-Solomon encoding and Datagram recovery.
//
// The package does not perform authentication or network I/O. Callers must
// authenticate the complete wire packet before passing a shard to Decoder.
package fec
