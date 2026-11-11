package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type ggufFacts struct {
	NCtx     int64
	NParams  int64
	SizeByte int64
	Quant    string
}

const (
	ggufTypeU8  = 0
	ggufTypeI8  = 1
	ggufTypeU16 = 2
	ggufTypeI16 = 3
	ggufTypeU32 = 4
	ggufTypeI32 = 5
	ggufTypeF32 = 6
	ggufTypeB   = 7
	ggufTypeStr = 8
	ggufTypeArr = 9
	ggufTypeU64 = 10
	ggufTypeI64 = 11
	ggufTypeF64 = 12
)

var (
	ggufMu    sync.Mutex
	ggufCache = map[string]ggufFacts{}
)

func ggufOf(path string) ggufFacts {
	if path == "" {
		return ggufFacts{}
	}
	ggufMu.Lock()
	if f, ok := ggufCache[path]; ok {
		ggufMu.Unlock()
		return f
	}
	ggufMu.Unlock()
	f, err := readGGUF(path)
	if err != nil {
		return ggufFacts{}
	}
	ggufMu.Lock()
	ggufCache[path] = f
	ggufMu.Unlock()
	return f
}

func readGGUF(path string) (ggufFacts, error) {
	fh, err := os.Open(path)
	if err != nil {
		return ggufFacts{}, err
	}
	defer fh.Close()
	var hdr [4]byte
	if _, err := io.ReadFull(fh, hdr[:]); err != nil || string(hdr[:]) != "GGUF" {
		return ggufFacts{}, fmt.Errorf("not a gguf")
	}
	var version uint32
	var tensors, kvs uint64
	if err := binary.Read(fh, binary.LittleEndian, &version); err != nil {
		return ggufFacts{}, err
	}
	if version < 2 {
		return ggufFacts{}, fmt.Errorf("gguf v%d unsupported", version)
	}
	if err := binary.Read(fh, binary.LittleEndian, &tensors); err != nil {
		return ggufFacts{}, err
	}
	if err := binary.Read(fh, binary.LittleEndian, &kvs); err != nil {
		return ggufFacts{}, err
	}
	var fileType uint32
	var archCtx int64
	for i := uint64(0); i < kvs; i++ {
		key, err := readGGUFString(fh)
		if err != nil {
			return ggufFacts{}, err
		}
		var typ uint32
		if err := binary.Read(fh, binary.LittleEndian, &typ); err != nil {
			return ggufFacts{}, err
		}
		switch typ {
		case ggufTypeU32:
			var v uint32
			if err := binary.Read(fh, binary.LittleEndian, &v); err != nil {
				return ggufFacts{}, err
			}
			switch {
			case key == "general.file_type":
				fileType = v
			case strings.HasSuffix(key, ".context_length"):
				archCtx = int64(v)
			}
		case ggufTypeI32:
			var v int32
			if err := binary.Read(fh, binary.LittleEndian, &v); err != nil {
				return ggufFacts{}, err
			}
			if strings.HasSuffix(key, ".context_length") {
				archCtx = int64(v)
			}
		case ggufTypeU64, ggufTypeI64:
			var v uint64
			if err := binary.Read(fh, binary.LittleEndian, &v); err != nil {
				return ggufFacts{}, err
			}
			if strings.HasSuffix(key, ".context_length") {
				archCtx = int64(v)
			}
		default:
			if err := skipGGUFValue(fh, typ); err != nil {
				return ggufFacts{}, err
			}
		}
	}
	var params int64
	for i := uint64(0); i < tensors; i++ {
		if _, err := readGGUFString(fh); err != nil {
			return ggufFacts{}, err
		}
		var nDims uint32
		if err := binary.Read(fh, binary.LittleEndian, &nDims); err != nil {
			return ggufFacts{}, err
		}
		count := int64(1)
		for d := uint32(0); d < nDims; d++ {
			var dim uint64
			if err := binary.Read(fh, binary.LittleEndian, &dim); err != nil {
				return ggufFacts{}, err
			}
			count *= int64(dim)
		}
		var typ uint32
		var off uint64
		if err := binary.Read(fh, binary.LittleEndian, &typ); err != nil {
			return ggufFacts{}, err
		}
		if err := binary.Read(fh, binary.LittleEndian, &off); err != nil {
			return ggufFacts{}, err
		}
		params += count
	}
	fi, err := fh.Stat()
	if err != nil {
		return ggufFacts{}, err
	}
	return ggufFacts{NCtx: archCtx, NParams: params, SizeByte: fi.Size(), Quant: fileTypeName(fileType)}, nil
}

func readGGUFString(r io.Reader) (string, error) {
	var n uint64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return "", err
	}
	if n > 1<<20 {
		return "", fmt.Errorf("string too long")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func skipGGUFValue(r io.Reader, typ uint32) error {
	fixed := map[uint32]int64{
		ggufTypeU8: 1, ggufTypeI8: 1, ggufTypeU16: 2, ggufTypeI16: 2, ggufTypeU32: 4,
		ggufTypeI32: 4, ggufTypeF32: 4, ggufTypeB: 1, ggufTypeU64: 8, ggufTypeI64: 8, ggufTypeF64: 8,
	}
	if n, ok := fixed[typ]; ok {
		_, err := io.CopyN(io.Discard, r, n)
		return err
	}
	switch typ {
	case ggufTypeStr:
		_, err := readGGUFString(r)
		return err
	case ggufTypeArr:
		var elem uint32
		if err := binary.Read(r, binary.LittleEndian, &elem); err != nil {
			return err
		}
		var count uint64
		if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
			return err
		}
		if n, ok := map[uint32]int64{ggufTypeU8: 1, ggufTypeI8: 1, ggufTypeU16: 2, ggufTypeI16: 2,
			ggufTypeU32: 4, ggufTypeI32: 4, ggufTypeF32: 4, ggufTypeB: 1, ggufTypeU64: 8,
			ggufTypeI64: 8, ggufTypeF64: 8}[elem]; ok {
			if count > 1<<28 {
				return fmt.Errorf("array too long")
			}
			_, err := io.CopyN(io.Discard, r, int64(count)*n)
			return err
		}
		for i := uint64(0); i < count; i++ {
			if err := skipGGUFValue(r, elem); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unknown gguf type %d", typ)
}

func fileTypeName(t uint32) string {
	names := map[uint32]string{
		0: "F32", 1: "F16", 2: "Q4_0", 3: "Q4_1", 7: "Q8_0", 8: "Q5_0", 9: "Q5_1",
		10: "Q2_K", 11: "Q3_K_S", 12: "Q3_K_M", 13: "Q3_K_L", 14: "Q4_K_S", 15: "Q4_K_M",
		16: "Q5_K_S", 17: "Q5_K_M", 18: "Q6_K", 19: "IQ2_XXS", 20: "IQ2_XS", 21: "IQ3_XXS",
		22: "IQ1_S", 23: "IQ4_NL", 24: "IQ3_S", 25: "IQ2_S", 26: "IQ4_XS", 27: "IQ1_M",
		28: "BF16", 29: "IQ4_NL_4_4", 30: "Q4_0_4_4", 31: "Q4_0_8_8", 32: "TQ1_0", 33: "TQ2_0",
		34: "IQ4_1", 35: "MXFP4_MOE",
	}
	if n, ok := names[t]; ok {
		return n
	}
	return fmt.Sprintf("ftype%d", t)
}
