package hpack

// 4-bit FSM Huffman decoder.
//
// A 4-bit FSM table is built once at init time from the canonical
// huffmanCodes table (RFC 7541 Appendix B). Per byte of input the
// decoder performs exactly 2 table lookups (one per nibble), each
// producing 0 to 2 emitted symbols and a next-state transition. This
// is ~100x faster than the previous bit-by-bit walk that linearly
// scanned all 257 codes per attempted symbol.
//
// Layout: hufFSM[stateIdx*16 + nibble] = entry. Each entry encodes:
//   - up to 2 emitted symbols (a 4-bit nibble in the trie can produce
//     at most 4 symbols if the trie were degenerate, but for the RFC
//     7541 table the maximum is 2);
//   - the next state index;
//   - a "padOK" flag indicating that ending decoding at this state is
//     a valid EOS-prefix padding (state lies on the all-ones path
//     from root, depth ≤ 7);
//   - an "invalid" flag set when the nibble traverses an EOS symbol
//     (256) or an undefined branch.
//
// State 0 is always the root.

type hufFSMEntry struct {
	syms    [2]uint8
	nsyms   uint8
	next    uint16
	padOK   bool
	invalid bool
}

var (
	hufFSM    []hufFSMEntry
	hufStates int
)

type hufTrieNode struct {
	sym      int // -1 if not a leaf, 256 = EOS
	left     *hufTrieNode
	right    *hufTrieNode
	depth    int  // distance from root in bits
	allOnes  bool // path from root to this node is all 1-bits
}

func init() {
	buildHuffmanFSM()
}

func buildHuffmanFSM() {
	root := newHufTrie()
	states := []*hufTrieNode{root}
	idx := map[*hufTrieNode]int{root: 0}

	type entryFill struct {
		state int
		nib   int
		entry hufFSMEntry
	}
	var fills []entryFill

	for processed := 0; processed < len(states); processed++ {
		s := states[processed]
		for nib := 0; nib < 16; nib++ {
			n := s
			var entry hufFSMEntry
			for bit := 3; bit >= 0; bit-- {
				b := (nib >> bit) & 1
				if b == 0 {
					n = n.left
				} else {
					n = n.right
				}
				if n == nil {
					entry.invalid = true
					break
				}
				if n.sym >= 0 {
					if n.sym == 256 {
						entry.invalid = true
						break
					}
					if entry.nsyms < 2 {
						entry.syms[entry.nsyms] = uint8(n.sym)
						entry.nsyms++
					}
					n = root
				}
			}
			if !entry.invalid {
				ns, ok := idx[n]
				if !ok {
					ns = len(states)
					idx[n] = ns
					states = append(states, n)
				}
				entry.next = uint16(ns)
				// padOK: state lies on all-1 path AND depth ≤ 7. Root
				// (n == root) qualifies trivially (depth 0).
				if n == root {
					entry.padOK = true
				} else if n.allOnes && n.depth <= 7 {
					entry.padOK = true
				}
			}
			fills = append(fills, entryFill{state: processed, nib: nib, entry: entry})
		}
	}

	hufStates = len(states)
	hufFSM = make([]hufFSMEntry, hufStates*16)
	for _, f := range fills {
		hufFSM[f.state*16+f.nib] = f.entry
	}
	hufStatePadOK = make([]bool, hufStates)
	hufStatePadOK[0] = true // root accepts (no partial code)
	for i := 1; i < hufStates; i++ {
		s := states[i]
		hufStatePadOK[i] = s.allOnes && s.depth <= 7
	}
}

func newHufTrie() *hufTrieNode {
	root := &hufTrieNode{sym: -1, depth: 0, allOnes: true}
	for sym, c := range huffmanCodes {
		n := root
		for i := int(c.nbits) - 1; i >= 0; i-- {
			bit := (c.code >> uint(i)) & 1
			if bit == 0 {
				if n.left == nil {
					n.left = &hufTrieNode{
						sym:     -1,
						depth:   n.depth + 1,
						allOnes: false,
					}
				}
				n = n.left
			} else {
				if n.right == nil {
					n.right = &hufTrieNode{
						sym:     -1,
						depth:   n.depth + 1,
						allOnes: n.allOnes,
					}
				}
				n = n.right
			}
		}
		n.sym = sym
	}
	return root
}

// huffmanDecodeFSM is the 4-bit FSM-driven decoder.
func huffmanDecodeFSM(dst, src []byte) ([]byte, error) {
	state := uint16(0)
	for _, b := range src {
		hi := uint16(b>>4) & 0x0f
		e := hufFSM[state*16+hi]
		if e.invalid {
			return nil, ErrInvalidHuffman
		}
		for i := uint8(0); i < e.nsyms; i++ {
			dst = append(dst, e.syms[i])
		}
		state = e.next

		lo := uint16(b) & 0x0f
		e = hufFSM[state*16+lo]
		if e.invalid {
			return nil, ErrInvalidHuffman
		}
		for i := uint8(0); i < e.nsyms; i++ {
			dst = append(dst, e.syms[i])
		}
		state = e.next
	}
	// At end, state must be root (state 0) OR represent valid EOS
	// padding (on all-1 path, depth ≤ 7). The "next" reached after
	// consuming the LAST nibble is what we must check, but we've stored
	// padOK on the entry that produced this state — re-derive from
	// state index by stepping through any nibble's entry whose next
	// equals state. Simpler: track per-state padOK directly.
	if state == 0 {
		return dst, nil
	}
	// Look up padOK by checking any entry that transitions INTO this
	// state — but we don't have a reverse map. Instead, a state's
	// padOK status depends solely on the corresponding trie node, so
	// we recompute it once at build time and store it in a parallel
	// table.
	if int(state) >= len(hufStatePadOK) || !hufStatePadOK[state] {
		return nil, ErrInvalidHuffman
	}
	return dst, nil
}

// hufStatePadOK[i] is true if state i's underlying trie node lies on
// the all-1 path from root with depth ≤ 7. Populated alongside
// hufFSM in buildHuffmanFSM.
var hufStatePadOK []bool
