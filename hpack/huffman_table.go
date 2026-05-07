// Code derived from RFC 7541 Appendix B; values are normative.
// DO NOT EDIT without consulting RFC 7541. The 4-bit FSM lookup
// (hpack/huffman_fsm.go) is built at init time from this table.
//
//go:generate echo "huffman_table.go is hand-derived from RFC 7541 Appendix B; no generator script."
package hpack

type huffmanCode struct {
	code  uint32
	nbits uint8
}

// huffmanCodes holds 256 byte symbols (0..255) and the EOS at index 256.
var huffmanCodes = [257]huffmanCode{
	{0x1ff8, 13},      // sym 0
	{0x7fffd8, 23},    // sym 1
	{0xfffffe2, 28},   // sym 2
	{0xfffffe3, 28},   // sym 3
	{0xfffffe4, 28},   // sym 4
	{0xfffffe5, 28},   // sym 5
	{0xfffffe6, 28},   // sym 6
	{0xfffffe7, 28},   // sym 7
	{0xfffffe8, 28},   // sym 8
	{0xffffea, 24},    // sym 9
	{0x3ffffffc, 30},  // sym 10
	{0xfffffe9, 28},   // sym 11
	{0xfffffea, 28},   // sym 12
	{0x3ffffffd, 30},  // sym 13
	{0xfffffeb, 28},   // sym 14
	{0xfffffec, 28},   // sym 15
	{0xfffffed, 28},   // sym 16
	{0xfffffee, 28},   // sym 17
	{0xfffffef, 28},   // sym 18
	{0xffffff0, 28},   // sym 19
	{0xffffff1, 28},   // sym 20
	{0xffffff2, 28},   // sym 21
	{0x3ffffffe, 30},  // sym 22
	{0xffffff3, 28},   // sym 23
	{0xffffff4, 28},   // sym 24
	{0xffffff5, 28},   // sym 25
	{0xffffff6, 28},   // sym 26
	{0xffffff7, 28},   // sym 27
	{0xffffff8, 28},   // sym 28
	{0xffffff9, 28},   // sym 29
	{0xffffffa, 28},   // sym 30
	{0xffffffb, 28},   // sym 31
	{0x14, 6},         // sym 32 ' '
	{0x3f8, 10},       // sym 33 '!'
	{0x3f9, 10},       // sym 34 '"'
	{0xffa, 12},       // sym 35 '#'
	{0x1ff9, 13},      // sym 36 '$'
	{0x15, 6},         // sym 37 '%'
	{0xf8, 8},         // sym 38 '&'
	{0x7fa, 11},       // sym 39 '\''
	{0x3fa, 10},       // sym 40 '('
	{0x3fb, 10},       // sym 41 ')'
	{0xf9, 8},         // sym 42 '*'
	{0x7fb, 11},       // sym 43 '+'
	{0xfa, 8},         // sym 44 ','
	{0x16, 6},         // sym 45 '-'
	{0x17, 6},         // sym 46 '.'
	{0x18, 6},         // sym 47 '/'
	{0x0, 5},          // sym 48 '0'
	{0x1, 5},          // sym 49 '1'
	{0x2, 5},          // sym 50 '2'
	{0x19, 6},         // sym 51 '3'
	{0x1a, 6},         // sym 52 '4'
	{0x1b, 6},         // sym 53 '5'
	{0x1c, 6},         // sym 54 '6'
	{0x1d, 6},         // sym 55 '7'
	{0x1e, 6},         // sym 56 '8'
	{0x1f, 6},         // sym 57 '9'
	{0x5c, 7},         // sym 58 ':'
	{0xfb, 8},         // sym 59 ';'
	{0x7ffc, 15},      // sym 60 '<'
	{0x20, 6},         // sym 61 '='
	{0xffb, 12},       // sym 62 '>'
	{0x3fc, 10},       // sym 63 '?'
	{0x1ffa, 13},      // sym 64 '@'
	{0x21, 6},         // sym 65 'A'
	{0x5d, 7},         // sym 66 'B'
	{0x5e, 7},         // sym 67 'C'
	{0x5f, 7},         // sym 68 'D'
	{0x60, 7},         // sym 69 'E'
	{0x61, 7},         // sym 70 'F'
	{0x62, 7},         // sym 71 'G'
	{0x63, 7},         // sym 72 'H'
	{0x64, 7},         // sym 73 'I'
	{0x65, 7},         // sym 74 'J'
	{0x66, 7},         // sym 75 'K'
	{0x67, 7},         // sym 76 'L'
	{0x68, 7},         // sym 77 'M'
	{0x69, 7},         // sym 78 'N'
	{0x6a, 7},         // sym 79 'O'
	{0x6b, 7},         // sym 80 'P'
	{0x6c, 7},         // sym 81 'Q'
	{0x6d, 7},         // sym 82 'R'
	{0x6e, 7},         // sym 83 'S'
	{0x6f, 7},         // sym 84 'T'
	{0x70, 7},         // sym 85 'U'
	{0x71, 7},         // sym 86 'V'
	{0x72, 7},         // sym 87 'W'
	{0xfc, 8},         // sym 88 'X'
	{0x73, 7},         // sym 89 'Y'
	{0xfd, 8},         // sym 90 'Z'
	{0x1ffb, 13},      // sym 91 '['
	{0x7fff0, 19},     // sym 92 '\\'
	{0x1ffc, 13},      // sym 93 ']'
	{0x3ffc, 14},      // sym 94 '^'
	{0x22, 6},         // sym 95 '_'
	{0x7ffd, 15},      // sym 96 '`'
	{0x3, 5},          // sym 97 'a'
	{0x23, 6},         // sym 98 'b'
	{0x4, 5},          // sym 99 'c'
	{0x24, 6},         // sym 100 'd'
	{0x5, 5},          // sym 101 'e'
	{0x25, 6},         // sym 102 'f'
	{0x26, 6},         // sym 103 'g'
	{0x27, 6},         // sym 104 'h'
	{0x6, 5},          // sym 105 'i'
	{0x74, 7},         // sym 106 'j'
	{0x75, 7},         // sym 107 'k'
	{0x28, 6},         // sym 108 'l'
	{0x29, 6},         // sym 109 'm'
	{0x2a, 6},         // sym 110 'n'
	{0x7, 5},          // sym 111 'o'
	{0x2b, 6},         // sym 112 'p'
	{0x76, 7},         // sym 113 'q'
	{0x2c, 6},         // sym 114 'r'
	{0x8, 5},          // sym 115 's'
	{0x9, 5},          // sym 116 't'
	{0x2d, 6},         // sym 117 'u'
	{0x77, 7},         // sym 118 'v'
	{0x78, 7},         // sym 119 'w'
	{0x79, 7},         // sym 120 'x'
	{0x7a, 7},         // sym 121 'y'
	{0x7b, 7},         // sym 122 'z'
	{0x7ffe, 15},      // sym 123 '{'
	{0x7fc, 11},       // sym 124 '|'
	{0x3ffd, 14},      // sym 125 '}'
	{0x1ffd, 13},      // sym 126 '~'
	{0xffffffc, 28},   // sym 127
	{0xfffe6, 20},     // sym 128
	{0x3fffd2, 22},    // sym 129
	{0xfffe7, 20},     // sym 130
	{0xfffe8, 20},     // sym 131
	{0x3fffd3, 22},    // sym 132
	{0x3fffd4, 22},    // sym 133
	{0x3fffd5, 22},    // sym 134
	{0x7fffd9, 23},    // sym 135
	{0x3fffd6, 22},    // sym 136
	{0x7fffda, 23},    // sym 137
	{0x7fffdb, 23},    // sym 138
	{0x7fffdc, 23},    // sym 139
	{0x7fffdd, 23},    // sym 140
	{0x7fffde, 23},    // sym 141
	{0xffffeb, 24},    // sym 142
	{0x7fffdf, 23},    // sym 143
	{0xffffec, 24},    // sym 144
	{0xffffed, 24},    // sym 145
	{0x3fffd7, 22},    // sym 146
	{0x7fffe0, 23},    // sym 147
	{0xffffee, 24},    // sym 148
	{0x7fffe1, 23},    // sym 149
	{0x7fffe2, 23},    // sym 150
	{0x7fffe3, 23},    // sym 151
	{0x7fffe4, 23},    // sym 152
	{0x1fffdc, 21},    // sym 153
	{0x3fffd8, 22},    // sym 154
	{0x7fffe5, 23},    // sym 155
	{0x3fffd9, 22},    // sym 156
	{0x7fffe6, 23},    // sym 157
	{0x7fffe7, 23},    // sym 158
	{0xffffef, 24},    // sym 159
	{0x3fffda, 22},    // sym 160
	{0x1fffdd, 21},    // sym 161
	{0xfffe9, 20},     // sym 162
	{0x3fffdb, 22},    // sym 163
	{0x3fffdc, 22},    // sym 164
	{0x7fffe8, 23},    // sym 165
	{0x7fffe9, 23},    // sym 166
	{0x1fffde, 21},    // sym 167
	{0x7fffea, 23},    // sym 168
	{0x3fffdd, 22},    // sym 169
	{0x3fffde, 22},    // sym 170
	{0xfffff0, 24},    // sym 171
	{0x1fffdf, 21},    // sym 172
	{0x3fffdf, 22},    // sym 173
	{0x7fffeb, 23},    // sym 174
	{0x7fffec, 23},    // sym 175
	{0x1fffe0, 21},    // sym 176
	{0x1fffe1, 21},    // sym 177
	{0x3fffe0, 22},    // sym 178
	{0x1fffe2, 21},    // sym 179
	{0x7fffed, 23},    // sym 180
	{0x3fffe1, 22},    // sym 181
	{0x7fffee, 23},    // sym 182
	{0x7fffef, 23},    // sym 183
	{0xfffea, 20},     // sym 184
	{0x3fffe2, 22},    // sym 185
	{0x3fffe3, 22},    // sym 186
	{0x3fffe4, 22},    // sym 187
	{0x7ffff0, 23},    // sym 188
	{0x3fffe5, 22},    // sym 189
	{0x3fffe6, 22},    // sym 190
	{0x7ffff1, 23},    // sym 191
	{0x3ffffe0, 26},   // sym 192
	{0x3ffffe1, 26},   // sym 193
	{0xfffeb, 20},     // sym 194
	{0x7fff1, 19},     // sym 195
	{0x3fffe7, 22},    // sym 196
	{0x7ffff2, 23},    // sym 197
	{0x3fffe8, 22},    // sym 198
	{0x1ffffec, 25},   // sym 199
	{0x3ffffe2, 26},   // sym 200
	{0x3ffffe3, 26},   // sym 201
	{0x3ffffe4, 26},   // sym 202
	{0x7ffffde, 27},   // sym 203
	{0x7ffffdf, 27},   // sym 204
	{0x3ffffe5, 26},   // sym 205
	{0xfffff1, 24},    // sym 206
	{0x1ffffed, 25},   // sym 207
	{0x7fff2, 19},     // sym 208
	{0x1fffe3, 21},    // sym 209
	{0x3ffffe6, 26},   // sym 210
	{0x7ffffe0, 27},   // sym 211
	{0x7ffffe1, 27},   // sym 212
	{0x3ffffe7, 26},   // sym 213
	{0x7ffffe2, 27},   // sym 214
	{0xfffff2, 24},    // sym 215
	{0x1fffe4, 21},    // sym 216
	{0x1fffe5, 21},    // sym 217
	{0x3ffffe8, 26},   // sym 218
	{0x3ffffe9, 26},   // sym 219
	{0xffffffd, 28},   // sym 220
	{0x7ffffe3, 27},   // sym 221
	{0x7ffffe4, 27},   // sym 222
	{0x7ffffe5, 27},   // sym 223
	{0xfffec, 20},     // sym 224
	{0xfffff3, 24},    // sym 225
	{0xfffed, 20},     // sym 226
	{0x1fffe6, 21},    // sym 227
	{0x3fffe9, 22},    // sym 228
	{0x1fffe7, 21},    // sym 229
	{0x1fffe8, 21},    // sym 230
	{0x7ffff3, 23},    // sym 231
	{0x3fffea, 22},    // sym 232
	{0x3fffeb, 22},    // sym 233
	{0x1ffffee, 25},   // sym 234
	{0x1ffffef, 25},   // sym 235
	{0xfffff4, 24},    // sym 236
	{0xfffff5, 24},    // sym 237
	{0x3ffffea, 26},   // sym 238
	{0x7ffff4, 23},    // sym 239
	{0x3ffffeb, 26},   // sym 240
	{0x7ffffe6, 27},   // sym 241
	{0x3ffffec, 26},   // sym 242
	{0x3ffffed, 26},   // sym 243
	{0x7ffffe7, 27},   // sym 244
	{0x7ffffe8, 27},   // sym 245
	{0x7ffffe9, 27},   // sym 246
	{0x7ffffea, 27},   // sym 247
	{0x7ffffeb, 27},   // sym 248
	{0xfffffe, 28},    // sym 249
	{0x7ffffec, 27},   // sym 250
	{0x7ffffed, 27},   // sym 251
	{0x7ffffee, 27},   // sym 252
	{0x7ffffef, 27},   // sym 253
	{0x7fffff0, 27},   // sym 254
	{0x3ffffee, 26},   // sym 255
	{0x3fffffff, 30},  // sym 256 (EOS)
}
