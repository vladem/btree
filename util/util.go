package util

/*
Compares two byte arrays, returns integer:
if lhs < rhs:	-1
if lhs == rhs:	0
if lhs > rhs:	1
*/
func Compare(lhs, rhs []byte) int8 {
	lenn := len(lhs)
	if len(lhs) > len(rhs) {
		lenn = len(rhs)
	}
	idx := 0
	for idx < lenn && lhs[idx] == rhs[idx] {
		idx += 1
	}
	if idx != lenn {
		if lhs[idx] > rhs[idx] {
			return 1
		} else {
			return -1
		}
	}
	if len(lhs) > len(rhs) {
		return 1
	} else if len(lhs) < len(rhs) {
		return -1
	}
	return 0
}
