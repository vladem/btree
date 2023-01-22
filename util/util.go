package util

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/vladem/btree/storage"
)

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

func PrintStats(strg storage.INodeStorage) {
	fmt.Printf("write, calls: [%v], bytes: [%v]\nread, calls: [%v], bytes: [%v]\n", strg.Statistics().WriteCalls, strg.Statistics().BytesWritten, strg.Statistics().ReadCalls, strg.Statistics().BytesRead)
}

func PrintNode(node storage.INode) {
	keys := ""
	values := ""
	children := ""
	for i := 0; i < node.KeyCount(); i++ {
		keys += string(node.Key(i)) + ", "
		if node.IsLeaf() {
			values += string(node.Value(i)) + ", "
		} else {
			children += fmt.Sprintf("%d", node.Child(i)) + ", "
		}
	}
	if !node.IsLeaf() {
		children += fmt.Sprintf("%d", node.Child(node.KeyCount())) + ", "
	}
	fmt.Printf("Node [%v]:\n"+
		"leaf: [%v],\n"+
		"keys: [%v],\n"+
		"values: [%v],\n"+
		"children: [%v]\n\n",
		node.Id(),
		node.IsLeaf(),
		keys,
		values,
		children,
	)
}

func PrintTree(strg storage.INodeStorage) error {
	seen := make(map[uint32]struct{})
	queue := []uint32{strg.RootNode().Id()}
	for len(queue) > 0 {
		curId := queue[0]
		queue = queue[1:]
		if curId == storage.InvalidNodeId {
			continue
		}
		if _, found := seen[curId]; found {
			return fmt.Errorf("met [%v] twice", curId)
		}
		seen[curId] = struct{}{}
		node, err := strg.LoadNode(curId)
		if err != nil {
			return err
		}
		if !node.IsLeaf() {
			queue = append(queue, node.Children(0, node.KeyCount()+1)...)
		}
		PrintNode(node)
	}
	return nil
}

func ReverseSliceBytes(s [][]byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func ShuffleSliceBytes(s [][]byte) {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	r.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
}

func TimeBasedFileName() string {
	return time.Now().Format("2006-01-02 15:04:05.99999999")
}
