package bplustree

import "slices"

type Node struct {
	keys []string

	// For Internal Nodes
	// len(keys) = 3
	// len(children) = 4
	// Keys:     [      "p0"      "p1"      "p2"      ]
	// Children:  child0    child1    child2    child3
	// child0 = "" <= xxx < "p0"
	// child1 = "p0" <= xxx < "p1"
	// child2 = "p1" <= xxx < "p2"
	// child3 = "p2" <= xxx
	children []*Node
	parent   *Node

	// For Leaf Nodes
	isLeaf bool
	vals   []string
	next   *Node
}

func (n *Node) Get(key string) (string, bool) {
	if n.isLeaf {
		// try searching the node in its keys
		for i, k := range n.keys {
			if k == key {
				return n.vals[i], true
			}
		}
	} else {
		// try searching in its children
		for i, childNode := range n.children {
			lessKey := ""
			if i != 0 {
				lessKey = n.keys[i-1]
			}
			greaterKey := ""
			if i != len(n.children)-1 {
				greaterKey = n.keys[i]
			}

			if i == len(n.children)-1 {
				// Last Child so no upperbound
				if lessKey <= key {
					return childNode.Get(key)
				}
			} else {
				// Both LowerBound and UpperBound is present
				if lessKey <= key && key < greaterKey {
					return childNode.Get(key)
				}
			}
		}
	}

	return "", false
}

func (n *Node) findLeaf(key string) *Node {
	// Found the leaf
	if n.isLeaf {
		return n
	}

	// Find the correct child by traversing
	// try searching in its children
	for i, childNode := range n.children {
		lessKey := ""
		if i != 0 {
			lessKey = n.keys[i-1]
		}
		greaterKey := ""
		if i != len(n.children)-1 {
			greaterKey = n.keys[i]
		}

		if i == len(n.children)-1 {
			// Last Child so no upperbound
			if lessKey <= key {
				return childNode.findLeaf(key)
			}
		} else {
			// Both LowerBound and UpperBound is present
			if lessKey <= key && key < greaterKey {
				return childNode.findLeaf(key)
			}
		}
	}

	return nil
}

type BPlusTree struct {
	// All the leaf nodes lies at the same level
	root *Node

	// Degree is the maximum number of keys
	// so each node (except root) should have
	// no of keys in range of [ceil(d/2), d]
	// For Internal Nodes, they will have len(keys)+1 children
	degree int
}

func NewBPlusTree(degree int) *BPlusTree {
	root := &Node{
		keys:   []string{},
		vals:   []string{},
		isLeaf: true,
	}

	return &BPlusTree{
		root:   root,
		degree: degree,
	}
}

func (t *BPlusTree) Get(key string) (string, bool) {
	return t.root.Get(key)
}

func (t *BPlusTree) Delete(key string) bool {
	leaf := t.root.findLeaf(key)

	keyIndex := -1
	for i, k := range leaf.keys {
		if k == key {
			keyIndex = i
			break
		}
	}

	if keyIndex == -1 {
		return false
	}

	leaf.keys = append(leaf.keys[:keyIndex], leaf.keys[keyIndex+1:]...)
	leaf.vals = append(leaf.vals[:keyIndex], leaf.vals[keyIndex+1:]...)

	return true
}

func (t *BPlusTree) Put(key string, val string) {
	// Find the correct leaf node and insert the key/val
	leaf := t.root.findLeaf(key)

	// Loop through existing keys to insert the keys
	for i, k := range leaf.keys {
		if k == key {
			// Key already exists!
			leaf.vals[i] = val
			return
		}

		// Initial leaf:
		//  Keys     =  ["pA", "pB", "pD"]
		//  newKey   =  pC (at index 2)
		//  New Keys = ["pA", "pB", "pC", "pD"]
		if key < k {
			leaf.keys = slices.Insert(leaf.keys, i, key)
			leaf.vals = slices.Insert(leaf.vals, i, val)
			if len(leaf.keys) > t.degree {
				t.splitLeaf(leaf)
			}
			return
		}
	}

	// Key is larger than all the existing keys
	// so insert at the end
	leaf.keys = append(leaf.keys, key)
	leaf.vals = append(leaf.vals, val)
	if len(leaf.keys) > t.degree {
		t.splitLeaf(leaf)
	}
	return
}

func (t *BPlusTree) splitLeaf(leaf *Node) {
	midpoint := len(leaf.keys) / 2

	leftNode := &Node{
		keys:   leaf.keys[:midpoint],
		vals:   leaf.vals[:midpoint],
		isLeaf: true,
	}
	rightNode := &Node{
		keys:   leaf.keys[midpoint:],
		vals:   leaf.vals[midpoint:],
		isLeaf: true,
	}
	leftNode.next = rightNode
	promoteKey := rightNode.keys[0]

	if leaf.parent == nil {
		// This is root node
		newRoot := &Node{
			keys:     []string{promoteKey},
			children: []*Node{leftNode, rightNode},
			isLeaf:   false, // It's an internal node now!
		}
		leftNode.parent = newRoot
		rightNode.parent = newRoot
		t.root = newRoot
	} else {
		// Leaf has parent
		parent := leaf.parent
		leftNode.parent = parent
		rightNode.parent = parent

		t.insertIntoInternal(parent, promoteKey, leftNode, rightNode)
	}

}

func (t *BPlusTree) insertIntoInternal(parent *Node, key string, leftChild, rightChild *Node) {
	// Got
	// promote key -> "c"
	// leftChild = (b) <= xxx < c
	// rightChild = c <= xxx < (z)

	// Before:
	// parent.keys = ["a", "b", "z"]
	// parent.children = ["leaf0", "leaf1", "leaf2", "leaf3"]
	// leaf0 = xxx < a
	// leaf1 = a <= xxx < b
	// leaf2 = b <= xxx < z     <<------ This node is getting split
	// leaf3 = z <= xxx

	// Logic:
	// insert position we need = 2
	// find the first key greater than promote key
	// insert position would be after that

	// After:
	// parent.keys = ["a", "b", "c", "z"]
	// parent.children = ["leaf0", "leaf1", "leaf2", "leaf3", "leaf4"]
	// leaf0 = xxx < a
	// leaf1 = a <= xxx < b
	// leaf2 = b <= xxx < c     <<-------- Left Child
	// leaf3 = c <= xxx < z     <<-------- Right Child
	// leaf4 = z <= xxx

	// Find insertion position for the key
	insertPos := 0
	for i, k := range parent.keys {
		if key < k {
			break
		}
		insertPos = i + 1
	}
	// Insert the key
	parent.keys = slices.Insert(parent.keys, insertPos, key)

	// Remove old child at insertPos
	// Insert leftChild & rightChild
	parent.children = slices.Delete(parent.children, insertPos, insertPos+1)
	parent.children = slices.Insert(parent.children, insertPos, leftChild)
	parent.children = slices.Insert(parent.children, insertPos+1, rightChild)

	// Check for overflow
	if len(parent.keys) > t.degree {
		t.splitInternal(parent)
	}
}

func (t *BPlusTree) splitInternal(internal *Node) {
	// Before split:
	// internal.keys = ["b", "d", "f", "h", "j"]  // 5 keys - overflow!
	// internal.children = [c0, c1, c2, c3, c4, c5]
	// c0 = xxx < b
	// c1 = b <= xxx < d
	// c2 = d <= xxx < f
	// c3 = f <= xxx < h
	// c4 = h <= xxx < j
	// c5 = j <= xxx

	// After split (midpoint = 2):
	// Left node:
	//   keys = ["b", "d"]
	//   children = [c0, c1, c2]
	//   c0 = xxx < b
	//   c1 = b <= xxx < d
	//   c2 = d <= xxx < f

	// Promote: "f" (middle key goes up!)

	// Right node:
	//   keys = ["h", "j"]
	//   children = [c3, c4, c5]
	//   c3 = f <= xxx < h
	//   c4 = h <= xxx < j
	//   c5 = j <= xxx

	midpoint := len(internal.keys) / 2

	promoteKey := internal.keys[midpoint]

	leftNode := &Node{
		keys:     internal.keys[:midpoint],
		children: internal.children[:midpoint+1],
		isLeaf:   false,
	}
	rightNode := &Node{
		keys:     internal.keys[midpoint+1:],
		children: internal.children[midpoint+1:],
		isLeaf:   false,
	}

	for _, child := range leftNode.children {
		child.parent = leftNode
	}
	for _, child := range rightNode.children {
		child.parent = rightNode
	}

	if internal.parent == nil {
		// Create new root
		newRoot := &Node{
			keys:     []string{promoteKey},
			children: []*Node{leftNode, rightNode},
			isLeaf:   false,
		}
		leftNode.parent = newRoot
		rightNode.parent = newRoot
		t.root = newRoot
	} else {
		// Insert into parent
		parent := internal.parent
		leftNode.parent = parent
		rightNode.parent = parent
		t.insertIntoInternal(parent, promoteKey, leftNode, rightNode)
	}
}
