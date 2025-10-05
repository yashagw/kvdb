package bplustree

import (
	"fmt"
	"testing"

	"github.com/alecthomas/assert"
)

func TestBasicTree(t *testing.T) {
	tree := NewBPlusTree(4)
	tree.Put("dog", "v11")
	tree.Put("cat", "v21")
	tree.Put("zebra", "v31")

	v, ok := tree.Get("dog")
	assert.True(t, ok)
	assert.Equal(t, v, "v11")

	v, ok = tree.Get("cat")
	assert.True(t, ok)
	assert.Equal(t, v, "v21")

	v, ok = tree.Get("zebra")
	assert.True(t, ok)
	assert.Equal(t, v, "v31")

	v, ok = tree.Get("random")
	assert.False(t, ok)

	tree.Put("lion", "v41")
	v, ok = tree.Get("lion")
	assert.True(t, ok)
	assert.Equal(t, v, "v41")

	tree.Put("cat", "v22")
	v, ok = tree.Get("cat")
	assert.True(t, ok)
	assert.Equal(t, v, "v22")

	ok = tree.Delete("dog")
	assert.True(t, ok)

	_, ok = tree.Get("dog")
	assert.False(t, ok)
}

func TestSplitting(t *testing.T) {
	tree := NewBPlusTree(3)
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	for i, key := range keys {
		tree.Put(key, fmt.Sprintf("v%d", i+1))
	}

	for i, key := range keys {
		val, ok := tree.Get(key)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("v%d", i+1), val)
	}

	_, ok := tree.Get("x")
	assert.False(t, ok)

	ok = tree.Delete("g")
	assert.True(t, ok, "Delete should succeed")
	_, ok = tree.Get("g")
	assert.False(t, ok, "dog should be deleted")

	for i, key := range keys {
		if key == "g" {
			continue
		}

		val, ok := tree.Get(key)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("v%d", i+1), val)
	}
}
