package find_first_unique

type Node struct {
	prev  *Node
	next  *Node
	value interface{}
}

type List struct {
	head *Node
	tail *Node
	size int
}

type Iterator struct {
	cur  *Node
	list *List
}

func NewIterator(l *List) *Iterator {
	return &Iterator{
		cur:  nil,
		list: l,
	}
}

func (itr *Iterator) Next() bool {
	if itr.cur == nil {
		itr.cur = itr.list.head
		if itr.cur == nil {
			return false
		}
	}
	if itr.cur.next == nil {
		return false
	}
	itr.cur = itr.cur.next
	return true
}

func (itr *Iterator) Get() *Node {
	return itr.cur
}

func NewList() *List {
	return &List{}
}

func (l *List) PushBack(value interface{}) (node *Node) {
	if l.size == 0 {
		l.head = &Node{
			prev:  nil,
			next:  nil,
			value: value,
		}
		l.tail = l.head
		l.size++
		return l.head
	}
	node = &Node{
		prev:  l.tail,
		next:  nil,
		value: value,
	}
	l.tail.next = node
	l.tail = node
	l.size++
	return
}

func (l *List) PopBack() (value interface{}, success bool) {
	if l.size == 0 {
		return
	}
	if l.size == 1 {
		value = l.tail.value
		l.tail = nil
		l.head = nil
		l.size--
		return value, true
	}
	l.tail.prev.next = nil
	value = l.tail.value
	l.tail = l.tail.prev
	l.size--
	return value, true
}

func (l *List) PopFront() (value interface{}, success bool) {
	if l.size == 0 {
		return
	}
	if l.size == 1 {
		value = l.head.value
		l.head = nil
		l.tail = nil
		l.size--
		return value, true
	}
	l.head.next.prev = nil
	value = l.head.value
	l.head = l.head.next
	l.size--
	return value, true
}

// func (l *List) DeleteByValue(value interface{}) {
// 	if l.size == 0 {
// 		return
// 	}
// 	p := l.head
// 	for {
// 		if p == nil {
// 			break
// 		}
// 		if strings.Compare(p.value, value) == 0 {
// 			if p == l.head {
// 				l.PopFront()
// 				return
// 			}
// 			if p == l.tail {
// 				l.PopBack()
// 				return
// 			}
// 			p.prev.next = p.next
// 			p.next.prev = p.prev
// 			l.size--
// 			return
// 		}
// 		p = p.next
// 	}
// }
