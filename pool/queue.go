package pool

// jobItem wraps a submitted Job as it waits in the queue.
type jobItem[T any] struct {
	job   Job[T]
	seq   int64
	index int
}

// jobQueue is a container/heap.Interface implementation that orders items
// by descending priority, breaking ties by submission order (seq).
type jobQueue[T any] []*jobItem[T]

func (q jobQueue[T]) Len() int { return len(q) }

func (q jobQueue[T]) Less(i, j int) bool {
	if q[i].job.Priority != q[j].job.Priority {
		return q[i].job.Priority > q[j].job.Priority
	}
	return q[i].seq < q[j].seq
}

func (q jobQueue[T]) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *jobQueue[T]) Push(x any) {
	item := x.(*jobItem[T])
	item.index = len(*q)
	*q = append(*q, item)
}

func (q *jobQueue[T]) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*q = old[:n-1]
	return item
}
