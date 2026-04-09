package stats

type counter int64

func newCounter() *counter {
	c := counter(0)
	return &c
}

func (c *counter) Add(n int64) {
	*c += counter(n)
}

func (c *counter) Get() int64 {
	return int64(*c)
}

func (c *counter) Reset() {
	*c = 0
}
