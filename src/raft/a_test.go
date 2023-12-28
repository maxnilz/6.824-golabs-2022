package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestCh(t *testing.T) {
	wg := sync.WaitGroup{}

	a := make(chan struct{})
	b := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Second)
		for {
			select {
			case <-tick.C:
				fmt.Println("tick")
			case <-a:
				fmt.Println("a")
			case <-b:
				fmt.Println("b")
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			a <- struct{}{}
			fmt.Println("sent a")
			time.Sleep(time.Second)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			b <- struct{}{}
			fmt.Println("sent b")
			time.Sleep(time.Second)
		}
	}()

	wg.Wait()
}

func ffc(seq int64, cancel <-chan struct{}) {
	<-cancel
	fmt.Println("+++", seq)
}

func TestChCancel(t *testing.T) {
	tick := time.NewTicker(time.Second)

	cancel := make(chan struct{})
	var seq int64
	for {
		select {
		case <-tick.C:
			close(cancel)
			cancel = make(chan struct{})
			fmt.Println("===", seq)
			go func() {
				ffc(seq, cancel)
			}()
			seq++
			tick.Reset(time.Second)
		}
	}
}

func ff(c chan string) {
	fmt.Printf("ff %p\n", &c)
	ptr := (*string)(unsafe.Pointer(&c))
	fmt.Printf("ff data: %p\n", ptr)
	fmt.Printf("ff data 2: %p\n", &(*ptr))
}

func TestRef(t *testing.T) {
	wg := sync.WaitGroup{}
	ch := make(chan string)
	fmt.Printf("1 %p\n", &ch)
	ptr := (*string)(unsafe.Pointer(&ch))
	fmt.Printf("2: %p\n", ptr)
	fmt.Printf("3: %p\n", &(*ptr))
	ff(ch)

	wg.Add(1)
	go func(c chan string) {
		defer wg.Done()
		select {
		case <-c:
			fmt.Printf("=== %p\n", &c)
		}
	}(ch)
	time.Sleep(time.Second)
	close(ch)
	ch = make(chan string)
	fmt.Printf("2 %p\n", &ch)

	ch2 := make(chan string)
	fmt.Printf("%p\n", ch2)
	wg.Wait()
}

func TestLock(t *testing.T) {
	a := sync.Mutex{}
	a.Lock()
	a.Unlock()
	a.Unlock()
}

func TestCh1(t *testing.T) {

	var ch chan bool
	// close(ch) // panic: cannot close a nil channel

	ch = make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case val := <-ch:
				if !val {
					println("000")
					return
				}
				println("0", val)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case val := <-ch:
				if !val {
					println("111")
					return
				}
				println("1", val)
			}
		}
	}()
	ch <- true
	ch <- true
	ch <- true
	ch <- true
	close(ch)
	// close(ch) // panic: close a closed channel

	wg.Wait()
}

func blockf() int64 {
	time.Sleep(time.Second * 10)
	return 10
}

func cancelable(ans *int64, cancel chan struct{}) int64 {
	i := int64(0)
	p := &i
	go func() {
		r := blockf()
		atomic.StoreInt64(ans, r)
		atomic.StoreInt64(p, i)
		fmt.Println("blockf returns")
	}()

	for {
		n := atomic.LoadInt64(p)
		fmt.Println("n = ", n)
		select {
		case <-cancel:
			fmt.Println("cancel received")
			goto end
		default:
			time.Sleep(time.Second)
		}
	}

end:
	fmt.Println("cancelable returns")
	return atomic.LoadInt64(p)
}

func TestBlocking(t *testing.T) {
	cancel := make(chan struct{})

	var ans int64
	done := make(chan int64)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := cancelable(&ans, cancel)
		done <- n
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 3)
		fmt.Println("canceling")
		close(cancel)
	}()

	out := <-done
	fmt.Println("out = ", out)
	fmt.Println("ans = ", ans)
	time.Sleep(time.Second * 15)
	fmt.Println("ans = ", ans)
	wg.Wait()
}
