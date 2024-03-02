package cfg

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

var (
	listener net.Listener
	err      error
	server   http.Server
	graceful = flag.Bool("g", false, "listen on fd open 3 (internal use only)")
)

type MyHandler struct {
}

func (m *MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Println("request start at ", time.Now(), r.URL.Path+"?"+r.URL.RawQuery,
		"request done at pid:", os.Getpid())
	time.Sleep(10 * time.Second)
	_, _ = w.Write([]byte("this is test reponse"))

	fmt.Println("request done at ", time.Now(), " pid:", os.Getpid())
}

func (m MyHandler) StartApp() {
	flag.Parse()
	fmt.Println("start-up at ", time.Now(), *graceful)
	if *graceful {
		var fd = os.NewFile(3, "")
		listener, err = net.FileListener(fd)
		fmt.Printf("graceful-reborn %v %v %#v \n", fd.Fd(), fd.Name(), listener)
	} else {
		listener, err = net.Listen("tcp", ":8080")
		var tcp, ok = listener.(*net.TCPListener)
		if !ok {
			fmt.Println("listener.(*net.TCPListener) error", ok)
			return
		}
		var fd, err2 = tcp.File()
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		fmt.Printf("first-boot %v %v %#v \n", fd.Fd(), fd.Name(), listener)
	}

	var server = http.Server{
		Handler:     &m,
		ReadTimeout: 5 * time.Second,
	}

	log.Printf("Actual pid is %d\n", syscall.Getpid())
	if err != nil {
		println(err)
	}

	log.Printf("listener: %v\n", listener)
	go func() {
		// 不能阻塞主线程
		if err := server.Serve(listener); err != nil {
			log.Println(err)
		}
	}()

	m.signals()
}

func (m MyHandler) signals() {
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGTERM)
	go func() {
		<-time.After(time.Second * 15)
		fmt.Println("主动重启程序...")
		ch <- syscall.SIGTERM
	}()
	for {
		var sig = <-ch
		log.Printf("signal: %v", sig)
		var ctx, _ = context.WithTimeout(context.Background(), 20*time.Second)

		switch sig {
		case syscall.SIGTERM, syscall.SIGHUP:
			println("signal cause reloading")
			signal.Stop(ch)

			{ //fork new child process
				var tl, ok = listener.(*net.TCPListener)
				if !ok {
					fmt.Println("listener is not tcp listener")
					return
				}

				var currFD, err = tl.File()
				if err != nil {
					fmt.Println("acquiring listener file failed")
					return
				}

				var cmd = exec.Command(os.Args[0], "-g")
				cmd.ExtraFiles, cmd.Stdout, cmd.Stderr = []*os.File{currFD}, os.Stdout, os.Stderr
				if err = cmd.Start(); err != nil {
					fmt.Println("cmd.Start fail: ", err)
					return
				}

				fmt.Println("forked new pid: ", cmd.Process.Pid)
			}

			_ = server.Shutdown(ctx)
			fmt.Println("graceful shutdown at ", time.Now())
		}
	}
}
