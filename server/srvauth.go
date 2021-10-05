/*
Copyright (c) 2021 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package server

import (
	"net"
	"io"
	//"context"
	"golang.org/x/net/proxy"
	"github.com/maxymania/synapse/c2s"
	"github.com/maxymania/synapse/p2p"
	"github.com/maxymania/synapse/globals"
	"github.com/maxymania/synapse/proto"
	"bytes"
	"sync"
	"crypto/rand"
)

type partmap map[string][]byte
type memoizer_i [2]partmap

var MaxCacheEntries int = 1<<15

type memoizer struct {
	sync.RWMutex
	inner memoizer_i
	c uint
}
func (m memoizer) checkOK(dom string,pub []byte) bool {
	m.RLock(); defer m.RUnlock()
	opub,ok := m.inner[m.c][dom]
	if !ok { opub,ok = m.inner[m.c^1][dom] }
	if !ok { return false }
	return bytes.Equal(opub,pub)
}
func (m memoizer) approve(dom string,pub []byte) {
	m.Lock(); defer m.Unlock()
	ap := m.inner[m.c^1]
	if ap!=nil { delete(ap,dom) }
	ap = m.inner[m.c]
	if ap==nil { ap = make(partmap); m.inner[m.c] = ap }
	ap[dom] = append([]byte(nil),pub...)
	if len(ap) >= MaxCacheEntries {
		m.c ^= 1
		m.inner[m.c] = nil
	}
}

type token struct {
	stat c2s.Status
	dom  string
}
var _ c2s.Srv_Token = (*token)(nil)

func (t *token) Status() c2s.Status { return t.stat }
func (t *token) Domain() string { return t.dom }
func (t *token) done() {
	if t.stat==c2s.Pending {
		t.stat = c2s.Rejected
	}
}

type okToken string

func (t okToken) Status() c2s.Status { return c2s.Accepted }
func (t okToken) Domain() string { return string(t) }
var _ c2s.Srv_Token = okToken("")

type notarget int
func (notarget) Create(t p2p.Token, p p2p.Path) (io.WriteCloser,error) {
	return nil,p2p.EDlRejected
}

var p2pcc = &p2p.ClientContext{Target:notarget(0)}

type PeerConnectAuth struct{
	Rand   io.Reader
	Dialer proxy.Dialer
	mem memoizer
}
var _ c2s.Srv_Auth = (*PeerConnectAuth)(nil)

func verifyPeer(p *PeerConnectAuth, t *token,pub []byte) {
	defer t.done()
	addr := net.JoinHostPort(t.dom,globals.Port_p2p)
	conn,err := p.Dialer.Dial("tcp",addr)
	if err!=nil { return }
	cli,err := p2pcc.NewClient(conn)
	if err!=nil { return }
	defer cli.Close()
	myrand := p.Rand
	if myrand==nil { myrand = rand.Reader }
	sa := &proto.ServerAuth{Rand:myrand}
	ok,err := cli.AuthStep2(sa,pub,t.dom)
	if err!=nil || !ok { return }
	t.stat = c2s.Accepted
	
	// finally, memoize result to prevent further queries.
	p.mem.approve(t.dom,pub)
}
func (p *PeerConnectAuth) Login(pub []byte, domain string) c2s.Srv_Token {
	if p.mem.checkOK(domain,pub) { return okToken(domain) } // shortcut!
	t := &token{c2s.Pending,domain}
	go verifyPeer(p,t,pub)
	return t
}


