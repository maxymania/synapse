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


package onion

import (
	"io"
	"net"
	"math/rand"
	"fmt"
	"encoding/base64"
	"strings"
	"golang.org/x/net/proxy"
	
	"github.com/cretz/bine/control"
	"github.com/cretz/bine/torutil/ed25519"
	
	"github.com/maxymania/synapse/proto"
	"github.com/maxymania/synapse/globals"
)

func bindToAnyPort() (l net.Listener,port int,err error) {
	for i := 0 ; i<25 ; i++ {
		port = rand.Intn(32000)+16000
		l,err = net.Listen("tcp",fmt.Sprint(":",port))
		if err!=nil { break }
	}
	return
}
func errand(es ...error) error {
	for _,e := range es { if e!=nil { return e } }
	return nil
}

type KeySet struct {
	TorKeyType  string
	TorKeyData  string
	AuthPubKey  string
	AuthPrivKey string
}
func (kp *KeySet) Type() control.KeyType {
	return control.KeyType(kp.TorKeyType)
}
func (kp *KeySet) Blob() string {
	return kp.TorKeyData
}
func NewKeySet(crng io.Reader) (*KeySet,error) {
	kp,err := ed25519.GenerateKey(crng)
	if err!=nil { return nil,err }
	ekp := control.ED25519Key{kp}
	pub,pri,err := proto.GenKeyPair(crng)
	if err!=nil { return nil,err }
	return &KeySet{
		string(ekp.Type()),
		ekp.Blob(),
		base64.StdEncoding.EncodeToString(pub),
		base64.StdEncoding.EncodeToString(pri),
	},nil
}
func CreateDialer(ctc *control.Conn) (proxy.Dialer,error) {
	info,err := ctc.GetInfo("net/listeners/socks")
	if err!=nil { return nil,err }
	for _,ie := range info {
		if ie.Key!="net/listeners/socks" { continue }
		addr := ie.Val
		pnet := "tcp"
		if strings.HasPrefix(addr,"unix:") {
			addr = addr[5:]
			pnet = "unix"
		}
		return proxy.SOCKS5(pnet,addr,nil,nil)
	}
	return nil, fmt.Errorf("no SOCKS5 port!")
}

func BindListener(ctc *control.Conn, ks *KeySet, kp *proto.KeyPair) (net.Listener,error) {
	pub,err1 := base64.StdEncoding.DecodeString(ks.AuthPubKey)
	pri,err2 := base64.StdEncoding.DecodeString(ks.AuthPrivKey)
	err := errand(err1,err2)
	if err!=nil { return nil,err }
	
	l,p,err := bindToAnyPort()
	if err!=nil { return nil,err }
	kvp := []*control.KeyVal{{Key:globals.Port_p2p,Val:fmt.Sprint(p)}}
	req := &control.AddOnionRequest{Key:ks,Ports:kvp}
	resp,err := ctc.AddOnion(req)
	if err!=nil { l.Close(); return nil,err }
	kp.Pub, kp.Pri = pub, pri
	kp.Domain = strings.ToLower(resp.ServiceID)+".onion"
	return l,nil
}


