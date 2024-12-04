package main

import (
	"encoding/json"
	"fmt"
	"log"

	zmq4 "github.com/pebbe/zmq4"
)

type ZmqBrokerClient struct {
	socket *zmq4.Socket
}

func NewZmqBrokerClient() *ZmqBrokerClient {
	context, err := zmq4.NewContext()
	if err != nil {
		log.Fatal(err)
	}

	client, err := context.NewSocket(zmq4.REQ)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect("tcp://localhost:5555")
	if err != nil {
		log.Fatal(err)
	}

	return &ZmqBrokerClient{socket: client}
}

func (z *ZmqBrokerClient) DispatchEvent(event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = z.socket.Send(string(data), 0)
	if err != nil {
		return err
	}

	reply, err := z.socket.Recv(0)
	if err != nil {
		return err
	}

	fmt.Println("reply", reply)
	return nil
}
