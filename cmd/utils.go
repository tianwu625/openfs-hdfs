package cmd

import (
	"net"
	"errors"
)

const (
	defaultMinioPort = "9000"
	defaultMinioWebPort = "9001"
)

var errOpfsOccupation error = errors.New("openfs occupy this port")

func checkOpfsOccupy(port string) error {
	if port == defaultMinioPort ||
	   port == defaultMinioWebPort {
		return errOpfsOccupation
	}

	return nil
}

func checkAddressValid(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}

	if err := checkOpfsOccupy(port); err != nil {
		return err
	}

	return nil
}
