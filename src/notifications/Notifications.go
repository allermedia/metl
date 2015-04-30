// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package command provides runnable commands for the cli interface.
// Command unlock provides unlocking options for hanging jobs.
package notifications

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/tbruyelle/hipchat-go/hipchat"
	"time"
)

type Notifier interface {
	Notify(Message)
}

type Message struct {
	Jobname   string
	Status    string
	TimeTaken time.Duration
	Rows      uint
	Accepted  uint
	Rejected  uint
}

func (m Message) String() string {
	return fmt.Sprintf("%s: processed %d rows; accepted %d and rejected %d in %s", m.Jobname, m.Rows, m.Accepted, m.Rejected, m.TimeTaken)
}

type HipChat struct {
	Token string
	Room  string
}

func (h *HipChat) Notify(msg Message) {
	c := hipchat.NewClient(h.Token)

	// https://www.hipchat.com/docs/apiv2/method/send_room_notification
	nr := &hipchat.NotificationRequest{
		Message: msg.String(),
		// Update info here based on what type of notify message we have (status)
		Notify: false, // Send desktop notification
		Color:  "green",
	}

	_, err := c.Room.Notification(h.Room, nr)
	if err != nil {
		log.WithFields(log.Fields{
			"notify": "hipchat",
		}).Warn(err)
	}
}

func (h *HipChat) String() string {
	return "HipChat"
}
