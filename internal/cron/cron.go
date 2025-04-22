package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Tbits007/auth-message-relay/internal/lib/producer"
	"github.com/Tbits007/auth-message-relay/internal/storage/postgres/eventRepo"
	"github.com/google/uuid"
)

type Cron struct {
	repo 	 *eventRepo.EventRepo
	producer *producer.Producer
}

func NewCron(
	repo 	 *eventRepo.EventRepo, 
	producer *producer.Producer,
) *Cron {
	return &Cron{
		repo: repo,
		producer: producer,
	}
}

func (c *Cron) Run(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil 
		case <-ticker.C:
			if err := c.processBatch(ctx); err != nil {
				return fmt.Errorf("Error processing batch: %v", err)
			}
		}
	}
}

func (c *Cron) processBatch(ctx context.Context) error {
	events, err := c.repo.Read(ctx, 20)
	if err != nil {
		return fmt.Errorf("read rows: %w", err) 
	}
	
	if len(events) == 0 {
		return nil 
	}	

	var ids []uuid.UUID
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshaling: %w", err)
		}

		if err := c.producer.SendMessage(ctx, nil, data); err != nil {
			return fmt.Errorf("sending msg's: %w", err)
		}

		ids = append(ids, event.ID)
	}

	if err := c.repo.Update(ctx, ids); err != nil {
		return fmt.Errorf("updating msg's: %w", err)
	}

	return nil
}




