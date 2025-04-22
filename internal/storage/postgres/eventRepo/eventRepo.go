package eventRepo

import (
	"context"
	"fmt"

	"github.com/Tbits007/auth-message-relay/internal/domain/models/eventModel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventRepo struct {
	db *pgxpool.Pool
}

func NewEventRepo(db *pgxpool.Pool) *EventRepo {
	return &EventRepo{
		db: db,
	}
}

func (u *EventRepo) Read(
	ctx 	  context.Context,
	batchSize int,
) ([]eventModel.Event, error) {
	const op = "postgres.eventRepo.Read"

	query := `
	SELECT id, event_type, payload
	FROM outbox
	WHERE status = 'pending'
	LIMIT $1
	FOR UPDATE SKIP LOCKED
	`

	rows, err := u.db.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("%s: query error: %w", op, err)
	}	
	defer rows.Close()

	result := make([]eventModel.Event, 0, batchSize)
	for rows.Next() {
		var event eventModel.Event
		err = rows.Scan(
			&event.ID,
			&event.EventType,
			&event.Payload,
			&event.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: scan error: %w", op, err)
		}		
		result = append(result, event)	
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: rows iteration error: %w", op, err)
	}

	return result, nil 
}

func (u *EventRepo) Update(
	ctx 	  context.Context,
	ids []uuid.UUID,
) error {
	const op = "postgres.eventRepo.Update"

	query := `
	UPDATE outbox 
	SET status = 'processed'
	WHERE id = ANY($1)
	`

	_, err := u.db.Exec(ctx, query, ids)
	if err != nil {
		return fmt.Errorf("%s: exec query: %w", op, err)
	}

	return nil 
}