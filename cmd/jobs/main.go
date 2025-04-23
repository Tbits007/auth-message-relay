package jobs

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Tbits007/auth-message-relay/internal/config"
	"github.com/Tbits007/auth-message-relay/internal/cron"
	"github.com/Tbits007/auth-message-relay/internal/lib/producer"
	"github.com/Tbits007/auth-message-relay/internal/storage/postgres/eventRepo"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.MustLoad()
	
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Postgres.User,
        cfg.Postgres.Password,
        cfg.Postgres.Host,
        cfg.Postgres.Port,
        cfg.Postgres.DBName,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, connString) 
	if err != nil {
		log.Fatalf("failed to initialize db %v", err)
	}

	repo := eventRepo.NewEventRepo(db)
	producer := producer.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)

	scheduler := cron.NewCron(repo, producer)
	scheduler.Run(ctx)
}
