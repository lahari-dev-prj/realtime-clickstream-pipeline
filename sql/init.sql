-- Sink table for windowed aggregation results written by the Spark job
CREATE TABLE IF NOT EXISTS product_activity_5min (
    window_start   TIMESTAMP NOT NULL,
    window_end     TIMESTAMP NOT NULL,
    product_id     VARCHAR(50) NOT NULL,
    event_type     VARCHAR(20) NOT NULL,
    event_count    BIGINT NOT NULL,
    unique_users   BIGINT NOT NULL,
    inserted_at    TIMESTAMP DEFAULT now(),
    PRIMARY KEY (window_start, product_id, event_type)
);

CREATE INDEX IF NOT EXISTS idx_product_activity_window
    ON product_activity_5min (window_start);
