CREATE TABLE notification (
    notification_id UUID PRIMARY KEY,
    message VARCHAR(300),
    recipient VARCHAR(40),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);