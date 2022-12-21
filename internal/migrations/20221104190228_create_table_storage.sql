-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
-- +goose StatementEnd
CREATE TABLE IF NOT EXISTS storage (
                         id SERIAL PRIMARY KEY,
                         full_url text,
                         user_id text NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS  index_name ON public.storage USING btree (full_url);
-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
DROP TABLE IF EXISTS storage;