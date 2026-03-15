# FastAPI + DBDuck Production Example

This app is a production-style FastAPI service that uses `DBDuck` for DB interaction.

## Features

- App lifespan startup/shutdown with shared `UDOM` connection
- Health endpoint
- Orders CRUD endpoints
- Bulk insert endpoint
- Pagination (`page`, `page_size`)
- Safe DB defaults (`allow_unsafe_where_strings=false`)

## Environment Variables

- `APP_DB_TYPE` (default: `sql`)
- `APP_DB_INSTANCE` (default: `sqlite`)
- `APP_DB_URL` (default: `sqlite:///fastapi_dbduck.db`)
- `APP_LOG_LEVEL` (default: `INFO`)

## Run

```bash
uvicorn examples.fastapi_dbduck_app.main:app --host 0.0.0.0 --port 8000 --workers 2
```

## Endpoints

- `GET /health`
- `POST /orders`
- `POST /orders/bulk`
- `GET /orders?paid=true&page=1&page_size=20`
- `GET /orders/{order_id}`
- `PATCH /orders/{order_id}`
- `DELETE /orders/{order_id}`

## Quick Test

```bash
curl -X POST http://127.0.0.1:8000/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"order_id\":101,\"customer\":\"A\",\"paid\":false}"
```
