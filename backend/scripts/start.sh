#!/bin/bash
set -e

echo "=== DataApp Startup ==="
echo "PORT=${PORT:-8000}"
echo "DATABASE_URL set: $([ -n "$DATABASE_URL" ] && echo YES || echo NO)"

echo "Running init_db..."
python scripts/init_db.py

echo "Testing import..."
python -c "
import sys
sys.stdout.flush()
print('Importing main...', flush=True)
try:
    import main
    print('Import OK', flush=True)
except Exception as e:
    import traceback
    print('IMPORT ERROR:', e, flush=True)
    traceback.print_exc()
    sys.exit(1)
"

echo "Starting uvicorn on port ${PORT:-8000}..."
exec python -m uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
