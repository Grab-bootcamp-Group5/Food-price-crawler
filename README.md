# Install Poetry (if missing)  ➜  https://python-poetry.org/docs/#installation
curl -sSL https://install.python-poetry.org | python3 -

export PATH="$HOME/.local/bin:$PATH"
export PATH="/root/.local/bin:$PATH"

source ~/.bashrc


# Initialise Poetry project
poetry init --name foodprice_crawler --python ">=3.11" -n


poetry run python - <<'PY'
import asyncio
from db.models import init_db
asyncio.run(init_db())
PY
