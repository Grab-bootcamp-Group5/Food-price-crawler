# Install Poetry (if missing)  ➜  https://python-poetry.org/docs/#installation
curl -sSL https://install.python-poetry.org | python3 -

export PATH="$HOME/.local/bin:$PATH"

# if on root
export PATH="/root/.local/bin:$PATH"

source ~/.bashrc


# Initialise Poetry project


poetry run python db/init_db.py

sqlite3 prices.db
.tables

poetry run playwright install


poetry run python crawler/stores/run_stores.py --domain cooponline
