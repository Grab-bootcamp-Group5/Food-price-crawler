# Install Poetry (if missing)  ➜  https://python-poetry.org/docs/#installation
curl -sSL https://install.python-poetry.org | python3 -

export PATH="$HOME/.local/bin:$PATH"
source ~/.bashrc


# Initialise Poetry project
poetry init --name foodprice_crawler --python ">=3.11" -n
