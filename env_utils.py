"""Utilitat per carregar variables d'entorn des de .env."""
import os


def load_local_env(env_file: str = ".env"):
    """Carrega variables d'entorn des d'un fitxer .env si existeix."""
    # Buscar .env al directori del projecte NormaCat
    base = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base, env_file)
    if not os.path.isfile(env_path):
        return

    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
