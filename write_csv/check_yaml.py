import yaml
from pathlib import Path

path = Path("config/tablas.yaml")
with open(path, "r") as f:
    config = yaml.safe_load(f)

print(f"Archivo YAML detectado: {path.exists()}")
print(f"Tablas configuradas: {list(config['tablas'].keys())}")