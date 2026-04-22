"""
config.paths
~~~~~~~~~~~~
Centralizes all directory and file paths used in the project.
"""

from pathlib import Path

# Project root resolution
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent

# Config directories
CONFIG_DIR = PROJECT_ROOT / "config"
ENV_DIR = CONFIG_DIR / "env"
JSON_DIR = CONFIG_DIR / "json"

# Material directories
MATERIAL_DIR = PROJECT_ROOT / "material"
CUSTOS_XLSX = MATERIAL_DIR / "custo.xlsx"
CUSTOS_JSON = MATERIAL_DIR / "produtos_custo.json"
