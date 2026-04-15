"""
config.tenant
~~~~~~~~~~~~~~
Shared multi-tenant setup logic.

Provides ``configurar_ambiente()`` — the single entry point for
resolving which store (tenant) the current execution targets.
Used by ``main.py``, standalone scripts, and batch jobs.
"""

import argparse
import logging
import os
import sys
from typing import Optional

logger = logging.getLogger(__name__)


def configurar_ambiente(
    descricao: str = "ETL Pipeline — Multi-Tenant",
    args_list: Optional[list[str]] = None,
) -> str:
    """Determines the target store and loads its environment file.

    Resolution order:
        1. ``--loja`` CLI argument (e.g. ``python main.py --loja prohair``)
        2. Interactive ``input()`` prompt if no CLI argument was given.

    After resolving the store name, locates the corresponding
    ``.env.<store>`` file, validates its existence, and calls
    ``settings.inicializar()`` to populate all module-level
    configuration variables.

    Args:
        descricao: Description shown in ``--help`` output.
        args_list: Explicit argument list (for testing).
                   Defaults to ``sys.argv[1:]``.

    Returns:
        Sanitized store name (e.g. 'prohair').
    """
    parser = argparse.ArgumentParser(description=descricao)
    parser.add_argument(
        "--loja",
        type=str,
        required=False,
        default=None,
        help="Nome da loja (ex: prohair, progrowth). "
             "Se omitido, sera solicitado interativamente.",
    )
    args = parser.parse_args(args_list)

    # --- Resolve store name ---
    if args.loja:
        nome_loja = args.loja
    else:
        print("=====================================================")
        print(f"  {descricao}")
        print("=====================================================\n")
        nome_loja = input("Digite o nome da loja (ex: prohair, progrowth): ")

    # --- Sanitize ---
    nome_loja = nome_loja.strip().lower()

    if not nome_loja:
        print("\n[ERRO] Nenhum nome de loja foi informado.")
        sys.exit(1)

    # --- Locate and validate .env file ---
    env_file = f".env.{nome_loja}"

    if not os.path.exists(env_file):
        print(f"\n[ERRO] Arquivo '{env_file}' nao encontrado.")
        print(f"   A loja '{nome_loja}' nao esta cadastrada.")
        print(f"   Crie o arquivo '{env_file}' na raiz do projeto com as")
        print("   credenciais da loja antes de executar o pipeline.")
        sys.exit(1)

    # --- Load environment and initialize settings ---
    from src.config.settings import inicializar
    inicializar(env_file)

    logger.info("Ambiente configurado para a loja: '%s'", nome_loja)
    return nome_loja
