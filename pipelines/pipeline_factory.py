"""
Factory for creating main_[entity]() functions.
Eliminates duplicate try-except blocks across all pipeline modules.
"""

import logging
import sys

from config import Config

logger = logging.getLogger(__name__)


def create_main_pipeline(pipeline_class, class_name: str, max_paginas: int = None, system: str = "inchurch", use_config_token: bool = True):
    """
    Factory function that creates a main_[entity]() function.

    Args:
        pipeline_class: The pipeline class to instantiate (e.g., ClientesPipeline)
        class_name: Human-readable name for logging (e.g., "clientes Inchurch")
        max_paginas: Max pages for API pagination (optional)
        system: System for token lookup - "inchurch" or "a6" (default: "inchurch")
        use_config_token: Whether to use token from Config (default: True)

    Returns:
        A main_[entity]() function
    """
    def main():
        # Get credentials from config
        access_token = Config.get_token(system) if use_config_token else None
        db_connection = Config.DB_CONNECTION_STRING

        try:
            # Instantiate pipeline
            pipeline = pipeline_class(db_connection, access_token)

            # Execute with optional max_paginas
            if max_paginas:
                sucesso = pipeline.executar(max_paginas=max_paginas)
            else:
                sucesso = pipeline.executar()

            status = "✅ Sucesso" if sucesso else "❌ Falha"
            print(f"Pipeline de {class_name}: {status}")

        except Exception as e:
            logger.error(f"💥 Erro fatal em pipeline de {class_name}: {e}")
            sys.exit(1)

    return main
