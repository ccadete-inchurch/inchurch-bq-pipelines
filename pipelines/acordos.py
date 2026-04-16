import logging
import sys

import pandas as pd

from .base_classes import AcordosPipeline
from .pipeline_factory import create_main_pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AcordosInchurchPipeline(AcordosPipeline):
    """Inchurch-specific acordos pipeline."""

    def executar(self, max_paginas: int):
        """
        Orquestra a execução do pipeline de acordos.
        """
        logger.info("🚀 Iniciando pipeline de acordos Inchurch")

        parametros = {
            "filtrarpor": "todasParcelas",
            "exibirDadosDasParcelas": "1",
            "itensPorPagina": "200"
        }

        dados_brutos = self.extrair_dados_endpoint("acordos", max_paginas=max_paginas, **parametros)
        if not dados_brutos:
            return True

        df = self._processar_acordos_detalhado(dados_brutos)
        df = self._tratar_tipos_dados_acordos(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        df = self._otimizar_memoria(df)
        return self.carregar_para_postgres(
            df=df,
            tabela="splgc-acordos",
            modo='replace'
        )


main_acordos = create_main_pipeline(
    AcordosInchurchPipeline,
    class_name="acordos",
    max_paginas=400,
    system="inchurch"
)
