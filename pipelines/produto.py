import logging
import sys

import pandas as pd

from .base_classes import ProdutoPipeline
from .pipeline_factory import create_main_pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ProdutoInchurchPipeline(ProdutoPipeline):
    """Inchurch-specific produto pipeline."""

    def executar(self, max_paginas: int):
        """
        Orquestra a execução do pipeline de produtos.
        """
        logger.info("🚀 Iniciando pipeline de produtos")

        parametros = {
            "apenasColunasPrincipais": "1",
            "comDadosDoGrupo": "1",
            "status": "2",
            "itensPorPagina": "200"
        }

        dados_brutos = self.extrair_dados_endpoint("clientes", max_paginas=max_paginas, **parametros)
        if not dados_brutos:
            return True

        df = self._processar_produto_detalhado(dados_brutos)
        df = self._tratar_tipos_dados_produto(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        df = self._otimizar_memoria(df)
        return self.carregar_para_postgres(
            df=df,
            tabela="splgc-produto",
            modo='replace',
            chave_unica='id_sacado_sac',
            script_rodado="Pipeline de produtos Inchurch"
        )


main_produto = create_main_pipeline(
    ProdutoInchurchPipeline,
    class_name="produto",
    max_paginas=150,
    system="inchurch"
)
