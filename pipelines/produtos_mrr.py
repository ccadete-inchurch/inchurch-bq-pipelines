import logging

import pandas as pd

from .base_classes import ProdutosBaseMRRPipeline
from .pipeline_factory import create_main_pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ProdutosBaseMRRInchurchPipeline(ProdutosBaseMRRPipeline):
    """Inchurch-specific produtos base MRR pipeline."""

    def executar(self, max_paginas: int):
        """
        Orquestra a execução do pipeline de acordos.
        """
        logger.info("🚀 Iniciando pipeline de produtos base MRR Inchurch")

        parametros = {
            "itensPorPagina": "200"
        }

        dados_brutos = self.extrair_dados_endpoint("produtos?", max_paginas=max_paginas, **parametros)
        if not dados_brutos:
            return True

        df = self._processar_produtos_base_mrr_detalhado(dados_brutos)
        df = self._tratar_tipos_dados_produtos_base_mrr(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        df = self._otimizar_memoria(df)
        return self.carregar_para_postgres(
            df=df,
            tabela="splgc-produtos_base_mrr",
            modo='replace',
            chave_unica='id_produto_prd',
            script_rodado="Pipeline de produtos base MRR Inchurch"
        )


main_produtos_base_mrr = create_main_pipeline(
    ProdutosBaseMRRInchurchPipeline,
    class_name="produtos MRR",
    max_paginas=3,
    system="inchurch"
)
