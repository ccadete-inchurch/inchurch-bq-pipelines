import logging

import pandas as pd

from .base_classes import ClientesPipeline
from .pipeline_factory import create_main_pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClientesInchurchPipeline(ClientesPipeline):
    """Inchurch-specific clientes pipeline."""

    def executar(self, max_paginas: int):
        """
        Orquestra a execução do pipeline de clientes.
        """
        logger.info("🚀 Iniciando pipeline de clientes Inchurch")

        parametros = {
            "apenasColunasPrincipais": "1",
            "status": "2",
            "itensPorPagina": "200"
        }

        dados_brutos = self.extrair_dados_endpoint("clientes", max_paginas=max_paginas, **parametros)
        if not dados_brutos:
            return True

        df = self._processar_clientes_detalhado(dados_brutos)
        df = self._tratar_tipos_dados_clientes(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        df = self._otimizar_memoria(df)
        return self.carregar_para_postgres(
            df=df,
            tabela="splgc-clientes-inchurch",
            modo='append',
            chave_unica='id_sacado_sac',
            script_rodado="Pipeline de clientes Inchurch"
        )


main_clientes_inchurch = create_main_pipeline(
    ClientesInchurchPipeline,
    class_name="clientes Inchurch",
    max_paginas=150,
    system="inchurch"
)
