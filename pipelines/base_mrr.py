import logging
import sys
from datetime import datetime

import pandas as pd

from .base_classes import MrrPipeline
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MrrInchurchPipeline(MrrPipeline):
    """Inchurch-specific MRR pipeline."""

    def executar(self, max_paginas: int, dt_inicio: str):
        """
        Orquestra a execução do pipeline de cobranças.
        """
        logger.info("🚀 Iniciando pipeline de MRR")

        parametros = {
                "tipo": "contratospendentes",
                "filtrarpor": "dt_atualizacao_mens",
                "dtInicio": dt_inicio,
                "itensPorPagina": "200"
        }

        dados_brutos = self.extrair_dados_endpoint("recorrencias/recorrenciasdeplanos?", max_paginas=max_paginas, **parametros)
        if not dados_brutos:
            return True

        df = self.processar_recorrencias(dados_brutos)
        df = self._tratar_tipos_dados_mrr(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        df = self._otimizar_memoria(df)

        if dt_inicio == "01/01/2018":
            modo = 'replace'
        else:
            modo = 'append'

        return self.carregar_para_postgres(
            df=df,
            tabela="splgc-tabela_mrr",
            modo=modo,
            chave_unica='id_mensalidade_mens',
            script_rodado="Pipeline de MRR"
        )


def tabela_mrr(year: int):
    """
    Main function for MRR pipeline.
    Accepts year, converts to dt_inicio, and executes pipeline.
    """
    access_token = Config.get_token("inchurch")
    db_connection = Config.DB_CONNECTION_STRING

    dt_inicio = f"01/01/{year}"

    try:
        pipeline = MrrInchurchPipeline(db_connection, access_token)
        sucesso = pipeline.executar(max_paginas=200, dt_inicio=dt_inicio)

        status = "✅ Sucesso" if sucesso else "❌ Falha"
        print(f"Pipeline de MRR {year}: {status}")

    except Exception as e:
        logger.error(f"💥 Erro fatal em pipeline de MRR: {e}")
        sys.exit(1)
