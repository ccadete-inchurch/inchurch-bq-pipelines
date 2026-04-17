import logging

import pandas as pd

from .base_classes import DespesasPipeline
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DespesasInchurchPipeline(DespesasPipeline):
    """Inchurch-specific despesas pipeline."""

    def executar(self, dt_inicio: str, dt_fim: str, year: int):
        """
        Orquestra a execução do pipeline de despesas.
        """
        logger.info("🚀 Iniciando pipeline de despesas Inchurch")

        parametros = {
            "dtInicio": dt_inicio,
            "dtFim": dt_fim
        }

        dados_brutos = self.extrair_dados_endpoint("caixa", max_paginas=None, **parametros)
        if not dados_brutos:
            return True

        df = self._processar_despesas_detalhado(dados_brutos)
        df = self._tratar_tipos_dados_despesas(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        df = self._otimizar_memoria(df)
        return self.carregar_para_postgres(
            df=df,
            tabela=f"splgc-despesas-{year}",
            modo='replace',
            script_rodado=f"Pipeline de despesas Inchurch, {dt_inicio} a {dt_fim}"
        )


def main_despesas(year: int):
    access_token = Config.get_token("inchurch")
    db_connection = Config.DB_CONNECTION_STRING

    try:
        pipeline = DespesasInchurchPipeline(db_connection, access_token)
        sucesso = pipeline.executar(
            dt_inicio=f"01/01/{year}",
            dt_fim=f"12/31/{year}",
            year=year
        )
        status = "✅ Sucesso" if sucesso else "❌ Falha"
        print(f"Pipeline de despesas: {status}")

    except Exception as e:
        logger.error(f"💥 Erro fatal: {e}")
        raise
