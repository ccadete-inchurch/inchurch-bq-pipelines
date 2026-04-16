import logging
import sys

import pandas as pd

from .base_classes import CobrancasPipeline
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CobrancasCompA6HistPipeline(CobrancasPipeline):
    """A6-specific cobrancas competência historical pipeline."""

    def executar(self, max_paginas: int, dt_inicio: str, dt_fim: str):
        """
        Orquestra a execução do pipeline de cobranças.
        """
        logger.info("🚀 Iniciando pipeline de cobranças por competência A6")

        parametros = {
            "apenasColunasPrincipais": "1",
            "exibirComposicaoDosBoletos": "1",
            "dtInicio": dt_inicio,
            "dtFim": dt_fim,
            "filtrarpor": "competencia",
            "itensPorPagina": "200"
        }

        dados_brutos = self.extrair_dados_endpoint("cobranca", max_paginas=max_paginas, **parametros)
        if not dados_brutos:
            return True

        df = self._processar_cobrancas_detalhado(dados_brutos)
        df = self._tratar_tipos_dados_cobrancas(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        df = self._otimizar_memoria(df)
        return self.carregar_para_postgres(
            df=df,
            tabela="splgc-cobrancas_competencia-hist",
            modo='append',
            chave_unica='comp.id_composicao_comp',
            script_rodado=f"Pipeline de cobranças por competência A6 histórico, {dt_inicio} a {dt_fim}"
        )


def main_cobrancas_comp_a6_hist():
    access_token = Config.get_token("a6")
    db_connection = Config.DB_CONNECTION_STRING

    try:
        pipeline = CobrancasCompA6HistPipeline(db_connection, access_token)

        for year in range(2021, 2025):
            dt_inicio = f"01/01/{year}"
            dt_fim = f"12/31/{year}"
            sucesso = pipeline.executar(
                max_paginas=250,
                dt_inicio=dt_inicio,
                dt_fim=dt_fim
            )
            status = "✅ Sucesso" if sucesso else "❌ Falha"
            print(f"Pipeline de cobranças competência A6 histórico {year}: {status}")

    except Exception as e:
        logger.error(f"💥 Erro fatal: {e}")
        sys.exit(1)
