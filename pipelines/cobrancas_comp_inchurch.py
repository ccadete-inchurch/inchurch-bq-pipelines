import logging
import sys

import pandas as pd

from .base_classes import CobrancasPipeline
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CobrancasCompInchurchPipeline(CobrancasPipeline):
    """Inchurch-specific cobrancas competência pipeline."""

    def executar(self, max_paginas: int, dt_inicio: str, dt_fim: str, modo: str = 'append', script_rodado: str = ''):
        """
        Orquestra a execução do pipeline de cobranças.
        """
        logger.info("🚀 Iniciando pipeline de cobranças por competência Inchurch")

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
            tabela="splgc-cobrancas_competencia-all",
            modo=modo,
            chave_unica='comp.id_composicao_comp',
            script_rodado=script_rodado
        )


def main_cobrancas_comp_inchurch(year: int, month: int):
    access_token = Config.get_token("inchurch")
    db_connection = Config.DB_CONNECTION_STRING

    try:
        pipeline = CobrancasCompInchurchPipeline(db_connection, access_token)

        if month != 0:
            dt_inicio = f"{month:02d}/01/{year}"
            start = pd.to_datetime(dt_inicio) + pd.DateOffset(months=1)
            ultimo_dia = (start + pd.offsets.MonthEnd(0)).day
            dt_fim = f"{month + 1:02d}/{ultimo_dia:02d}/{year}"

            sucesso = pipeline.executar(
                max_paginas=200,
                dt_inicio=dt_inicio,
                dt_fim=dt_fim,
                modo='append',
                script_rodado=f"Pipeline de cobranças por competência Inchurch, {dt_inicio} a {dt_fim}"
            )
            status = "✅ Sucesso" if sucesso else "❌ Falha"
            print(f"Pipeline de cobranças {dt_inicio} a {dt_fim}: {status}")
            return

        # month == 0: loop bimestral (pares de meses), replace na primeira carga
        for mes in range(1, 12, 2):
            dt_inicio = f"{mes:02d}/01/{year}"
            start = pd.to_datetime(dt_inicio) + pd.DateOffset(months=1)
            ultimo_dia = (start + pd.offsets.MonthEnd(0)).day
            dt_fim = f"{mes + 1:02d}/{ultimo_dia:02d}/{year}"

            modo = 'replace' if dt_inicio == "01/01/2025" else 'append'

            sucesso = pipeline.executar(
                max_paginas=200,
                dt_inicio=dt_inicio,
                dt_fim=dt_fim,
                modo=modo,
                script_rodado=f"Pipeline de cobranças por competência Inchurch, {dt_inicio} a {dt_fim}"
            )
            status = "✅ Sucesso" if sucesso else "❌ Falha"
            print(f"Pipeline de cobranças {dt_inicio} a {dt_fim}: {status}")

    except Exception as e:
        logger.error(f"💥 Erro fatal: {e}")
        sys.exit(1)

