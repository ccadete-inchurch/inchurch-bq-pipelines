import logging
import sys
from datetime import datetime

import pandas as pd

from .base_classes import CobrancasPipeline
from .pipeline_factory import create_main_pipeline
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CobrancasLiqInchurchPipeline(CobrancasPipeline):
    """Inchurch-specific cobrancas liquidação pipeline with error handling."""

    def executar(self, max_paginas: int, dt_inicio: str, dt_fim: str, script_rodado: str = '', silenciar_validacao: bool = False):
        logger.info("🚀 Iniciando pipeline de cobranças por liquidação Inchurch")

        parametros = {
            "apenasColunasPrincipais": "1",
            "exibirComposicaoDosBoletos": "1",
            "dtInicio": dt_inicio,
            "dtFim": dt_fim,
            "filtrarpor": "liquidacao",
            "itensPorPagina": "200"
        }

        try:
            dados_brutos = self.extrair_dados_endpoint("cobranca", max_paginas=max_paginas, **parametros)
            if not dados_brutos:
                logger.warning(f"Nenhum dado retornado para o período {dt_inicio} - {dt_fim}")
                return True

            df = self._processar_cobrancas_detalhado(dados_brutos)
            df = self._tratar_tipos_dados_cobrancas(df)

            if df.empty:
                logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
                return True

            df = self._otimizar_memoria(df)
            return self.carregar_para_postgres(
                df=df,
                tabela="splgc-cobrancas_liquidacao-all",
                modo='append',
                chave_unica='comp.id_composicao_comp',
                script_rodado=script_rodado,
                silenciar_validacao=silenciar_validacao
            )
        except Exception as e:
            logger.error(f"Erro ao executar {script_rodado}: {e}")
            return False


def main_cobrancas_liq_inchurch(year: int, month: int):
    access_token = Config.get_token("inchurch")
    db_connection = Config.DB_CONNECTION_STRING

    try:
        pipeline = CobrancasLiqInchurchPipeline(db_connection, access_token)

        if month != 0:
            # Mês específico: janela de 3 meses a partir do mês informado
            dt_inicio = f"{month:02d}/01/{year}"
            start = pd.to_datetime(dt_inicio) + pd.DateOffset(months=2)
            ultimo_dia = (start + pd.offsets.MonthEnd(0)).day
            dt_fim = f"{start.month:02d}/{ultimo_dia:02d}/{start.year}"

            sucesso = pipeline.executar(
                max_paginas=200,
                dt_inicio=dt_inicio,
                dt_fim=dt_fim,
                script_rodado=f"Pipeline de cobranças por liquidação Inchurch, {dt_inicio} a {dt_fim}"
            )
            status = "✅ Sucesso" if sucesso else "❌ Falha"
            print(f"Pipeline de cobranças {dt_inicio} a {dt_fim}: {status}")
            return

        # month == 0: loop mensal, notificação por trimestre
        trimestres = [(1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12)]

        for trimestre in trimestres:
            meses_sucesso = []
            dt_inicio_trimestre = None

            for mes in trimestre:
                dt_inicio = f"{mes:02d}/01/{year}"
                ultimo_dia = (pd.to_datetime(dt_inicio) + pd.offsets.MonthEnd(0)).day
                dt_fim = f"{mes:02d}/{ultimo_dia:02d}/{year}"

                if dt_inicio_trimestre is None:
                    dt_inicio_trimestre = dt_inicio

                sucesso = pipeline.executar(
                    max_paginas=200,
                    dt_inicio=dt_inicio,
                    dt_fim=dt_fim,
                    script_rodado=f"Pipeline de cobranças por liquidação Inchurch, {dt_inicio} a {dt_fim}",
                    silenciar_validacao=True
                )
                meses_sucesso.append(sucesso)

            status_bool = all(meses_sucesso)
            status_txt = "✅ Sucesso" if status_bool else "❌ Falha"

            pipeline._registrar_validacao(
                script_rodado=f"Pipeline de cobranças por liquidação Inchurch, {dt_inicio_trimestre} a {dt_fim}",
                mensagem=status_txt,
                sucesso=status_bool
            )
            print(f"Pipeline de cobranças {dt_inicio_trimestre} a {dt_fim}: {status_txt}")

    except Exception as e:
        logger.error(f"💥 Erro fatal: {e}")
        sys.exit(1)