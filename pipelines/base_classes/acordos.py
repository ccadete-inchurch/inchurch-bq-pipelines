import logging
import pandas as pd

from ..pipeline_base import BasePipeline

logger = logging.getLogger(__name__)


class AcordosPipeline(BasePipeline):
    """Base pipeline for acordos data processing."""

    def _processar_acordos_detalhado(self, dados_brutos) -> pd.DataFrame:
        if not dados_brutos:
            return pd.DataFrame()

        df = pd.DataFrame(dados_brutos)

        colunas_desejadas = [
            'id_sacado_sac', 'st_nome_sac', 'st_sincro_sac',
            'id_recebimento_recb', 'dt_vencimento_recb', 'dt_competencia_recb',
            'dt_liquidacao_recb', 'fl_tipo_acoi', 'dt_desfeito_aco'
        ]

        colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
        df = df[colunas_existentes]

        return df

    def _tratar_tipos_dados_acordos(self, df: pd.DataFrame) -> pd.DataFrame:
        tipos_mapping = {
            'date': ['dt_vencimento_recb', 'dt_competencia_recb',
                     'dt_liquidacao_recb', 'dt_desfeito_aco'],
            'string': ['id_sacado_sac', 'st_nome_sac', 'st_sincro_sac',
                       'fl_tipo_acoi', 'id_recebimento_recb']
        }

        for tipo, colunas in tipos_mapping.items():
            colunas_existentes = [col for col in colunas if col in df.columns]

            if not colunas_existentes:
                continue

            if tipo == 'date':
                for col in colunas_existentes:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            elif tipo == 'float64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df
