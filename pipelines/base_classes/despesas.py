import logging
import pandas as pd

from ..pipeline_base import BasePipeline

logger = logging.getLogger(__name__)


class DespesasPipeline(BasePipeline):
    """Base pipeline for despesas data processing."""

    def _processar_despesas_detalhado(self, dados_brutos) -> pd.DataFrame:
        if not dados_brutos:
            return pd.DataFrame()

        df = pd.DataFrame([{
            'dt_liquidacao_mov': registro.get('dt_liquidacao_mov'),
            'st_historico_mov': registro.get('st_historico_mov'),
        } for registro in dados_brutos])

        centro_de_custo_list = []
        for i, registro in enumerate(dados_brutos):
            centro_de_custo = registro.get('centro_de_custo', [])
            if isinstance(centro_de_custo, list):
                for custo in centro_de_custo:
                    centro_de_custo_list.append({
                        'row_idx': i,
                        'st_descricao_cc': custo.get('st_descricao_cc'),
                        'id_centrocusto_cc': custo.get('id_centrocusto_cc'),
                        'valor': custo.get('valor'),
                    })

        if centro_de_custo_list:
            cc_df = pd.DataFrame(centro_de_custo_list)
            df_indexed = df.reset_index().rename(columns={'index': 'row_idx'})
            df = df_indexed.merge(cc_df, on='row_idx', how='left').drop('row_idx', axis=1)

        df = df.dropna(subset=['dt_liquidacao_mov'])

        df = self._tratar_tipos_dados_despesas(df)

        return df

    def _tratar_tipos_dados_despesas(self, df: pd.DataFrame) -> pd.DataFrame:
        tipos_mapping = {
            'date': ['dt_liquidacao_mov'],
            'float64': ['valor'],
            'string': ['st_historico_mov', 'st_descricao_cc', 'id_centrocusto_cc']
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
