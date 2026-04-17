import logging
import pandas as pd

from ..pipeline_base import BasePipeline

logger = logging.getLogger(__name__)


class GrupoPipeline(BasePipeline):
    """Base pipeline for grupo data processing."""

    def _processar_grupo_detalhado(self, dados_brutos) -> pd.DataFrame:
        if not dados_brutos:
            return pd.DataFrame()

        df = pd.DataFrame(dados_brutos)

        colunas_desejadas = [
            'id_sacado_sac', 'st_sincro_sac', 'st_nome_sac', 'sacado_grupo'
        ]

        colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
        df = df[colunas_existentes]

        if 'st_nome_sac' in df.columns:
            mask_nome = ~df['st_nome_sac'].str.contains('Igreja Do Joao|Teste ', na=False)
            df = df[mask_nome]

        if 'sacado_grupo' in df.columns:
            df = df.explode('sacado_grupo')
            df['grupo'] = df['sacado_grupo'].apply(lambda x: x.get('st_nome_grp') if isinstance(x, dict) else None)

            grupos_desejados = [
                "Ana Carolina",
                "Priscila Oliveira"
            ]
            df = df[df['grupo'].isin(grupos_desejados)]
            df = df.drop(columns=['sacado_grupo'])

        df = df.dropna(subset=['id_sacado_sac'])
        df = df.drop_duplicates(subset=['id_sacado_sac'])

        return df

    def _tratar_tipos_dados_clientes(self, df: pd.DataFrame) -> pd.DataFrame:
        tipos_mapping = {
            'string': ['id_sacado_sac', 'st_nome_sac', 'st_sincro_sac', 'grupo']
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
