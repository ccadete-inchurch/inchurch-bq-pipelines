import logging
import pandas as pd

from ..pipeline_base import BasePipeline

logger = logging.getLogger(__name__)


class ClientesPipeline(BasePipeline):
    """Base pipeline for clientes data processing."""

    def _processar_clientes_detalhado(self, dados_brutos) -> pd.DataFrame:
        if not dados_brutos:
            return pd.DataFrame()

        df = pd.DataFrame(dados_brutos)

        colunas_desejadas = [
            'id_sacado_sac', 'st_sincro_sac', 'st_nome_sac', 'dt_desativacao_sac',
            'dt_cadastro_sac', 'st_diavencimento_sac', 'st_email_sac', 'st_telefone_sac',
            'st_fax_sac', 'st_cgc_sac', 'mrr', 'quantidade_cobs_atrasadas', 'total_devido'
        ]

        colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
        df = df[colunas_existentes]

        df = df.dropna(subset=['id_sacado_sac'])
        df = df.drop_duplicates(subset=['id_sacado_sac'])

        if 'st_email_sac' in df.columns:
            mask_joao = ~df['st_email_sac'].str.contains('@joao', case=False, na=False)
            mask_inchurch = (~df['st_email_sac'].str.contains('@inchurch', case=False, na=False) |
                             df['st_email_sac'].str.contains('financeiro@inchurch', case=False, na=False))
            df = df[mask_joao & mask_inchurch]

        if 'st_nome_sac' in df.columns:
            mask_nome = ~df['st_nome_sac'].str.contains('Igreja Do Joao|Teste ', na=False)
            df = df[mask_nome]

        if 'st_email_sac' in df.columns:
            df = df.assign(
                st_email_sac_1=df['st_email_sac'].str.split(';').str[0],
                st_email_sac_2=df['st_email_sac'].str.split(';').str[1]
            )
            df = df.assign(
                st_email_sac_1_1=df['st_email_sac_1'].str.split(',').str[0],
                st_email_sac_1_2=df['st_email_sac_1'].str.split(',').str[1]
            )
            df = df.drop(columns=['st_email_sac', 'st_email_sac_1'])
            df = df.rename(columns={'st_email_sac_1_1': 'st_email_sac.1.1'})
            df = df.rename(columns={'st_email_sac_1_2': 'st_email_sac.1.2'})
            df = df.rename(columns={'st_email_sac_2': 'st_email_sac.2'})

        return df

    def _tratar_tipos_dados_clientes(self, df: pd.DataFrame) -> pd.DataFrame:
        tipos_mapping = {
            'date': ['dt_desativacao_sac', 'dt_cadastro_sac'],
            'float64': ['mrr', 'total_devido'],
            'Int64': ['quantidade_cobs_atrasadas', 'st_diavencimento_sac'],
            'string': ['id_sacado_sac', 'st_nome_sac', 'st_sincro_sac',
                       'st_email_sac_1_1', 'st_email_sac_1_2', 'st_email_sac_2',
                       'st_telefone_sac', 'st_fax_sac', 'st_cgc_sac']
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
