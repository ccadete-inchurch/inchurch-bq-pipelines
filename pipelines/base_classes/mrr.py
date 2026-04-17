import logging
import pandas as pd

from ..pipeline_base import BasePipeline

logger = logging.getLogger(__name__)


class MrrPipeline(BasePipeline):
    """Base pipeline for MRR data processing."""

    def processar_recorrencias(self, dados_brutos) -> pd.DataFrame:
        if not dados_brutos:
            return pd.DataFrame()

        df = pd.DataFrame(dados_brutos)

        colunas_desejadas = [
            'id_sacado_sac', 'st_nome_sac', 'st_sincro_sac', 'dt_desativacao_sac', 'dt_cadastro_sac',
            'id_plano_pla', 'st_nome_pla', 'id_mensalidade_mens', 'st_valor_mens', 'st_qntd_mens',
            'dt_atualizacao_mens', 'id_produto_prd', 'dt_inicio_mens', 'dt_fim_mens'
        ]

        colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
        df = df[colunas_existentes]

        df = df.dropna(subset=['id_sacado_sac'])

        if 'st_nome_sac' in df.columns:
            mask_nome = ~df['st_nome_sac'].str.contains('Igreja Do Joao|Teste ', na=False)
            df = df[mask_nome]

        df['valor_total'] = pd.to_numeric(df['st_valor_mens'], errors='coerce') * \
            pd.to_numeric(df['st_qntd_mens'], errors='coerce')

        df = df.drop_duplicates(subset=['id_mensalidade_mens'])

        return df

    def _tratar_tipos_dados_mrr(self, df: pd.DataFrame) -> pd.DataFrame:
        tipos_mapping = {
            'date': ['dt_desativacao_sac', 'dt_cadastro_sac', 'dt_atualizacao_mens',
                     'dt_inicio_mens', 'dt_fim_mens'],
            'float64': ['st_valor_mens'],
            'Int64': ['st_qntd_mens'],
            'string': ['id_sacado_sac', 'st_nome_sac', 'st_sincro_sac', 'id_plano_pla',
                       'st_nome_pla', 'id_mensalidade_mens', 'id_produto_prd'],
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


class ProdutosBaseMRRPipeline(BasePipeline):
    """Base pipeline for produtos base MRR data processing."""

    def _processar_produtos_base_mrr_detalhado(self, dados_brutos) -> pd.DataFrame:
        if not dados_brutos:
            return pd.DataFrame()

        df = pd.DataFrame(dados_brutos)

        colunas_desejadas = [
            'id_produto_prd', 'st_descricao_prd'
        ]

        colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
        df = df[colunas_existentes]

        return df

    def _tratar_tipos_dados_produtos_base_mrr(self, df: pd.DataFrame) -> pd.DataFrame:
        tipos_mapping = {
            'string': ['id_produto_prd', 'st_descricao_prd']
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
