import logging
import pandas as pd

from ..pipeline_base import BasePipeline

logger = logging.getLogger(__name__)


class CobrancasPipeline(BasePipeline):
    """Base pipeline for cobrancas data processing."""

    def _processar_cobrancas_detalhado(self, dados_brutos) -> pd.DataFrame:
        if not dados_brutos:
            return pd.DataFrame()

        df = pd.DataFrame(dados_brutos)

        colunas_desejadas = [
            'id_sacado_sac', 'st_nome_sac', 'st_nomeref_sac', 'vl_total_recb',
            'st_sincro_sac', 'dt_desativacao_sac', 'st_telefone_sac', 'st_email_sac',
            'st_cgc_sac', 'id_recebimento_recb', 'dt_vencimento_recb', 'dt_recebimento_recb',
            'dt_geracao_recb', 'dt_competencia_recb', 'dt_liquidacao_recb', 'fl_status_recb',
            'nome_forma_pagamento_cliente', 'link_2via', 'compo_recebimento'
        ]

        colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
        df = df[colunas_existentes]

        if 'compo_recebimento' in df.columns:
            df_exploded = df.explode('compo_recebimento')

            colunas_comp_desejadas = [
                'st_descricao_prd',
                'st_mesano_comp',
                'st_valor_comp',
                'nm_quantidade_comp',
                'st_conta_cont',
                'descricaocontacategoria',
                'id_composicao_comp',
                'id_mensalidade_comp'
            ]

            comp_df = pd.json_normalize(df_exploded['compo_recebimento'].dropna().tolist())

            colunas_comp_existentes = [col for col in colunas_comp_desejadas if col in comp_df.columns]
            comp_df = comp_df[colunas_comp_existentes]

            comp_df.columns = ['comp.' + col for col in comp_df.columns]

            df_exploded = df_exploded.drop('compo_recebimento', axis=1)
            df = pd.concat([df_exploded.reset_index(drop=True), comp_df], axis=1)

        df = df.dropna(subset=['comp.id_composicao_comp'])
        df = df.drop_duplicates(subset=['comp.id_composicao_comp'], keep='first')

        if 'st_email_sac' in df.columns:
            mask_joao = ~df['st_email_sac'].str.contains('@joao', case=False, na=False)
            mask_inchurch = (~df['st_email_sac'].str.contains('@inchurch', case=False, na=False) |
                             df['st_email_sac'].str.contains('financeiro@inchurch', case=False, na=False))
            df = df[mask_joao & mask_inchurch]

        if 'st_nome_sac' in df.columns:
            mask_nome = ~df['st_nome_sac'].str.contains('Igreja Do Joao|Teste ', na=False)
            df = df[mask_nome]

        df['comp.valor'] = (pd.to_numeric(df['comp.nm_quantidade_comp'], errors='coerce') *
                           pd.to_numeric(df['comp.st_valor_comp'], errors='coerce'))

        return df

    def _tratar_tipos_dados_cobrancas(self, df: pd.DataFrame) -> pd.DataFrame:
        tipos_mapping = {
            'date': ['dt_vencimento_recb', 'dt_recebimento_recb', 'dt_liquidacao_recb',
                     'dt_desativacao_sac', 'comp.st_mesano_comp', 'dt_geracao_recb',
                     'dt_competencia_recb'],
            'float64': ['comp.st_valor_comp', 'vl_total_recb'],
            'Int64': ['comp.nm_quantidade_comp'],
            'string': ['id_sacado_sac', 'st_nome_sac', 'st_sincro_sac', 'st_email_sac',
                       'st_cgc_sac', 'comp.st_conta_cont', 'comp.descricaocontacategoria',
                       'fl_status_recb', 'st_telefone_sac', 'comp.st_descricao_prd',
                       'comp.id_composicao_comp', 'id_recebimento_recb', 'st_nomeref_sac',
                       'link_2via', 'nome_forma_pagamento_cliente', 'comp.id_mensalidade_comp']
        }

        for tipo, colunas in tipos_mapping.items():
            colunas_existentes = [col for col in colunas if col in df.columns]

            if not colunas_existentes:
                continue

            if tipo == 'date':
                for col in colunas_existentes:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            elif tipo == 'float64':
                df[colunas_existentes] = df[colunas_existentes].apply(pd.to_numeric, errors='coerce')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df
