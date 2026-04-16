import logging
import pandas as pd

from .pipeline_base import BasePipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClientesPipeline(BasePipeline):
    """Base pipeline for clientes data processing."""

    def _processar_clientes_detalhado(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado específico para clientes - OTIMIZADO.
        """
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
        """
        Tratamentos de tipo específicos para clientes - SUPER OTIMIZADO.
        """
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
                df[colunas_existentes] = df[colunas_existentes].astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df


class CobrancasPipeline(BasePipeline):
    """Base pipeline for cobrancas data processing."""

    def _processar_cobrancas_detalhado(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado específico para cobranças - OTIMIZADO.
        """
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
        """
        Tratamentos de tipo específicos para cobranças - SUPER OTIMIZADO.
        """
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


class DespesasPipeline(BasePipeline):
    """Base pipeline for despesas data processing."""

    def _processar_despesas_detalhado(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado para despesas com todos os tratamentos
        """
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
        """
        Tratamentos de tipo específicos para despesas - SUPER OTIMIZADO.
        """
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
                df[colunas_existentes] = df[colunas_existentes].astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df


class ProdutoPipeline(BasePipeline):
    """Base pipeline for produto data processing."""

    def _processar_produto_detalhado(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado específico para produtos - OTIMIZADO.
        """
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

            df['produto'] = df['sacado_grupo'].apply(lambda x: x.get('st_nome_grp') if isinstance(x, dict) else None)

            produtos_desejados = [
                "App da Igreja",
                "App Lite Setup 0",
                "App Lite Setup R$",
                "Gateway Paypal - LITE",
                "Gateway Paypal - PRO",
                "White Label PRO"
            ]
            df = df[df['produto'].isin(produtos_desejados)]

            df = df.drop(columns=['sacado_grupo'])

        df = df.dropna(subset=['id_sacado_sac'])
        df = df.drop_duplicates(subset=['id_sacado_sac'])

        return df

    def _tratar_tipos_dados_produto(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamentos de tipo específicos para produtos - SUPER OTIMIZADO.
        """
        tipos_mapping = {
            'string': ['id_sacado_sac', 'st_nome_sac',
                       'st_sincro_sac', 'produto']
        }

        for tipo, colunas in tipos_mapping.items():
            colunas_existentes = [col for col in colunas if col in df.columns]

            if not colunas_existentes:
                continue

            if tipo == 'date':
                for col in colunas_existentes:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            elif tipo == 'float64':
                df[colunas_existentes] = df[colunas_existentes].astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df


class AcordosPipeline(BasePipeline):
    """Base pipeline for acordos data processing."""

    def _processar_acordos_detalhado(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado específico para acordos - OTIMIZADO.
        """
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
        """
        Tratamentos de tipo específicos para acordos - SUPER OTIMIZADO.
        """
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
                df[colunas_existentes] = df[colunas_existentes].astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df


class GrupoPipeline(BasePipeline):
    """Base pipeline for grupo data processing."""

    def _processar_grupo_detalhado(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado específico para grupos - OTIMIZADO.
        """
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
        """
        Tratamentos de tipo específicos para clientes - SUPER OTIMIZADO.
        """
        tipos_mapping = {
            'string': ['id_sacado_sac', 'st_nome_sac',
                       'st_sincro_sac', 'grupo']
        }

        for tipo, colunas in tipos_mapping.items():
            colunas_existentes = [col for col in colunas if col in df.columns]

            if not colunas_existentes:
                continue

            if tipo == 'date':
                for col in colunas_existentes:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            elif tipo == 'float64':
                df[colunas_existentes] = df[colunas_existentes].astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df


class MrrPipeline(BasePipeline):
    """Base pipeline for MRR data processing."""

    def processar_recorrencias(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado específico para cobranças - OTIMIZADO.
        """
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
        """
        Tratamentos de tipo específicos para cobranças - SUPER OTIMIZADO.
        """
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
                df[colunas_existentes] = df[colunas_existentes].astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df


class ProdutosBaseMRRPipeline(BasePipeline):
    """Base pipeline for produtos base MRR data processing."""

    def _processar_produtos_base_mrr_detalhado(self, dados_brutos) -> pd.DataFrame:
        """
        Processamento detalhado específico para produtos base MRR - OTIMIZADO.
        """
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
        """
        Tratamentos de tipo específicos para produtos base MRR - SUPER OTIMIZADO.
        """
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
                df[colunas_existentes] = df[colunas_existentes].astype('float64')
            elif tipo == 'Int64':
                for col in colunas_existentes:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:  # string
                df[colunas_existentes] = df[colunas_existentes].astype(str)

        return df
