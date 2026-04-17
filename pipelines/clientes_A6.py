import logging

import pandas as pd

from .base_classes import ClientesPipeline
from .pipeline_factory import create_main_pipeline

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClientesPipelineA6(ClientesPipeline):
    """A6-specific override for clientes pipeline with grupo and produto columns."""

    def _processar_clientes_detalhado(self, dados_brutos) -> pd.DataFrame:
        """Process clientes for A6 with grupo and produto columns."""
        df = super()._processar_clientes_detalhado(dados_brutos)

        # Adicionar colunas "grupo" e "produto" específicas para A6
        df['grupo'] = "Atos6"
        df['produto'] = "Atos6"

        return df

    def _tratar_tipos_dados_clientes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Treat data types with A6-specific columns."""
        tipos_mapping = {
            'date': ['dt_desativacao_sac', 'dt_cadastro_sac'],
            'float64': ['mrr', 'total_devido'],
            'Int64': ['quantidade_cobs_atrasadas', 'st_diavencimento_sac'],
            'string': ['id_sacado_sac', 'st_nome_sac', 'st_sincro_sac',
                       'st_email_sac_1_1', 'st_email_sac_1_2', 'st_email_sac_2',
                       'st_telefone_sac', 'st_fax_sac', 'st_cgc_sac', 'grupo', 'produto']
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
                for coluna in colunas_existentes:
                    if coluna in ['id_sacado_sac', 'st_sincro_sac']:
                        mask = (df[coluna].notna()) & (df[coluna] != "") & (~df[coluna].astype(str).str.startswith("A"))
                        df.loc[mask, coluna] = "A" + df.loc[mask, coluna].astype(str)

        return df


    def executar(self, max_paginas: int):
        """
        Orquestra a execução do pipeline de clientes.
        """
        logger.info("🚀 Iniciando pipeline de clientes A6")

        parametros = {
            "apenasColunasPrincipais": "1",
            "status": "2",  # Busca todos os clientes (ativos, inativos, etc)
            "itensPorPagina": "200"
        }

        # Chama métodos herdados da classe mãe (BasePipeline)
        dados_brutos = self.extrair_dados_endpoint("clientes", max_paginas=max_paginas, **parametros)
        if not dados_brutos:
            return True  # Não é um erro fatal se a API não retornar dados

        # Chama métodos específicos desta classe filha
        df = self._processar_clientes_detalhado(dados_brutos)
        df = self._tratar_tipos_dados_clientes(df)

        if df.empty:
            logger.info("DataFrame vazio após processamento, nenhuma carga necessária.")
            return True

        # Chama mais métodos herdados
        df = self._otimizar_memoria(df)
        return self.carregar_para_postgres(
            df=df,
            tabela="splgc-clientes-a6",
            modo='append',
            chave_unica='id_sacado_sac',
            script_rodado="Pipeline de clientes A6"
        )


main_clientes_a6 = create_main_pipeline(
    ClientesPipelineA6,
    class_name="clientes A6",
    max_paginas=150,
    system="a6"
)
