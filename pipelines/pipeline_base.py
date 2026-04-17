import logging
import time
from typing import Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import create_engine

from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BasePipeline:
    def __init__(self, db_connection_string: str, access_token: str):
        self.db_engine = create_engine(
            db_connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self.url_base = Config.SUPERLOGICA_API_URL
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "app_token": Config.SUPERLOGICA_APP_TOKEN,
            "Access_token": access_token
        }

    def extrair_dados_endpoint(self, endpoint: str, max_paginas: Optional[int] = None, **params) -> List[Dict]:
        todos_dados = []

        if max_paginas:
            for pagina in range(1, max_paginas + 1):
                logger.info(f"Processando {endpoint} - página {pagina}/{max_paginas}")
                params['pagina'] = pagina

                max_tentativas = Config.API_MAX_RETRIES
                dados_pagina = None
                for tentativa in range(max_tentativas):
                    try:
                        response = requests.get(
                            f"{self.url_base}{endpoint}", headers=self.headers, params=params, timeout=Config.API_TIMEOUT
                        )
                        response.raise_for_status()
                        dados_pagina = response.json()

                        if not dados_pagina or not isinstance(dados_pagina, list):
                            logger.info(f"{endpoint} página {pagina} vazia. Finalizando extração.")
                            break

                        todos_dados.extend(dados_pagina)
                        time.sleep(Config.API_RATE_LIMIT_SLEEP)
                        break

                    except requests.exceptions.RequestException as e:
                        logger.warning(f"Falha na tentativa {tentativa + 1}/{max_tentativas} para a página {pagina}: {e}")
                        if tentativa < max_tentativas - 1:
                            import random
                            wait_time = min(2 ** tentativa + random.uniform(0, 1), 32)
                            time.sleep(wait_time)
                        else:
                            logger.error(f"Todas as {max_tentativas} tentativas falharam para a página {pagina}. Pulando para a próxima.")

                if not dados_pagina or not isinstance(dados_pagina, list):
                    break

        else:
            logger.info(f"Processando {endpoint} - requisição única.")
            try:
                response = requests.get(
                    f"{self.url_base}{endpoint}", headers=self.headers, params=params, timeout=Config.API_TIMEOUT
                )
                response.raise_for_status()
                dados = response.json()
                if dados and isinstance(dados, list):
                    todos_dados.extend(dados)
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro na requisição {endpoint}: {e}")

        logger.info(f"{endpoint}: {len(todos_dados)} registros totais extraídos da API.")
        return todos_dados

    def _otimizar_memoria(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.select_dtypes(include=['object']):
            if df[col].nunique() / len(df) < 0.5:
                df[col] = df[col].astype('category')
        return df

    def _registrar_validacao(self, script_rodado: str, mensagem: str = "", sucesso: bool = True):
        from sqlalchemy import text

        if not script_rodado:
            return

        try:
            with self.db_engine.connect() as conn:
                with conn.begin():
                    conn.execute(text("""
                        INSERT INTO public.splgc_validacoes (script, mensagem, sucesso, dt_update)
                        VALUES (:script, :mensagem, :sucesso, CURRENT_DATE)
                    """), {"script": script_rodado, "mensagem": mensagem, "sucesso": sucesso})
            logger.info(f"✅ Validação registrada: {script_rodado}")
        except Exception as e:
            logger.error(f"❌ Erro ao registrar validação: {e}")

    def carregar_para_postgres(self, df: pd.DataFrame, tabela: str, modo: str = 'replace', chave_unica: str = "", script_rodado: str = "", silenciar_validacao: bool = False):
        from sqlalchemy import inspect, text

        if df.empty:
            logger.info(f"DataFrame vazio, nenhuma ação de carga necessária para a tabela {tabela}.")
            return True

        inspector = inspect(self.db_engine)
        tabela_existe = inspector.has_table(tabela)

        logger.info("Deixando pandas inferir tipos automaticamente (sem dtype_mapping)")

        try:
            # --- CASO 1: A TABELA NÃO EXISTE ---
            if not tabela_existe:
                logger.info(f"Tabela '{tabela}' não existe. Criando pela primeira vez...")

                with self.db_engine.connect() as conn:
                    with conn.begin() as trans:
                        try:
                            df.head(0).to_sql(name=tabela, con=conn, if_exists='fail', index=False)
                            if chave_unica:
                                logger.info(f"Adicionando chave primária '{chave_unica}' à nova tabela '{tabela}'.")
                                conn.execute(text(f'ALTER TABLE "{tabela}" ADD PRIMARY KEY ("{chave_unica}");'))

                            df.to_sql(name=tabela, con=conn, if_exists='append', index=False, chunksize=Config.DB_BATCH_SIZE)
                            trans.commit()

                            mensagem_sucesso = f"✅ {script_rodado}. {len(df)} registros inseridos."
                            logger.info(mensagem_sucesso)

                            if not silenciar_validacao:
                                self._registrar_validacao(script_rodado, mensagem_sucesso, True)

                            return True

                        except Exception as e:
                            trans.rollback()
                            logger.error(f"❌ Erro na criação da tabela '{tabela}': {e}")

                            if not silenciar_validacao:
                                self._registrar_validacao(script_rodado, str(e), False)
                            raise e

            # --- CASO 2: MODO REPLACE ---
            if modo == 'replace':
                logger.info(f"Modo 'replace'. Executando TRUNCATE e inserindo dados em '{tabela}'.")
                with self.db_engine.connect() as conn:
                    with conn.begin() as _:
                        conn.execute(text(f'TRUNCATE TABLE "{tabela}";'))
                        df.to_sql(name=tabela, con=conn, if_exists='append', index=False, chunksize=Config.DB_BATCH_SIZE)

                mensagem_sucesso = f"✅ {script_rodado}. {len(df)} novos registros inseridos."
                logger.info(mensagem_sucesso)

                if not silenciar_validacao:
                    self._registrar_validacao(script_rodado, mensagem_sucesso, True)

                return True

            # --- CASO 3: MODO APPEND ---
            elif modo == 'append':
                if chave_unica:
                    temp_table = f"temp_{tabela.replace('-', '_')}"
                    with self.db_engine.connect() as conn:
                        try:
                            with conn.begin() as trans:
                                df.to_sql(name=temp_table, con=conn, if_exists='replace', index=False)
                                chave_unica_safe = chave_unica.replace('.', '_').replace('-', '_')
                                conn.execute(text(f'CREATE INDEX IF NOT EXISTS "idx_{chave_unica_safe}" ON "{temp_table}" ("{chave_unica}");'))
                                logger.info(f"{len(df)} registros inseridos na tabela temporária")

                                cols_list = df.columns
                                cols_sql_insert = ", ".join([f'"{col}"' for col in cols_list])
                                cols_sql_select = ", ".join([f'"{col}"' for col in cols_list])
                                update_cols_list = [col for col in cols_list if col != chave_unica]
                                update_cols_sql = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols_list])
                                upsert_sql = text(f"""
                                    INSERT INTO "{tabela}" ({cols_sql_insert})
                                    SELECT {cols_sql_select} FROM "{temp_table}"
                                    ON CONFLICT ("{chave_unica}") DO UPDATE SET {update_cols_sql};
                                """)
                                conn.execute(upsert_sql)
                                conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table}";'))

                            mensagem_sucesso = f"✅ {script_rodado}. Upsert de {len(df)} registros concluído com sucesso."
                            logger.info(mensagem_sucesso)

                            if not silenciar_validacao:
                                self._registrar_validacao(script_rodado, mensagem_sucesso, True)

                        except Exception as e:
                            logger.error(f"❌ Erro durante o upsert: {e}")

                            if not silenciar_validacao:
                                self._registrar_validacao(script_rodado, str(e), False)
                            try:
                                conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table}";'))
                                conn.commit()
                            except Exception as cleanup_error:
                                logger.warning(f"⚠️ Erro ao limpar tabela temporária {temp_table}: {cleanup_error}")
                            raise

                    return True

                else:
                    with self.db_engine.connect() as conn:
                        df.to_sql(name=tabela, con=conn, if_exists='append', index=False, chunksize=Config.DB_BATCH_SIZE)

                    mensagem_sucesso = f"✅ {script_rodado}. Carga (append) de {len(df)} registros concluída."
                    logger.info(mensagem_sucesso)

                    if not silenciar_validacao:
                        self._registrar_validacao(script_rodado, mensagem_sucesso, True)

                    return True

        except Exception as e:
            mensagem_erro = f"❌ {script_rodado}. Erro ao carregar em '{tabela}': {e}"
            logger.error(mensagem_erro)

            if not silenciar_validacao:
                self._registrar_validacao(script_rodado, mensagem_erro, False)
            raise e
