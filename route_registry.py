"""
Registry of all pipeline routes.
Each route entry contains: path, target function, and description.
"""

from pipelines.acordos import main_acordos
from pipelines.base_mrr import tabela_mrr
from pipelines.clientes_A6 import main_clientes_a6
from pipelines.clientes_inchurch import main_clientes_inchurch
from pipelines.cobrancas_comp_A6 import main_cobrancas_comp_a6
from pipelines.cobrancas_comp_A6_hist import main_cobrancas_comp_a6_hist
from pipelines.cobrancas_comp_inchurch import main_cobrancas_comp_inchurch
from pipelines.cobrancas_comp_inchurch_hist import main_cobrancas_comp_inchurch_hist
from pipelines.cobrancas_liq_A6 import main_cobrancas_liq_a6
from pipelines.cobrancas_liq_inchurch import main_cobrancas_liq_inchurch
from pipelines.despesas import main_despesas
from pipelines.grupo import main_grupo
from pipelines.produto import main_produto
from pipelines.produtos_mrr import main_produtos_base_mrr


PIPELINE_ROUTES = [
    {
        "path": "/cobrancas/comp_inchurch_hist/<int:year>/<int:month>",
        "function": main_cobrancas_comp_inchurch_hist,
        "args_names": ["year", "month"],
        "description": "Pipeline de cobranças por competência Inchurch histórico"
    },
    {
        "path": "/cobrancas/comp_inchurch/<int:year>/<int:month>",
        "function": main_cobrancas_comp_inchurch,
        "args_names": ["year", "month"],
        "description": "Pipeline de cobranças por competência Inchurch"
    },
    {
        "path": "/cobrancas/comp_a6",
        "function": main_cobrancas_comp_a6,
        "args_names": [],
        "description": "Pipeline de cobranças por competência A6"
    },
    {
        "path": "/cobrancas/comp_a6_hist",
        "function": main_cobrancas_comp_a6_hist,
        "args_names": [],
        "description": "Pipeline de cobranças por competência A6 histórico"
    },
    {
        "path": "/cobrancas/liq_inchurch/<int:year>/<int:month>",
        "function": main_cobrancas_liq_inchurch,
        "args_names": ["year", "month"],
        "description": "Pipeline de cobranças por liquidação Inchurch"
    },
    {
        "path": "/cobrancas/liq_a6",
        "function": main_cobrancas_liq_a6,
        "args_names": [],
        "description": "Pipeline de cobranças por liquidação A6"
    },
    {
        "path": "/clientes/inchurch",
        "function": main_clientes_inchurch,
        "args_names": [],
        "description": "Pipeline de clientes Inchurch"
    },
    {
        "path": "/clientes/a6",
        "function": main_clientes_a6,
        "args_names": [],
        "description": "Pipeline de clientes A6"
    },
    {
        "path": "/despesas/<int:year>",
        "function": main_despesas,
        "args_names": ["year"],
        "description": "Pipeline de despesas"
    },
    {
        "path": "/grupo",
        "function": main_grupo,
        "args_names": [],
        "description": "Pipeline de grupos"
    },
    {
        "path": "/produto",
        "function": main_produto,
        "args_names": [],
        "description": "Pipeline de produtos"
    },
    {
        "path": "/acordos",
        "function": main_acordos,
        "args_names": [],
        "description": "Pipeline de acordos"
    },
    {
        "path": "/mrr/<int:year>",
        "function": tabela_mrr,
        "args_names": ["year"],
        "description": "Pipeline de MRR"
    },
    {
        "path": "/produtos_mrr",
        "function": main_produtos_base_mrr,
        "args_names": [],
        "description": "Pipeline de produtos MRR"
    }
]
