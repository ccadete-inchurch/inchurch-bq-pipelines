from .clientes import ClientesPipeline
from .cobrancas import CobrancasPipeline
from .despesas import DespesasPipeline
from .produto import ProdutoPipeline
from .acordos import AcordosPipeline
from .grupo import GrupoPipeline
from .mrr import MrrPipeline, ProdutosBaseMRRPipeline

__all__ = [
    'ClientesPipeline',
    'CobrancasPipeline',
    'DespesasPipeline',
    'ProdutoPipeline',
    'AcordosPipeline',
    'GrupoPipeline',
    'MrrPipeline',
    'ProdutosBaseMRRPipeline',
]
