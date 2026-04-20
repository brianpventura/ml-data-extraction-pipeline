from abc import ABC, abstractmethod
from typing import Any, Dict, List

class BaseMarketplaceAdapter(ABC):
    """
    Abstract adapter defining the contract for standardizing
    order data from different marketplaces into a generic representation
    aligned with the database schema.
    """

    def __init__(self, raw_data: List[Dict[str, Any]], id_canal: int) -> None:
        self.raw_data = raw_data
        self.id_canal = id_canal

    @abstractmethod
    def padronizar_clientes(self) -> List[Dict[str, Any]]:
        """Returns standard customers list mapping dim_cliente."""
        pass

    @abstractmethod
    def padronizar_pedidos(self) -> List[Dict[str, Any]]:
        """Returns fact data for order headers."""
        pass

    @abstractmethod
    def padronizar_itens(self) -> List[Dict[str, Any]]:
        """Returns fact data for order items."""
        pass

    @abstractmethod
    def padronizar_transacoes(self) -> List[Dict[str, Any]]:
        """Returns unpivoted financial transaction data."""
        pass

    @abstractmethod
    def padronizar_anuncios(self) -> List[Dict[str, Any]]:
        """Returns dimension data mapping items to listings."""
        pass
