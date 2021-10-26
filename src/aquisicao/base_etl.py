import abc
from pathlib import Path
import typing
import pandas as pd


class BaseETL(abc.ABC):
    """
    Classe que estrutura como qualquer  objeto de ETL deve funciona
    """

    caminho_entrada: Path
    caminho_saida: Path

    """
    Um UnderLine na frente significa PROTECTED    
    """
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]

    @property
    def dados_entrada(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário de dados de entrada
        :return:dicionário com o nome do arquivo e um dataframe com os dados
        """
        if self._dados_entrada is None:
            self.extract()
        return self._dados_entrada

    @property
    def dados_saida(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário de dados de saída
        :return:dicionário com o nome do arquivo e um dataframe com os dados
        """
        if self._dados_saida is None:
            self.extract()
        return self._dados_saida

    def __init__(self, entrada: str, saida: str, criar_caminho: bool = True) -> None:
        """
        Instancia o objeto de ETL Base
        :param entrada: string com caminho para a pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param criar_caminho: flag se devemos criar os caminhos
        """
        self.caminho_entrada = Path(entrada)
        self.caminho_saida = Path(saida)

        if criar_caminho:
            self.caminho_entrada.mkdir(parents=True, exist_ok=True)
            self.caminho_saida.mkdir(parents=True, exist_ok=True)

        self._dados_entrada = None
        self._dados_saida = None
        """
        Dois UnderLine na frente significa PRIVATE
        """

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extrai os dados de objeto
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para o formato de saída de interesse
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        for arq, df in self._dados_saida.items():
            df.to_parquet(self._dados_saida / arq, index=False)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados
        """
        self.extract()
        self.transform()
        self.load()
    
    