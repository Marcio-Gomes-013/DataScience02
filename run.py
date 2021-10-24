import click
from src.aquisicao.opcoes import EnumETL
from src.aquisicao.opcoes import ETL_DICT
import src.urllib.configs as conf_geral


@click.group()
def cli():
    pass

@cli.group()
def aquisicao():
    """
    Grupo de comandos que executam as funções de aquisiçõa
    """
@aquisicao.command()
@click.option("--etl", type=click.Choice([s.value for s in EnumETL]), help="Nome do ETL a ser executado")
@click.option('--entrada', default=conf_geral.PASTA_DADOS, help="string com caminho para a pasta de entrada")
@click.option('--saida', default=conf_geral.PASTA_SAIDA_AQUISICAO, help="string com caminho para pasta de saída")
@click.option('--criar_caminho', default=True, help="flag se devemos criar os caminhos")
def processa_dado(etl: str, entrada: str, saida: str, criar_caminho: str) -> None:
    """
    Execta o pipeline de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param entrada: string com caminho para a pasta de entrada
    :param saida: string com caminho para pasta de saída
    :param criar_caminho: flag se devemos criar os caminhos
    """
    objeto = ETL_DICT[EnumETL(etl)](entrada, saida, criar_caminho)
    objeto.pipeline()

if __name__ == "__main__":
    cli()
