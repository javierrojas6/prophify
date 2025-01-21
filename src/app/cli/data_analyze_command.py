import click
import pandas as pd

from app.cli.group import cli


@cli.command("data:analyze")
@click.argument("filename", type=click.Path())
@click.option("-e", "--extension", type=click.Choice(["json", "csv"]), default="csv", help="file extension")
@click.option("-d", "--delimiter", type=click.Choice([";", ","]), default=",", help="fields delimiter")
@click.option("-e", "--encoding", type=click.Choice(["utf-8", "ascii"]), default="utf-8", help="file encoding")
def data_analyze_command(filename: str, extension: str, delimiter: str, encoding: str):
    """
    Analyze command
    """
    df = pd.read_csv(filename, encoding=encoding, delimiter=delimiter)
