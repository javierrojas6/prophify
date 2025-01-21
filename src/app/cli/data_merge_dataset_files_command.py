import csv
import json
import warnings

import click
import glob2

from app.cli.group import cli
from app.data.preparation.dataset_files_merger import DatasetFilesMerger

warnings.filterwarnings("ignore")


@cli.command("data:merge-files")
@click.argument("settings_path", type=click.Path())
@click.argument("types_path", type=click.Path())
@click.argument("folder_path", type=click.Path())
@click.argument("merged_filename", type=click.Path())
@click.option("-t", "--transformations-path", type=click.Path(), help="transformations to apply to the dataset")
@click.option("-e", "--extension", type=click.Choice(["json", "csv"]), default="csv", help="file extension")
@click.option("-d", "--delimiter", type=click.Choice([";", ","]), default=",", help="fields delimiter")
@click.option("-e", "--encoding", type=click.Choice(["utf-8", "ascii"]), default="utf-8", help="file encoding")
@click.option("-s", "--save-intermediate", type=click.BOOL, default=False, help="save intermediate files")
def data_merge_dataset_files_command(
    settings_path: str,
    types_path: str,
    folder_path: str,
    merged_filename: str,
    transformations_path: str,
    extension: str,
    delimiter: str,
    encoding: str,
    save_intermediate: bool,
):
    """
    Combines the files found so that they become a single file, note that if there
    are duplicate IDs you will end up with multiple records per ID\n
    SETTINGS_PATH: path to the field configuration file\n
    TYPE_PATH: path to the field types file\n
    FOLDER_PATH: path to the directory containing the files
    """
    filenames = glob2.glob(f"{folder_path}/**/*.{extension}", recursive=True)
    if len(filenames) == 0:
        click.echo(f"No files were found inside of {folder_path}", err=True, color=True)
        return
    elif len(filenames) == 1:
        click.echo("Only one file was found, nothing was mixed", err=True, color=True)
        return

    click.echo("were found {n} files".format(n=len(filenames)))

    for file in filenames:
        with open(file, "r", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            field_names = reader.fieldnames
            rows_count = len([row[0] for row in reader.reader])
            click.echo(" - {file} ({cols}, {rows})".format(file=file, cols=len(field_names), rows=rows_count))
            f.close()

    field_types = None
    with open(types_path, encoding=encoding) as f:
        field_types = json.load(f)

    field_settings = None
    with open(settings_path, encoding=encoding) as f:
        field_settings = json.load(f)

    transformations = None
    with open(transformations_path, encoding=encoding) as f:
        transformations = json.load(f)

    index_fields = field_settings["index"] if "index" in field_settings else []

    if len(index_fields) > 0:
        click.echo("The files will be merged using the field ({index})".format(index=index_fields[0]))
    else:
        click.echo(
            "No index found, please specify one in {field_settings}, and try it again".format(
                field_settings=settings_path
            ),
            err=True,
            color=True,
        )
        return

    sort_field = field_settings["date"] if "date" in field_settings else []
    if len(sort_field) > 0:
        click.echo("The files will be sorted by ({index})".format(index=sort_field[0]))

    elif len(sort_field) == 0:
        click.echo("There is no sorting field", err=True, color=True)
        return

    click.echo("")
    click.echo("mixing files...")

    DatasetFilesMerger(
        filenames=filenames,
        field_settings=field_settings,
        field_types=field_types,
        transformations=transformations,
        merged_filename=merged_filename,
        encoding=encoding,
        delimiter=delimiter,
        save_intermediate=save_intermediate,
    )()
