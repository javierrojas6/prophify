import csv
import json

import click
import glob2

from app.cli.group import cli
from app.file.file_ops import FileOps


@cli.command("data:describe")
@click.argument("settings_path", type=click.Path())
@click.argument("types_path", type=click.Path())
@click.argument("folder_path", type=click.Path())
@click.option("-e", "--extension", type=click.Choice(["json", "csv"]), default="csv", help="file extension")
@click.option("-d", "--delimiter", type=click.Choice([";", ","]), default=",", help="fields delimiter")
@click.option("-e", "--encoding", type=click.Choice(["utf-8", "ascii"]), default="utf-8", help="file encoding")
def data_describe_command(
    settings_path: str, types_path: str, folder_path: str, extension: str, delimiter: str, encoding: str
):
    """
    Describes the morphology of the files found within the folder
    SETTINGS_PATH: path to the field configuration file\n
    TYPE_PATH: path to the field types file\n
    FOLDER_PATH: path to the directory containing the files
    """
    folder_path = FileOps.path_validator(folder_path)
    settings_path = FileOps.path_validator(settings_path)
    types_path = FileOps.path_validator(types_path)

    filenames = glob2.glob(f"{folder_path}/**/*.{extension}", recursive=True)
    if len(filenames) == 0:
        click.echo(f"No files were found inside of {folder_path}", err=True, color=True)
        return
    elif len(filenames) == 1:
        click.echo("Only one file was found, nothing was mixed", err=True, color=True)
        return

    click.echo("Were found {n} files".format(n=len(filenames)))

    for file in filenames:
        with open(file, "r", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            field_names = reader.fieldnames
            rows_count = len([row[0] for row in reader.reader])
            click.echo(" - {file} ({cols}, {rows})".format(file=file, cols=len(field_names), rows=rows_count))
            f.close()

    field_settings = None
    with open(settings_path, encoding=encoding) as f:
        field_settings = json.load(f)

    if not field_settings:
        click.echo("Settings file got error", err=True, color=True)
        return

    field_types = None
    with open(types_path, encoding=encoding) as f:
        field_types = json.load(f)

    if not field_types:
        click.echo("Field types file got error")
        return

    index_fields = field_settings["index"] if "index" in field_settings else []
    if len(index_fields) > 0:
        click.echo("Was found the index field: ({index})".format(index=index_fields[0]))
    else:
        click.echo(
            "No index found, please specify one in {field_settings},and try it again".format(
                field_settings=settings_path
            ),
            err=True,
            color=True,
        )
        return

    label_fields = field_settings["label"] if "label" in field_settings else []
    if len(label_fields) > 0:
        click.echo("Was found the label field ({index})".format(index=label_fields[0]))
    else:
        click.echo(
            "No label found, please specify one in {field_settings}, and try it again".format(
                field_settings=settings_path
            ),
            err=True,
            color=True,
        )
        return

    click.echo("The following types were found")
    fields_count = 0
    for key in field_types.keys():
        fields_count += len(field_types[key])
        click.echo(" - {key} with {count} fields".format(key=key, count=len(field_types[key])))
    click.echo("Total fields described: {}".format(fields_count))
