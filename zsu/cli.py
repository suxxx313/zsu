import click

from pathlib import Path

from .iterator import DataframeReader
from .run_model import RunModel


def str_vs_list(input):
    if "," in input:
        return input.split(",")
    else:
        return input


# noinspection PyShadowingBuiltins
@click.option('--modelversion', '-m',
              required=True,
              help="The model version to use for scoring")
@click.option('--output', '-o',
              required=True,
              help="Path and file name for output file")
@click.option('--input', '-i',
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Path to input file")
@click.option('--overwrite', '-w',
              default=True,
              help="Apply overwrites to final score"
              )
@click.option('--dump_vars', '-v',
              default=False,
              help="Dump all raw input attributes"
              )
@click.option('--sep', '-s',
              default=',',
              help="sep parameter for input file ")
@click.option('--header', '-a',
              default=0,
              help="header parameter for read_csv"
              )
@click.command(context_settings=dict(help_option_names=['-h', '--help']))
def cli(output, input, modelversion, overwrite, dump_vars, sep, header):
    input_path = Path(str(input)).resolve()
    output_path = Path(str(output))

    execute(modelversion, input_path, output_path, overwrite, dump_vars, sep, header)


def execute(modelversion, input_path, output_path, overwrite, dump_vars, sep, header):
    read_csv_kwargs = {
        "sep": sep,
        "header": header,
        "low_memory": False,
    }
    scoring_params = {
        "apply_overwrites": overwrite,
        "output_raw_attr": dump_vars
    }
    input_df = (
        DataframeReader(chunksize=10000)
        .from_file(input_path, **read_csv_kwargs)
    )
    output = (
        RunModel(str_vs_list(modelversion), input_df=input_df)
        .options(**scoring_params)
        .to_csv(output_path)
    )
    return output
