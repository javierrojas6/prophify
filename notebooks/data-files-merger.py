# %%
import json
import os
import sys

import glob2

# %%
# solo para notebook
os.chdir("..")
# %%
sys.path.append(os.path.join(os.getcwd(), "src"))
# %%
from app.data.preparation.dataset_files_merger import DatasetFilesMerger
from app.file.file_ops import FileOps

# %%
output_path = "salud-total"
folder_path = "datasets/salud-total"
file_type = "csv"
encoding = "utf-8"
delimiter = ";"
transformations_path = f"{output_path}/fields.transformations.json"
field_settings_path = f"{output_path}/fields.settings.json"
field_types_path = f"{output_path}/fields.types.json"
try_field_format = True
try_field_replacements = True
# %%
folder_path = FileOps.path_validator(folder_path)
field_settings_path = FileOps.path_validator(field_settings_path)
field_types_path = FileOps.path_validator(field_types_path)

# %%
filenames = glob2.glob(f"{folder_path}/**/*.{file_type}", recursive=True)
# %%
transformations = None
with open(transformations_path, encoding=encoding) as f:
    transformations = json.load(f)
# %%
field_types = None
with open(field_types_path, encoding=encoding) as f:
    field_types = json.load(f)
# %%
field_settings = None
with open(field_settings_path, encoding=encoding) as f:
    field_settings = json.load(f)
# %%
df = DatasetFilesMerger(
    filenames=filenames,
    field_settings=field_settings,
    field_types=field_types,
    transformations=transformations,
    merged_filename=f"{output_path}/salud-total.merged.csv",
    encoding=encoding,
    delimiter=delimiter,
    save_intermediate=False,
)()

# %%
df.info(100)
