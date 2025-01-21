import os
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, FilePath

from app.data.preparation.dataset_preparator import DatasetPreparator


# This class merges multiple dataset files based on specified field settings, types, and
# transformations, and saves the merged data to a CSV file.
class DatasetFilesMerger(BaseModel):
    filenames: List[FilePath]
    field_settings: dict
    field_types: dict
    transformations: dict
    merged_filename: str
    encoding: Optional[str] = "utf-8"
    delimiter: Optional[str] = ","
    save_intermediate: Optional[bool] = False

    def __call__(self) -> pd.DataFrame:
        dfs = []
        for file in self.filenames:

            to_file = None
            if self.save_intermediate:
                dirname = os.path.dirname(file)
                filename, ext = os.path.splitext(os.path.basename(file))
                to_file = f"{dirname}/{filename}.transformed{ext}"

            df_temp = DatasetPreparator(
                filename=file,
                field_settings=self.field_settings,
                field_types=self.field_types,
                transformations=self.transformations,
                encoding=self.encoding,
                delimiter=self.delimiter,
            )(to_file=to_file)

            dfs += [df_temp]

        df = None
        if len(self.filenames) == 1:  # no group possible
            df = dfs[0]

        if len(self.filenames) == 2:  # grouped
            a, b = dfs
            df = a.join(b)

        if df is not None:
            df = df.sort_values(by=self.field_settings["date"][0], ascending=True)
            df.to_csv(self.merged_filename, encoding=self.encoding)

        return df
