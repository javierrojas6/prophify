from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, FilePath

from app.data.preparation.replacement.replacement_factory import ReplacementFactory
from app.data.preparation.transformation.transformation_factory import TransformationFactory
from app.data.preparation.transformation.transformation_operator import TransformationOperator

# The `DatasetPreparator` class is designed to prepare and transform datasets by applying replacements
# and transformations based on specified field settings and data types.
class DatasetPreparator(BaseModel):
    filename: FilePath
    field_settings: dict
    field_types: dict
    transformations: dict
    encoding: Optional[str] = "utf-8"
    delimiter: Optional[str] = ","

    def __call__(
        self,
        make_replacements: bool = True,
        make_transformations: bool = True,
        set_indexes: bool = True,
        only_cast_transformations: bool = True,
        to_file: FilePath = None,
    ) -> pd.DataFrame:
        df = pd.read_csv(self.filename, encoding=self.encoding, delimiter=self.delimiter)

        replacements_group = self.transformations["by_data_type"]

        for value_type in replacements_group:
            fields = self.field_types[value_type] if value_type in self.field_types else []
            available_fields = list(set(fields) & set(df.columns))

            if len(available_fields) == 0:
                continue

            # make replacements
            if "replacements" in replacements_group[value_type] and make_replacements:
                replacements_records = replacements_group[value_type]["replacements"]
                self.apply_replacements(df, available_fields, replacements_records)

            # make transformations
            if "transformations" in replacements_group[value_type] and make_transformations:
                transformations_records = replacements_group[value_type]["transformations"]
                self.apply_transformations(df, available_fields, transformations_records, only_cast_transformations)

        # set indexes
        if set_indexes:
            index_fields = self.field_settings["index"] if "index" in self.field_settings else []
            available_fields = list(set(index_fields) & set(df.columns))
            if len(available_fields) > 0:
                df.set_index(available_fields, inplace=True)

        if to_file:
            df.to_csv(to_file)

        return df

    def apply_replacements(self, df: pd.DataFrame, fields: List[str], replacements_records: dict):
        replacement_factory = ReplacementFactory()
        for repl in replacements_records:
            replacement_op = replacement_factory(repl)
            df = replacement_op(df, fields)
        return df

    def apply_transformations(
        self,
        df: pd.DataFrame,
        fields: List[str],
        transformations_records: dict,
        only_cast_transformations: bool = False,
    ):
        transformation_factory = TransformationFactory()
        for repl in transformations_records:
            transformation_op = transformation_factory(repl)

            if only_cast_transformations and transformation_op.operator != TransformationOperator.CAST:
                continue

            df = transformation_op(df, fields)
        return df
