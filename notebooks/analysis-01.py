# %%
import json
import os
import sys
import webbrowser
from base64 import b64encode

import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from tqdm import tqdm
from dash import Dash, dash_table, dcc, html
from plotly.subplots import make_subplots

# %%
# solo para notebook
os.chdir("..")
# %%
sys.path.append(os.path.join(os.getcwd(), "src"))
# %%
from app.data.preparation.dataset_preparator import DatasetPreparator
from app.file.file_ops import FileOps

# %%
folder = "salud-total"
filename = f"{folder}/salud-total.merged.csv"
file_type = "csv"
encoding = "utf-8"
delimiter = ","
field_settings_path = f"{folder}/fields.settings.json"
field_types_path = f"{folder}/fields.types.json"
transformations_path = f"{folder}/fields.transformations.json"
port = 8050
render_option = "interactive"
# %%
folder_path = FileOps.path_validator(filename)
field_settings_path = FileOps.path_validator(field_settings_path)
field_types_path = FileOps.path_validator(field_types_path)
transformations_path = FileOps.path_validator(transformations_path)
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
df = DatasetPreparator(
    filename=filename,
    field_settings=field_settings,
    field_types=field_types,
    transformations=transformations,
    encoding=encoding,
    delimiter=delimiter,
)(make_replacements=False, only_cast_transformations=True)
print("df shape", df.shape)
# %%
df.info(10)
# %% removes empty fields
print("removing empty fields")
not_empty_fields = [field for field in df.columns if len(df[field].unique()) > 1]
df = df[not_empty_fields]
print("df shape", df.shape)
# %%
df_tmp = df.index.value_counts()
has_more_than_one_record = (df_tmp > 1).any()

if has_more_than_one_record:
    df = df.sort_values(by=field_settings["date"][0], ascending=True).groupby(field_settings["index"][0]).tail()

print("count of rows ", df.shape[0])


# %%
def wrap_chart(figure, title="", render_option: str = "interactive", style: dict = {}) -> dbc.Card:
    content = []
    if title != "" and len(title) > 3:
        content += [
            html.H5(
                title,
                className="card-title",
            )
        ]

    if render_option == "interactive":
        content += [dcc.Graph(figure=figure)]
    else:
        img_bytes = figure.to_image(format="webp")
        encoding = b64encode(img_bytes).decode()
        img_b64 = "data:image/png;base64," + encoding
        content += [html.Img(src=img_b64, style={"height": "auto", "width": "100%"})]

    return dbc.Card([dbc.CardBody(content)], style=style)


def get_correlation_dataframe(
    df: pd.DataFrame,
    fields: list[str],
    field_settings: dict,
    method: str = "pearson",
) -> pd.DataFrame:
    df_corr = df[fields].corr(method=method)[field_settings["label"]]
    mask = np.zeros_like(df_corr, dtype=bool)
    mask[np.triu_indices_from(mask)] = True
    return df_corr.mask(mask).dropna(how="all").sort_values(by=field_settings["label"], ascending=True)


# %%
methods = ["pearson", "kendall", "spearman"]
# methods = ["pearson", "pearson", "pearson"]
with tqdm(total=len(methods), ncols=120, desc="rendering correlations") as pbar:
    fields = list(
        set(df.columns)
        & (
            set(field_settings["label"] + field_types["int"] + field_types["decimal"] + field_types["binary"])
            - set(field_settings["index"])
        )
    )
    fig = make_subplots(
        rows=1, cols=3, subplot_titles=methods, horizontal_spacing=0.1, vertical_spacing=0.1, shared_yaxes=True
    )
    for i, method in enumerate(methods):
        pbar.set_description("rendering %s method" % method)

        df_corr = get_correlation_dataframe(df, fields, field_settings, method)
        corr_fig = px.imshow(
            df_corr,
            text_auto=True,
            aspect="auto",
            title=f"Correlation matrix of all fields vs label using {method} method",
        )
        corr_fig.update_traces(texttemplate="%{z:<2.4f}")
        fig.add_trace(corr_fig.data[0], row=1, col=i + 1)

        pbar.update()

    fig.update_layout(
        title_text="Customizing Subplot Axes",
        coloraxis=dict(colorscale="Viridis"),
        height=4000,
        width=600,
    )
    fig.show()
# %%
app = Dash(name="Basic exploratory data analysis b-EDA")
# %%

components = []
components += [
    dbc.Row(
        [
            html.H1(
                children="Basic exploratory data analysis b-EDA",
                style={"textAlign": "center", "backgroundColor": "#FFF"},
            )
        ]
    )
]

# %%
if has_more_than_one_record:
    field_name = field_settings["index"][0]
    components += [
        dbc.Row(
            [
                html.H2(children="this dataset contains multiple records per ID"),
                html.P(children="here is a distribution of records count by IDs count"),
                wrap_chart(
                    px.histogram(
                        df_tmp,
                        labels={"count": f"{field_name} count", "value": "Amount of records"},
                        title="count of index vs count of records",
                        histfunc="count",
                    ),
                ),
            ]
        )
    ]

# %%
values = df[field_settings["label"][0]].value_counts()
components += [
    dbc.Row(
        [
            wrap_chart(
                title=f'Data distribution against label field {field_settings["label"][0]}',
                figure=px.pie(
                    values=values.values,
                    names=list(map(lambda x: f"{x[0]}: {x[1]}", zip(values.index, values.values))),
                ),
            )
        ]
    )
]

# %%
fields = set(df.columns) - set([field_settings["label"][0], field_settings["index"][0]]) - set(field_types["text"])
with tqdm(total=len(fields), ncols=120, desc="rendering all fields comparison") as pbar:
    cards = []
    for field in fields:
        pbar.set_description("rendering %s" % field)
        cards += [
            wrap_chart(
                px.scatter(
                    df,
                    x=field_settings["label"][0],
                    y=field,
                    color=field_settings["label"][0],
                    marginal_x="histogram",
                    marginal_y="histogram",
                    title=f'{field} vs {field_settings["label"][0]}',
                ),
                render_option=render_option,
            )
        ]
        pbar.update()

    components += [dbc.Row(cards)]

# %%
fields = list(
    set(df.columns)
    & (
        set(field_settings["label"] + field_types["int"] + field_types["decimal"] + field_types["binary"])
        - set(field_settings["index"])
    )
)
figs = []
for method in ["pearson", "kendall", "spearman"]:
    fig = px.imshow(
        get_correlation_dataframe(df, fields, field_settings, method),
        text_auto=True,
        aspect="auto",
        width=400,
        height=4000,
        title="Correlation matrix of all fields vs label using %s method" % method,
    )
    fig.update_traces(texttemplate="%{z:<2.4f}")
    figs += [fig]

components += [
    dbc.Row(
        [
            wrap_chart(
                figure=fig,
                render_option=render_option,
                style={"display": "inlineBlock"},
            )
            for fig in figs
        ]
    )
]

# %%
print("initialing dashboard")
app.layout = dbc.Container(
    components,
    fluid=True,
    className="py-3",
    style={"backgroundColor": "#FFF"},
)
# %%
webbrowser.open(f"http://localhost:{port}")
# %%
app.run(debug=True, port=8050)

# %%
