from sklearn.metrics import mean_absolute_error, mean_squared_error

import pandas as pd
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

### calculate_metrics
# This function calculate the mean absolute error (mae), root mean squared error (rmse), mean forecast error (bias), correlation coefficient (R), and skill score.

# MAE, RMSE, and R are calculated using sklearn's metric module.

# Skill score is calculated by getting the climatology for each field within the input's date range.
# * Negative skill scores indicate the MSE for forecast is larger than the MSE for climatology
# * Positive skill scores indicate otherwise

# The function is very flexible given the data is formatted appropriately. It has the option of enabling normalization which is based on the average specified variable (ET, ETo, or ETof) throughout that field's historical data.
def calculate_metrics(
    data: pd.DataFrame,
    *,
    climatology_ref: pd.DataFrame,
    avgs_ref: pd.DataFrame,
    actual: str,
    expected: str,
    normalize: bool = False,
) -> pd.Series:
    # Calculate error metrics
    mae: float = mean_absolute_error(data[actual], data[expected])
    forecast_mse: float = mean_squared_error(data[actual], data[expected])
    rmse: float = np.sqrt(forecast_mse)

    # Correlation Coefficient (R)
    cor = data[actual].corr(data[expected]).astype(float)

    # Mean Forecast Bias determines if the forecast is overshooting or undershooting.
    # Greater positive number indicates overshooting.
    bias: float = np.mean(data[expected] - data[actual])

    # Climatology uses the mean actual variable for that time of year using historical data.
    field = data.head(1).squeeze()
    start_date = data["time"].min().dayofyear
    end_date = data["time"].max().dayofyear

    # Filter the climatology reference
    field_mask = climatology_ref["field_id"] == field["field_id"]
    crop_mask = climatology_ref["crop"] == field["crop"]
    date_mask = (climatology_ref["doy"] >= start_date) & (
        climatology_ref["doy"] <= end_date
    )

    climatology = climatology_ref[field_mask & crop_mask & date_mask][actual]
    climatology_mse = mean_squared_error(data[actual], climatology)

    # Positive skill score indicates the error in climatology is greater than forecast.
    # This means that forecast is outperforming climatology.
    skill_score = 1 - np.max(
        np.min((forecast_mse / climatology_mse), initial=2), initial=-1
    )

    if normalize:
        avg: float = avgs_ref[avgs_ref["field_id"] == field["field_id"]][actual].values[
            0
        ]

        mae: float = mae.astype(float) / avg.astype(float)
        rmse = np.sqrt(forecast_mse.astype(float) / avg.astype(float))
        bias = bias.astype(float) / avg.astype(float)

    return pd.Series(
        {
            "mae": mae.round(2),
            "rmse": rmse.round(2),
            "corr": cor.round(2),
            "bias": bias.round(2),
            "skill_score": skill_score.round(2),
        }
    )

### eval_metrics
# This function evaluates the metrics for each variable. The output is a DataFrame containing the metrics with a column specifying which variable (ET, ETo, ETof)
def eval_metrics(
    table: pd.DataFrame, by=["field_id", "crop"], **kwargs
) -> pd.DataFrame:
    metrics_table = pd.DataFrame(
        columns=[
            "field_id",
            "variable",
            "crop",
            "mae",
            "rmse",
            "corr",
            "bias",
            "skill_score",
        ]
    )

    et_metrics = (
        table.groupby(by=by)[["field_id", "crop", "time", "actual_et", "expected_et"]]
        .apply(
            calculate_metrics,
            actual="actual_et",
            expected="expected_et",
            **kwargs,
        )
        .reset_index()
    )
    et_metrics["variable"] = "ET"

    metrics_table = pd.concat(
        [
            et_metrics.astype(metrics_table.dtypes),
            metrics_table.astype(et_metrics.dtypes),
        ],
        ignore_index=True,
    )

    eto_metrics = (
        table.groupby(by=by)[["field_id", "crop", "time", "actual_eto", "expected_eto"]]
        .apply(
            calculate_metrics,
            actual="actual_eto",
            expected="expected_eto",
            **kwargs,
        )
        .reset_index()
    )
    eto_metrics["variable"] = "ETo"
    metrics_table = pd.concat(
        [
            eto_metrics.astype(metrics_table.dtypes),
            metrics_table.astype(eto_metrics.dtypes),
        ],
        ignore_index=True,
    )

    etof_metrics = (
        table.groupby(by=by)[
            ["field_id", "crop", "time", "actual_etof", "expected_etof"]
        ]
        .apply(
            calculate_metrics,
            actual="actual_etof",
            expected="expected_etof",
            **kwargs,
        )
        .reset_index()
    )
    etof_metrics["variable"] = "ETof"
    metrics_table = pd.concat(
        [
            etof_metrics.astype(metrics_table.dtypes),
            metrics_table.astype(etof_metrics.dtypes),
        ],
        ignore_index=True,
    )

    return metrics_table

### timeseries_rel
# This plot function utilizes the seaborn relplot method to create grids of plots. Particularly useful for showing distribution on one cell.
def timeseries_rel(
    data,
    *,
    y,
    plot="rel",
    col=None,
    row=None,
    hue=None,
    kind="line",
    refline=None,
    title="",
    ylabel="",
    as_percent=False,
    tighten=False,
    errorbar=None,
    export_img: bool | str = None,
    title_template={},
    **kwargs,
):
    match plot:
        case "rel":
            rel = sns.relplot(
                data=data,
                x="forecasting_date",
                y=y,
                col=col,
                row=row,
                hue=hue,
                kind=kind,
                errorbar=errorbar,
                **kwargs,
            )
        case "dis":
            rel = sns.displot(
                data=data, x=y, col=col, row=row, hue=hue, kind=kind, **kwargs
            )
        case "cat":
            rel = sns.catplot(
                data=data,
                x="forecasting_date",
                y=y,
                col=col,
                row=row,
                hue=hue,
                kind=kind,
                errorbar=errorbar,
                **kwargs,
            )
        case "lm":
            rel = sns.lmplot(
                data=data,
                x="forecasting_date",
                y=y,
                col=col,
                row=row,
                hue=hue,
                kind=kind,
                errorbar=errorbar,
                **kwargs,
            )
        case _:
            raise Exception("Not a valid plot type.")

    # Relabel y axis
    if ylabel:
        rel.set_ylabels(ylabel)
    # Relabel x axis
    rel.tick_params(axis="x", rotation=90)
    plt.suptitle(title, y=1.02)
    rel.set_titles(**title_template)
    if as_percent is True:
        for ax in rel.axes.flat:
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    if plot != "dis":
        rel.set_xlabels("Forecasting Date")
        for ax in rel.axes.flat:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%B"))

    if tighten:
        rel.figure.subplots_adjust(wspace=0, hspace=0.1)

    if refline:
        rel.refline(**refline)

    if type(export_img) is bool and export_img is True:
        rel.savefig(fname=f"images/monterey/{str(title)}")
    elif type(export_img) is str:
        rel.savefig(fname=f"images/monterey/{export_img}")

    return rel

### trim_extremes
# Trim the edges of the DataFrame along provided columns with provided threshold.
def trim_extremes(data, *, cols, threshold):
    # Convert single input as list
    if type(cols) is not list:
        cols = [cols]
    # Go through each column. Rank the values by % then remove the extremes.
    for c in cols:
        data[f"{c}_pct"] = data[c].rank(pct=True)
        data.drop(index=data[(data[f"{c}_pct"] <= threshold)].index, inplace=True)
        data.drop(columns=f"{c}_pct", inplace=True)
    return data