import pickle
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sklearn
import sklearn.metrics
import sklearn.cross_decomposition
import sklearn.linear_model
import statsmodels
import statsmodels.api
import warnings

import mdm_python.data.db_entsoe as db_entsoe


model_directory = Path("data/models").resolve()


def prepare_raw_data(data: pd.DataFrame) -> dict:
    """
    Transform the data to log-scale with an offset to handle the high number of zeros
    The offset will change the skew-of the histogram close to 0
    """
    data = data.drop(columns="total")

    offset = dict(
        wind=1.4, solar=6, water_reservoir=900, water_river=150, water_pump=700
    )
    data_transformed = data.apply(
        lambda col: np.log10(col + offset[col.name]) if col.name in offset else col
    )

    """
    Create new DataFrame with the missing days in the index and NaN-Values
    Fill NaN-Values with the average between the previous and the next value
    """
    index_date = pd.date_range(
        start=data_transformed.index[0], end=data_transformed.index[-1], freq="D"
    )
    data_fixed = data_transformed.reindex(index_date)
    data_fixed = data_fixed.interpolate(method="linear")
    data_fixed = data_fixed.dropna()
    assert data_fixed.index.freq is not None, "Data must still be fixed-frequency"

    dict_of_transformed_data = dict()

    for col_name in data_fixed.columns:
        values = SimpleNamespace(
            name=col_name,
            transformed_values=data_fixed[col_name],
            offset=offset.get(col_name),
        )
        dict_of_transformed_data[col_name] = values

    return dict_of_transformed_data


def detrend_and_deseasonalize_data(prepared_data: dict) -> dict:
    """
    Detrend and deseasonlize the prepared data.
    Create a dictionnary with Name, detrended values and the baseline
    """
    warnings.filterwarnings(
        "ignore", message="Series.fillna with 'method' is deprecated.*"
    )

    breakpoints = dict(
        solar=pd.Timestamp(
            "2020-01-01", tz=prepared_data["solar"].transformed_values.index.tz
        ),  # breakpoints for structural break
    )

    for col_name, values in prepared_data.items():
        modelling_basis = values.transformed_values
        if breakpoints.get(col_name) is not None:
            modelling_basis = modelling_basis[
                modelling_basis.index >= breakpoints.get(col_name)
            ]
        weekly_seasonality = statsmodels.api.tsa.seasonal_decompose(
            modelling_basis, period=7, model="additive", extrapolate_trend="freq"
        )
        week_removed = modelling_basis - weekly_seasonality.seasonal
        yearly_seasonality = statsmodels.api.tsa.seasonal_decompose(
            week_removed, period=365, model="additive", extrapolate_trend="freq"
        )
        trend = yearly_seasonality.trend
        baseline = yearly_seasonality.seasonal + trend + weekly_seasonality.seasonal

        changed_data_deseasonalized = modelling_basis - baseline

        values.detrended_values = changed_data_deseasonalized
        values.baseline = baseline

    return prepared_data


def check_for_stationarity(name: str, data: pd.Series) -> bool:
    """
    Compute ADF-Value: If the value is very low (negative), it indicates, the data is Stationary
    Compute P-Value: If the value is between -0.05 and 0.05, it indicates, the data is Stationary
    """
    dftest = statsmodels.api.tsa.stattools.adfuller(data, autolag="AIC")

    if (dftest[0] < -2.8) and (abs(dftest[1]) > -0.05):
        # print(f'{name}-data is most likely Stationary\n')
        return True

    print(f"\nStationary-Test for {name}:")
    print("ADF:", dftest[0])
    print("P-Value:", dftest[1])

    print(f"{name}-data is most likely not Stationary!\n")
    return False


def check_if_white_noise(name: str, data: pd.Series) -> bool:
    """
    Create a TimeSeries-Plot: If the mean is different from 0, it indicates, the data is NOT White Noise
    Compute ACF and PACF: If at least one of them has some significant lags, it indicates, the data is NOT White Noise
    Compute Ljung-Box-Test: If the value is between -0.05 and 0.05, it indicates, the data is NOT White Noise
    """
    mean = data.mean()
    acf = statsmodels.tsa.stattools.acf(data, nlags=40)
    pacf = statsmodels.tsa.stattools.pacf(data, nlags=40)
    significant_acf_lags = (
        len([lag for lag, acf_value in enumerate(acf) if abs(acf_value) > 0.2]) - 1
    )
    significant_pacf_lags = (
        len([lag for lag, pacf_value in enumerate(pacf) if abs(pacf_value) > 0.2]) - 1
    )

    # Ljung-Box Test
    lb_test = statsmodels.api.stats.acorr_ljungbox(data, lags=[10], return_df=True)

    if (
        (significant_acf_lags > 1 or significant_pacf_lags > 1)
        and (abs(lb_test["lb_pvalue"].iloc[0]) < 0.05)
        and (mean != 0)
    ):
        # print(f'{name}-data is most likely not White Noise!\n')
        return True

    print(f"White-Noise-Test for {name}:")
    print(f'Ljung-Box-Test (p-value): {lb_test["lb_pvalue"].iloc[0]}')
    print(f"Mean: {mean}")
    print(f"Significant ACF-Lags: {significant_acf_lags}")
    print(f"Significant PACF-Lags: {significant_pacf_lags}")

    print(f"{name}-data is most likely White Noise!\n")
    return False


def get_acf_and_pacf_lags(name: str, data: pd.Series, plot_acf_pacf=False) -> tuple:
    """
    Compute the significant ACF- and PACF-Lags as a base for the modelling process
    If plot_acf_pacf is True, the ACF- and PACF-Lags are plotted
    """
    acf_data = statsmodels.tsa.stattools.acf(data, nlags=40)
    pacf_data = statsmodels.tsa.stattools.pacf(data, nlags=40)

    if plot_acf_pacf:
        # Create ACF plot
        plt.figure(figsize=(12, 6))
        statsmodels.graphics.tsaplots.plot_acf(
            data, lags=40, alpha=1
        )  # Plot ACF using statsmodels
        plt.fill_between(
            range(len(acf_data)), -0.2, 0.2, color="red", alpha=0.2
        )  # Add shaded region for significance level
        plt.title(f"Autocorrelation Function (ACF) of {name}")
        plt.xlabel("Lag")
        plt.ylabel("ACF")
        plt.show()

        # Create PACF plot
        plt.figure(figsize=(12, 6))
        statsmodels.graphics.tsaplots.plot_pacf(
            data, lags=40, alpha=1
        )  # Plot PACF using statsmodels
        plt.fill_between(
            range(len(pacf_data)), -0.2, 0.2, color="red", alpha=0.2
        )  # Add shaded region for significance level
        plt.title(f"Partial Autocorrelation Function (PACF) of {name}")
        plt.xlabel("Lag")
        plt.ylabel("PACF")
        plt.show()

    significant_acf_lags = (
        len([lag for lag, acf_value in enumerate(acf_data) if abs(acf_value) > 0.2]) - 1
    )
    significant_pacf_lags = (
        len([lag for lag, pacf_value in enumerate(pacf_data) if abs(pacf_value) > 0.2])
        - 1
    )

    return significant_acf_lags, significant_pacf_lags


def create_train_and_test_data(detrended_data: pd.Series, test_size=0.2):
    """
    Create a non-random Training- and a Test-Dataset
    """
    train_data = detrended_data.iloc[: -int(len(detrended_data) * test_size)]
    test_data = detrended_data.iloc[-int(len(detrended_data) * test_size) :]
    return train_data, test_data


def ARIMA_model(data: pd.Series, p: int, d: int, q: int):
    """
    Model the data with an ARIMA-Model; if there is seasonality, a seasonal difference is calculated
    Some warnings are ignored, since many parameters are tested for the best model
    """
    warnings.filterwarnings(
        "ignore", message="Non-stationary starting autoregressive parameters found."
    )
    warnings.filterwarnings(
        "ignore", message="Non-invertible starting MA parameters found."
    )
    warnings.filterwarnings(
        "ignore", message="Maximum Likelihood optimization failed to converge."
    )

    model = statsmodels.tsa.arima.model.ARIMA(data, order=(p, d, q), freq="D")
    fit_result = model.fit()
    return fit_result


def check_significance_of_coefficients(fitted_model, significance_level=0.5) -> bool:
    """
    Check, if the computed coefficients are significant
    """
    p_values = fitted_model.pvalues
    for i, p_value in enumerate(p_values):
        if not (abs(p_value) < significance_level):
            return False
        return True


def check_accuracy(fitted_model, test_data):
    """
    Check, how accurate the fitted Model used on the test-dataset,
    by computing the MeanSquaredError and the RootMeanSquaredError
    """
    y_true = test_data
    y_pred = fitted_model.forecast(steps=len(test_data))
    mse = sklearn.metrics.mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    return mse, rmse


def find_best_model(name, train_data, test_data, acf_lag, pacf_lag) -> dict:
    """
    Try different Meta-Parameters to check, if the AIC and MSE are becoming better
    Return the best model
    """
    best_model = None

    lowest_aic = 1000000
    lowest_mse = 1000000
    
    acf_lag = acf_lag if acf_lag<10 else 10 # Complex models need a lot of time to compute
    pacf_lag = pacf_lag if pacf_lag<10 else 10 # And are not performing better

    for p in range(acf_lag - 3 if acf_lag > 3 else 0, acf_lag + 1):
        for q in range(pacf_lag - 3 if pacf_lag > 3 else 0, pacf_lag + 1):
            for d in range(2):
                print(f"ARIMA for {name}: ({p}, {d}, {q})")
                fit_result = ARIMA_model(train_data, p, d, q)
                if not check_significance_of_coefficients(fit_result):
                    continue
                mse, rmse = check_accuracy(fit_result, test_data)
                if fit_result.aic < lowest_aic and mse < lowest_mse:
                    lowest_aic = fit_result.aic
                    lowest_mse = mse
                    best_model = dict(
                        name=name,
                        p=p,
                        d=d,
                        q=q,
                        model=fit_result.model,
                        mse=mse,
                        rmse=rmse,
                        aic=fit_result.aic,
                    )
    assert best_model is not None
    return best_model


def store_model(data_set: dict):
    model_directory.mkdir(parents=True, exist_ok=True)
    for name, data in data_set.items():
        with open(model_directory / f"{name}.pickle", "wb") as fh:
            pickle.dump(data.model, fh)


def run_modelling_process(raw_data: pd.Series) -> dict:
    """
    Prepare the data
    Check for Stationarity and White Noise
    Run the process to find the best model
    Store the model as a Pickle-File
    """
    data_set = prepare_raw_data(raw_data)
    data_set = detrend_and_deseasonalize_data(data_set)

    for name, data in data_set.items():
        if check_for_stationarity(name, data.detrended_values) and check_if_white_noise(
            name, data.detrended_values
        ):
            print(f"{name} is most likely stationary and not White Noise")

    for name, data in data_set.items():
        acf_lag, pacf_lag = get_acf_and_pacf_lags(name, data.detrended_values)
        train_data, test_data = create_train_and_test_data(data.detrended_values)
        best_model = find_best_model(name, train_data, test_data, acf_lag, pacf_lag)
        data.model = best_model

    store_model(data_set)

    return data_set


if __name__ == "__main__":
    energy_data = db_entsoe.extract_daily_energy()
    run_modelling_process(energy_data)
