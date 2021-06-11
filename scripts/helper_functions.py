import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def bootstrap_confidence_interval(sample, stat_function=np.mean, iterations=1000, ci=95):
    """Calculate the CI of chosen sample statistic using bootstrap sampling.

    CI = confidence interval

    Parameters
    ----------
    sample: Numpy array
        1-d numeric data

    stat_function: function, optional (default=np.mean)
        Function for calculating as sample statistic on data

    iterations: int, optional (default=1000)
        Number of bootstrap samples to create

    ci: int, optional (default=95)
        Percent of distribution encompassed by CI, 0<ci<100

    Returns
    -------
    tuple: lower_ci(float), upper_ci(float), bootstrap_samples_statistic(array)
        Lower and upper bounds of CI, sample stat from each bootstrap sample
    """
    boostrap_samples = bootstrap(sample, n=iterations)
    bootstrap_samples_stat = list(map(stat_function, boostrap_samples))
    low_bound = (100. - ci) / 2
    high_bound = 100. - low_bound
    lower_ci, upper_ci = np.percentile(bootstrap_samples_stat,
                                       [low_bound, high_bound])
    return lower_ci, upper_ci, bootstrap_samples_stat


def bootstrap(x: np.array, n: int):
    return np.random.choice(x, size=(n, len(x)), replace=True)


def schema_helper(directory):
    """
    Pulls the header and a single row of data to help build a schema for a folder of CSV's.
    Parameters
    ----------
    directory

    Returns
    -------
    prints the headers and top row to copy and paste in to web based schema builder.
    """
    schemas = {}
    for entry in os.scandir(directory):
        if (entry.path.endswith(".csv")) and (entry.is_file()):
            header_row = pd.read_csv(entry.path, nrows=1, encoding='UTF-16', sep='\t')

            schemas.append(header_row)
            print(header_row.columns)

    print(schemas)


def ecdf(vector, distinct=False):
    """
    Returns the results of an empirical cumulative density function applied to our input vector.
    Parameters
    ----------
    vector: 1-d list or list like object
        a 1-d vector that stores the input for the ecdf.
    distinct: Boolean
        a boolean that indicates if the ECDF should increment for distinct values or for every value.

    Returns
    -------
    A tuple of np.array's that serve as the input, output pairs for an ECDF.
    """
    n = len(vector)
    x = np.array(vector)
    x = np.sort(x)
    if distinct:
        y = [sum(x[x < val])/n for val in x]
        y = np.array(y)
    else:
        y = np.arange(1, n+1) / n
    return x, y


def main():
    pass


if __name__ == '__main__':
    main()
