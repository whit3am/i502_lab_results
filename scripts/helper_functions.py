import numpy as np


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
