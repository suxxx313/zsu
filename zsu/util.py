from collections.abc import MutableMapping, MutableSequence
from io import IOBase, TextIOBase
from pathlib import Path

import pandas as pd


def is_file_handle(path_or_handle):
    """Is the object an open file handle?"""
    return isinstance(path_or_handle, (IOBase,))


def is_file_text_mode(f):
    """Determine if file handle was opened in binary or text mode

    Args:
        f (file object): Open file handle

    Returns:
        bool: True if file is in text mode; False if binary mode
    """
    if isinstance(f, TextIOBase):
        return True
    else:
        return 'b' not in f.mode


def validate_path(path):
    """Validate and resolve given path"""
    path = Path(str(path)).resolve()
    if not path.exists():
        raise ValueError("File does not exist: %s" % path)
    return path


def flatten(dict_or_list, key_prefix='', sep='.'):
    """Convert hierarchical dict or list to a flat dictionary

    Args:
        dict_or_list: The input structure to flatten
        key_prefix: A prefix to add to keys
        sep: The separator between levels

    Returns:
        A flat dictionary

    Examples:
        >>> flatten({'a': {'b': 'c'}}) # doctest: +SKIP
        {'a.b': 'c'}
        >>> flatten({'a': ['b', 'c']}) # doctest: +SKIP
        {'a[0]': 'b', 'a[1]': 'c'}
    """
    if isinstance(dict_or_list, MutableMapping):
        return _flatten_dict(dict_or_list, key_prefix, sep)
    elif isinstance(dict_or_list, MutableSequence):
        return _flatten_list(dict_or_list, key_prefix, sep)
    else:
        return dict_or_list


def _flatten_dict(d, key_prefix='', sep='.'):
    """Flatten a nested dictionary

    Examples:
        >>> _flatten_dict({'a': {'b': 'c'}}) # doctest: +SKIP
        {'a.b': 'c'}
    """
    items = []
    for k, v in d.items():
        new_key = key_prefix + sep + k if key_prefix else k
        if isinstance(v, MutableMapping):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, MutableSequence):
            items.extend(_flatten_list(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _flatten_list(items, key_prefix='', sep='.'):
    """Flatten a list to a dictionary

    Examples:
        >>> _flatten_list(['a', 'b'], 'A') # doctest: +SKIP
        {'A[1]': 'b', 'A[0]': 'a'}
    """
    return _flatten_dict({"%s[%d]" % (key_prefix, i): item
                          for i, item in enumerate(items)}, sep=sep)


def multicolumn_value_counts(df, normalize=False) -> pd.DataFrame:
    """Run value_counts on multiple columns."""
    # todo: Include both counts and normalize?
    return (
        pd.concat([
            col.value_counts(normalize=normalize) for _, col in df.items()
        ], axis=1, sort=False)
    )
