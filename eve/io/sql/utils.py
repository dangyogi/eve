# -*- coding: utf-8 -*-

"""
    eve.io.sql.utils
    ~~~~~~~~~~~~

"""

import collections
from eve.utils import config


def setdefaults(d, u):
    r'''Recursively copies values from the `u` dict that are not in the
    `d` dict.

    This means that any value already in `d` is not overridden by `u`.
    '''
    for k, v in u.items():
        if k not in d:
            d[k] = v
        elif isinstance(v, collections.Mapping) and \
             isinstance(d[k], collections.Mapping):
            setdefaults(d[k], v)


def validate_filters(where, resource):
    allowed = config.DOMAIN[resource]['allowed_filters']
    if '*' not in allowed:
        for filt in where:
            key = filt.left.key
            if key not in allowed:
                return "filter on '%s' not allowed" % key
    return None
