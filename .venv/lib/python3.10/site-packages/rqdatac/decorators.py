# -*- coding: utf-8 -*-
import time
import types
import warnings
from functools import wraps, partial, update_wrapper
import pandas as pd

import rqdatac
from rqdatac.share.errors import OverwriteWarning


def deprecated(func=None, msg=None):
    if func is None:
        return partial(deprecated, msg=msg)

    if msg is None:
        msg = func.__name__ + " is deprecated, and will be removed in future."

    @wraps(func)
    def wrap(*args, **kwargs):
        warnings.warn(msg, category=DeprecationWarning, stacklevel=0)
        return func(*args, **kwargs)

    return wrap


def export_as_api(f=None, name=None, namespace=None, priority=0):
    if f is None:
        return partial(export_as_api, name=name, namespace=namespace)
    name = name if name else f.__name__
    if namespace:
        if hasattr(rqdatac, namespace):
            namespace = getattr(rqdatac, namespace)
        else:
            namespace_name = namespace
            namespace = types.ModuleType(namespace_name)
            namespace.__file__ = 'rqdatac plugin'
            namespace.__module__ = "rqdatac"
            setattr(rqdatac, namespace_name, namespace)
            rqdatac.__all__.append(namespace_name)
    else:
        namespace = rqdatac
        rqdatac.__all__.append(name)

    old_f = getattr(namespace, name, None)
    if old_f is not None:
        if old_f.__priority > priority:
            warnings.warn("!!!! CAN'T OVERWRITE API {} WITH {} BECAUSE OLD PRIPORITY {} > {} !!!!".format(name, f, old_f.__priority, priority), category=OverwriteWarning)
            return f
        warnings.warn("!!!! OVERWRITE API {} WITH {} !!!!".format(name, f), category=OverwriteWarning)

    f.__priority = priority
    setattr(namespace, name, f)

    return f


def retry(count, suppress_exceptions, delay=1.0):
    def decorate(func):
        @wraps(func)
        def wrap(*args, **kwargs):
            c = count
            while c > 0:
                try:
                    return func(*args, **kwargs)
                except suppress_exceptions as e:
                    c -= 1
                    if c == 0:
                        raise e
                    if delay:
                        time.sleep(delay)

        return wrap

    return decorate


def coroutine(func):
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen

    return primer


_ttl_cached_functions = set()


def ttl_cache(ttl):
    if not isinstance(ttl, int) or not ttl > 0:
        raise TypeError("Expected ttl to be a positive integer")

    def decorating_function(user_function):
        wrapper = _ttl_cache_wrapper(user_function, ttl)
        _ttl_cached_functions.add(wrapper)
        return update_wrapper(wrapper, user_function)

    return decorating_function


def ttl_cache_clear():
    for f in _ttl_cached_functions:
        f.clear()


def _ttl_cache_wrapper(user_function, ttl):
    sentinel = object()
    cache = {}
    cache_get = cache.get  # bound method to lookup a key or return None
    cache_clear = cache.clear

    def wrapper(*args, **kwargs):
        if kwargs:
            key = args + (repr(sorted(kwargs.items())),)
        else:
            key = args

        # in cpython, dict.get is thread-safe
        result = cache_get(key, sentinel)
        if result is not sentinel:
            expire_at, value = result
            if expire_at > time.time():
                return value
        value = user_function(*args, **kwargs)
        cache[key] = (time.time() + ttl, value)
        return value

    setattr(wrapper, 'clear', cache_clear)
    return wrapper


def compatible_with_parm(func=None, name=None, value=None, replace=None):
    if func is None:
        return partial(compatible_with_parm, name=name, value=value, replace=replace)

    @wraps(func)
    def wrap(*args, **kwargs):
        if name:
            if name in kwargs:
                msg = "'{}' is no longer used, please use '{}' instead ".format(name, replace)
                warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
                item = kwargs.pop(name)
                if item != value:
                    raise ValueError("'{}': except '{}', got '{}'".format(name, value, item))
        return func(*args, **kwargs)

    return wrap


def may_trim_bjse(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        from rqdatac import client
        result = func(*args, **kwargs)
        if not client.bjse_enabled():
            # 根据各种可能的数据类型进行处理.
            if isinstance(result, pd.DataFrame):
                if "order_book_id" in result.columns:
                    result = result[~result["order_book_id"].str.endswith("BJSE")]
                elif result.index.name == "order_book_id":
                    result = result[~result.index.str.endswith("BJSE")]
                elif result.index.names[0] == "order_book_id":
                    result = result[~result.index.get_level_values(0).str.endswith('BJSE')]
            elif isinstance(result, list):
                result = [item for item in result if not item.endswith("BJSE")]
            elif isinstance(result, pd.Series) and result.index.name == "order_book_id":
                result = result[~result.index.str.endswith("BJSE")]
            elif isinstance(result, dict):
                # 指数成分, 指数权重相关API, 可能返回一个dict, 需要对dict内部的数据进行处理
                if func.__name__ == "index_components":
                    if len(result) == 0:
                        return {}
                    date, components = result.popitem()
                    # index_components 要求 rice_create_tm 字段时, 里面的数据为 (order_book_id列表, rice_create_tm) 元组
                    if isinstance(components, tuple):
                        real_components, rice_create_tm = components
                        final_result = {}
                        real_components = [item for item in real_components if not item.endswith("BJSE")]
                        final_result[date] = (real_components, rice_create_tm)
                        for date, components in result.items():
                            real_components, rice_create_tm = components
                            real_components = [item for item in real_components if not item.endswith("BJSE")]
                            final_result[date] = (real_components, rice_create_tm)
                        result = final_result
                    else:   # 没有要求 rice_create_tm 字段, value 为一个单纯的 order_book_id 列表
                        for k in result:
                            result[k] = [item for item in result[k] if not item.endswith("BJSE")]
                        components = [item for item in components if not item.endswith("BJSE")]
                        result[date] = components
                elif func.__name__ in ("index_weights", "index_weights_ex"):
                    for k in result:
                        result[k] = result[k][~result[k].index.str.endswith("BJSE")]
        return result

    return wrap
