#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
公共工具函数：init_rqdatac / get_store_path。

由 fetch_futures.py 和 fetch_stocks.py 共同引用，避免重复代码。
"""
from __future__ import annotations

import os
from pathlib import Path


def init_rqdatac():
    """初始化 rqdatac，支持 primary URI + backup 凭证自动降级。

    环境变量：
      RQDATA_PRIMARY_URI     (必需) primary 连接 URI，如 tcp://license:xxx@host:port
      RQDATA_BACKUP_PASSWORD  (可选) backup license key，primary 失败时使用
      RQDATA_BACKUP_USERNAME  (可选) backup 用户名，默认 "license"
      RQDATA_BACKUP_HOST      (可选) backup 主机地址
      RQDATA_BACKUP_PORT      (可选) backup 端口，默认 16011

    Raises:
        RuntimeError: 连接失败或缺少必要的环境变量
    """
    import rqdatac

    primary_uri = os.environ.get("RQDATA_PRIMARY_URI")
    if not primary_uri:
        raise RuntimeError(
            "[ERROR] 未设置环境变量 RQDATA_PRIMARY_URI\n"
            "请在 ~/.bashrc 中添加：export RQDATA_PRIMARY_URI='tcp://license:xxx@host:port'"
        )

    # 尝试 primary
    try:
        rqdatac.init(uri=primary_uri)
        print("[INFO] rqdatac 连接成功 (primary)")
        return rqdatac
    except Exception as e:
        print(f"[WARN] primary 连接失败: {type(e).__name__}")

    # 尝试 backup
    backup_pw = os.environ.get("RQDATA_BACKUP_PASSWORD")
    if not backup_pw:
        raise RuntimeError("[ERROR] primary 失败且未设置 RQDATA_BACKUP_PASSWORD，无法降级")

    backup_user = os.environ.get("RQDATA_BACKUP_USERNAME", "license")
    backup_host = os.environ.get("RQDATA_BACKUP_HOST")

    try:
        backup_port = int(os.environ.get("RQDATA_BACKUP_PORT", "16011"))
    except ValueError:
        raise RuntimeError("[ERROR] RQDATA_BACKUP_PORT 必须是数字")

    if not backup_host:
        raise RuntimeError("[ERROR] primary 失败且未设置 RQDATA_BACKUP_HOST，无法降级")

    try:
        rqdatac.reset()
    except Exception:
        pass

    try:
        rqdatac.init(backup_user, backup_pw, (backup_host, backup_port))
        print("[INFO] rqdatac 连接成功 (backup)")
        return rqdatac
    except Exception as e2:
        raise RuntimeError(f"[ERROR] primary + backup 均失败: {type(e2).__name__}") from e2


def get_store_path() -> Path:
    """从环境变量 RQDATA_STORE_PATH 读取数据存储根路径。

    Raises:
        RuntimeError: 未设置 RQDATA_STORE_PATH
    """
    store = os.environ.get("RQDATA_STORE_PATH")
    if not store:
        raise RuntimeError(
            "[ERROR] 未设置环境变量 RQDATA_STORE_PATH\n"
            "请在 ~/.bashrc 中添加：export RQDATA_STORE_PATH='/path/to/data'"
        )
    return Path(store)
