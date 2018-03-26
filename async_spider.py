#!/usr/bin/env python
# coding=utf-8

import time
import json
import asyncio
from contextlib import contextmanager

from aiohttp import ClientSession


async def fetch(sem, url, session):
    """
    异步获取请求数据

    :param sem: Semaphore 实例
    :param url: 请求链接
    :param session: Session 实例
    :return:请求数据
    """
    try:
        async with sem:
            async with session.get(url) as response:
                data = await response.json()
            if data['code'] == 0:  # 只返回有效数据
                return data
    except Exception as e:     # 简单异常处理
        print(e)


async def run(count):
    """
    运行主函数

    :param count: 迭代次数
    :return: 请求数据列表
    """
    url = "http://api.bilibili.com/archive_stat/stat?aid={}"
    # 创建 Semaphore 实例
    sem = asyncio.Semaphore(MAX_CONNECT_COUNT)

    # 创建可复用的 Session，减少开销
    async with ClientSession() as session:
        tasks = [
            asyncio.ensure_future(fetch(sem, url.format(i), session))
            for i in range(count)]
        # 使用 gather(*tasks) 收集数据，wait(tasks) 不收集数据
        data = await asyncio.gather(*tasks)
    return data


# 上下文管理器，用与计算耗时
@contextmanager
def timer():
    start = time.time()
    try:
        yield
    finally:
        print('耗时: {} 秒'.format(time.time() - start))


if __name__ == "__main__":
    MAX_CONNECT_COUNT = 1000    # 最大并发数
    NUMBER = 20000
    with timer():
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(NUMBER))

        # 过滤数据
        result = [r for r in loop.run_until_complete(future) if r]
        with open('bili.json', 'w+') as f:
            json.dump(result, f)
    print('共爬取: {} 条数据'.format(len(result)))
