#!/usr/bin/env python
# coding=utf-8

import time
import json
import asyncio
from contextlib import contextmanager

import aiofiles
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
        return await asyncio.gather(*tasks)


async def save(count, path='bili.json'):
    """
    异步存储文件

    :param count: 迭代次数
    :param path: 文件路径
    """

    data = await asyncio.gather(asyncio.ensure_future(run(count)))
    result = [d for d in data[0] if d]
    async with aiofiles.open(path, mode='w+') as f:
        await f.write(json.dumps(result))
        print('爬取: {} 条数据'.format(len(result)))


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
    NUMBER = 2000
    with timer():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.ensure_future(save(NUMBER)))
