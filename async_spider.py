#!/usr/bin/env python
# coding=utf-8

import os
import json
import asyncio

import aiofiles
from aiohttp import ClientSession
from python_utls import timeit


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
            if data and data.get('code', None) == 0:  # 只返回有效数据
                return data
    except Exception as e:     # 简单异常处理
        print(e)


async def run(start, stop):
    """
    运行主函数

    :param start: range start
    :param stop: range stop
    :return: 请求数据列表
    """
    url = "http://api.bilibili.com/archive_stat/stat?aid={}"
    # 创建 Semaphore 实例
    sem = asyncio.Semaphore(MAX_CONNECT_COUNT)

    # 创建可复用的 Session，减少开销
    async with ClientSession() as session:
        tasks = [
            asyncio.ensure_future(fetch(sem, url.format(i), session))
            for i in range(start, stop)]
        # 使用 gather(*tasks) 收集数据，wait(tasks) 不收集数据
        return await asyncio.gather(*tasks)


async def save(start, stop, path, label):
    """
    异步存储文件

    :param start: range start
    :param stop: range stop
    :param path: 文件路径
    :param label: 任务名称
    """
    print("启动: 任务 {}".format(label))
    data = await asyncio.gather(asyncio.ensure_future(run(start, stop)))
    result = [d for d in data[0] if d]
    async with aiofiles.open(path, mode='w+') as f:
        await f.write(json.dumps(result))
        print('爬取: {} 条数据'.format(len(result)))


if __name__ == "__main__":
    MAX_CONNECT_COUNT = 1024    # 最大并发数
    NUMBER = 100
    with timeit.timeit_block('m'):
        loop = asyncio.get_event_loop()
        # index = [(16, 17), (17, 18)]
        # tasks = [
        #     asyncio.ensure_future(
        #         save(
        #             start=NUMBER * i[0],
        #             stop=NUMBER * i[1],
        #             path=os.path.join("json", "{}.json".format(NUMBER * i[1])),
        #             label=i[1]
        #         ))
        #     for i in index]
        # loop.run_until_complete(asyncio.gather(*tasks))
        #
        for i in range(0, 18, 2):
            tasks = [
                asyncio.ensure_future(
                    save(
                        start=NUMBER * i[0],
                        stop=NUMBER * i[1],
                        path=os.path.join("data", "{}.json".format(NUMBER * i[1])),
                        label=i[1]
                    ))
                for i in [(i, i + 1), (i + 1, i + 2)]]
            loop.run_until_complete(asyncio.gather(*tasks))
