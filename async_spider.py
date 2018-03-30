#!/usr/bin/env python
# coding=utf-8

import os
import time
import json
import asyncio

import aiofiles
import aiomysql
from aiohttp import ClientSession
from python_utls import timeit


async def fetch(sem, url, session):
    """
    异步获取请求数据

    :param sem: Semaphore 实例
    :param url: 请求链接
    :param session: Session 实例
    :return: 请求数据
    """
    try:
        async with sem:
            async with session.get(url) as response:
                data = await response.json()
            if data and data.get('code', None) == 0:  # 只返回有效数据
                return data
    except Exception as e:
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


async def save_to_files(start, stop, path, label):
    """
    异步存储数据至文件

    :param start: range start
    :param stop: range stop
    :param path: 文件路径
    :param label: 任务名称
    """
    print("Running: job {}".format(label))
    data = await asyncio.gather(asyncio.ensure_future(run(start, stop)))
    result = [d for d in data[0] if d]
    async with aiofiles.open(path, mode='w+') as f:
        await f.write(json.dumps(result))
        print('Fetch data: {}'.format(len(result)))


async def save_to_database(start, stop, label, loop):
    """
    异步存储数据至数据库

    :param start: range start
    :param stop: range stop
    :param label: 任务名称
    :param loop: 循环事件

    建表语句，只需建一次所以直接执行即可
    create table if not exists bili_video(
        v_aid int primary key,
        v_view int,
        v_danmaku int,
        v_reply int,
        v_favorite int,
        v_coin int,
        v_share int,
        v_name text);
    """
    print("Running: job {}".format(label))
    data = await asyncio.gather(asyncio.ensure_future(run(start, stop)))
    result = [
        (
            row['data']['aid'],         # 视频编号
            row['data']['view'],        # 播放量
            row['data']['danmaku'],     # 弹幕数
            row['data']['reply'],       # 评论数
            row['data']['favorite'],    # 收藏数
            row['data']['coin'],        # 硬币数
            row['data']['share'],       # 分享数
            ""                  # 视频名称（暂时为空）
        )
        for row in data[0]
        if row and row['data']['view'] != "--" and row['data']['aid'] != 0]
    conn = await aiomysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='0303',
        db='chenx',
        loop=loop)

    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO bili_video (v_aid, v_view, "
            "v_danmaku, v_reply, v_favorite, v_coin, v_share, v_name)"
            "values (%s, %s, %s, %s, %s, %s, %s, %s)", result)
        await conn.commit()
    conn.close()
    print('Fetch data: {}'.format(len(result)))


def get_files_tasks(index):
    """
    获取 `save_to_files()` 所需任务

    :param index: 任务索引
    :return: 任务列表
    """
    _tasks = [
        asyncio.ensure_future(
            save_to_files(
                start=NUMBER * i[0],
                stop=NUMBER * i[1],
                path=os.path.join("json", "{}.json".format(NUMBER * i[1])),
                label=i[1])
        ) for i in [(index, index + 1), (index + 1, index + 2)]
    ]
    return _tasks


def get_database_tasks(index, loop):
    """
    获取 `save_to_database()` 所需任务

    :param index: 任务索引
    :param loop: 事件循环
    :return: 任务列表
    """
    _tasks = [
        asyncio.ensure_future(
            save_to_database(
                start=NUMBER * i[0],
                stop=NUMBER * i[1],
                label=i[1],
                loop=loop)
        ) for i in [(index, index + 1), (index + 1, index + 2)]
    ]
    return _tasks


if __name__ == "__main__":
    MAX_CONNECT_COUNT = 1024    # 最大并发数
    NUMBER = 10000 * 50         # 单任务爬取数
    with timeit.timeit_block('h'):
        loop = asyncio.get_event_loop()
        for index in range(0, 42, 2):
            with timeit.timeit_block('m'):
                tasks = get_database_tasks(index, loop)
                # tasks = get_files_tasks(index)
                loop.run_until_complete(asyncio.gather(*tasks))
            print('Take a deep breath: 30s')
            time.sleep(30)
