#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio

from aiohttp import ClientSession
import pyquery
import aiomysql


COLUMNS = ["v_view", "v_danmaku", "v_reply", "v_favorite", "v_coin", "v_share"]

HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PASSWORD = "root"
DATABASE = "chenx"
CHARSET = "utf8"


async def get_video_aid(col, loop):
    """
    获取视频 id

    :param col: 数据表列
    :param loop: 循环事件
    """
    conn = await aiomysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        db=DATABASE,
        charset=CHARSET,
        loop=loop,
    )

    async with conn.cursor() as cur:
        sql = f"select v_aid from bili_video order by {col} desc limit 100"
        await cur.execute(sql)
        result = await cur.fetchall()
    conn.close()
    return result


async def fetch(aid, session):
    """
    异步获取请求数据

    :param aid: 视频 id
    :param session: Session 实例
    """
    url = f"https://www.bilibili.com/video/av{aid}"
    try:
        async with session.get(url) as response:
            req = await response.text()
            q = pyquery.PyQuery(req)
            return {aid: q("h1[title]").text()}

    except Exception as e:
        print(e)


async def get_video_name(col, loop):
    """
    获取视频名称

    :param col: 数据表列
    :param loop: 循环事件
    """
    result = await get_video_aid(col, loop)
    async with ClientSession() as session:
        tasks = [asyncio.ensure_future(fetch(i[0], session)) for i in result]
        # 使用 gather(*tasks) 收集数据，wait(tasks) 不收集数据
        return await asyncio.gather(*tasks)


async def save_to_database(col, loop):
    """
    将数据更新至数据库

    :param col: 数据表列
    :param loop: 循环事件
    """
    result = await get_video_name(col, loop)
    conn = await aiomysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        db=DATABASE,
        charset=CHARSET,
        loop=loop,
    )

    result = [(v_name, v_aid) for r in result for v_aid, v_name in r.items()]
    async with conn.cursor() as cur:
        await cur.executemany(
            "update bili_video set v_name = %s where v_aid = %s", result
        )
        await conn.commit()
    conn.close()
    print(f"{col} DONE!!!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for col in COLUMNS:
        loop.run_until_complete(
            asyncio.wait([asyncio.ensure_future(save_to_database(col, loop))])
        )
