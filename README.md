# B 站异步爬虫初体验

异步这个词在 JS 这样的编程语言中可以说是非常熟悉了，但是在 Python 中却一直是鲜为人知，毕竟 Python 可以支持多线程/多进程，并发/并行处理也可以提升效率。但是 Python3 中最雄心勃勃的库可以算是 [asyncio](https://docs.python.org/3/library/asyncio.html) 了。该库使得 Python 代码也可以写成异步的形式了！很赞是吧。

异步代码编写也不算复杂，主要是比较难以理解。在看了多方文档之后，尝试来自己动手写一个异步爬虫试试，鉴于 B 站的 API 确实好爬，不容易被封，所以又被我拿来尝试了.....

## asyncio

关于异步，这里先给出一个很经典的小栗子。
```python
import asyncio
import time


async def async_func(name):
    print("执行函数 {}".format(name))
    await asyncio.sleep(1)  # 暂停一秒
    print("函数 {} 执行完毕".format(name))


start = time.time()
loop = asyncio.get_event_loop()
tasks = [
    asyncio.ensure_future(async_func("foo")),
    asyncio.ensure_future(async_func("baz")),
]
loop.run_until_complete(asyncio.wait(tasks))
print("共耗时 {}".format(time.time() - start))


执行代码，输出

执行函数 foo
执行函数 baz
函数 foo 执行完毕
函数 baz 执行完毕
共耗时 1.0010371208190918
```

相同的功能，普通的代码是这样的
```python
import time


def sync_func(name):
    print("执行函数 {}".format(name))
    time.sleep(1)
    print("函数 {} 执行完毕".format(name))


start = time.time()
sync_func("foo")
sync_func("baz")
print("共耗时 {}".format(time.time() - start))

执行代码，输出
执行函数 foo
函数 foo 执行完毕
执行函数 baz
函数 baz 执行完毕
共耗时 2.000490188598633
```
很明显，函数等待过程中线程并不会堵塞，而是先继续执行其他任务，等该任务执行完成再进行 *回调*。

关于 asyncio 更多资料请阅读 [官方文档](https://docs.python.org/3/library/asyncio.html)。


## aiohttp

用过 Python 的人肯定都用过 requests 库，但是该库是不支持异步的，看作者 Github 好像 3.0 版本开始会支持？在这里就要用到一个异步网络请求的第三库 [aiohttp](https://github.com/aio-libs/aiohttp)

没听过的话先看看下面的例子，该库仅支持 Python3.5+，因为 3.5+ 才提供了 `await/async` 的语法
```python
import aiohttp
import asyncio
import async_timeout

async def fetch(session, url):
    async with async_timeout.timeout(10):
        async with session.get(url) as response:
            return await response.text()

async def main():
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, 'http://python.org')
        print(html)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```

不知道为什么，莫名觉得这样的代码写起来很舒服....

接下来就是爬虫代码的编写了，参考了 [https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html](https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html) 这篇文章，具体请查看 [async_bili.py](https://github.com/chenjiandongx/async-bili-spider/blob/master/async_spider.py)。

和我的另外一个 B 站多进程爬虫 [chenjiandongx/bili-spider](https://github.com/chenjiandongx/bili-spider) 做了速度的对比。

**多进程爬取 20000 条数据，并发 1024 个线程**
``` shell
共耗时: 83.93677091598511 秒
```

**异步爬取 20000 条数据，并发数为 1000**
```shell
共耗时: 24.2032790184021 秒
```
两者效率好像差了接近 4 倍....，可以初探到异步编程的魅力了！!


## LICENSE

MIT [©chenjiandongx](https://github.com/chenjiandongx)
