import logging
import asyncio
import aiohttp
import logger



class Websocket:
    """ websocket接口封装
    """

    def __init__(self, url, check_conn_interval=2, send_hb_interval=2):
        """ 初始化
        @param url 建立websocket的地址
        @param check_conn_interval 检查websocket连接时间间隔
        @param send_hb_interval 发送心跳时间间隔，如果是0就不发送心跳消息
        """
        self._url = url
        self._check_conn_interval = check_conn_interval
        self._send_hb_interval = send_hb_interval
        self.ws = None  # websocket连接对象

    def initialize(self):
        """ 初始化
        """
        # 注册服务 检查连接是否正常
        print("注册服务 检查连接是否正常")
        heartbeat.register(self._check_connection, self._check_conn_interval)
        # 注册服务 发送心跳
        print("注册服务 发送心跳")
        # 建立websocket连接
        print("建立websocket连接")
        asyncio.get_event_loop().create_task(self._connect())

    async def _connect(self):
        logger.info("url:", self._url, caller=self)
        # print("proxy:",proxy)
        session = aiohttp.ClientSession()
        try:
            self.ws = await session.ws_connect(self._url, timeout=10, autoping=True)
            # print(self.ws)
        except Exception as e:
            print("ERROR:{},{}".format(e.__class__, e))
            self.ws = await session.ws_connect(self._url, timeout=10, autoping=True)
            print(self.ws)
        except aiohttp.client_exceptions.ClientConnectorError:
            logger.error("connect to server error! url:", self._url, caller=self)
            return
        asyncio.get_event_loop().create_task(self.connected_callback())
        asyncio.get_event_loop().create_task(self.receive())

    async def _reconnect(self):
        """ 重新建立websocket连接
        """
        logger.warn("reconnecting websocket right now!", caller=self)
        await self._connect()

    async def connected_callback(self):
        """ 连接建立成功的回调函数
        * NOTE: 子类继承实现
        """
        pass

    async def receive(self):
        """ 接收消息
        """
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    data = msg.data
                await asyncio.get_event_loop().create_task(self.process(data))
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await asyncio.get_event_loop().create_task(self.process_binary(msg.data))
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warn("receive event CLOSED:", msg, caller=self)
                await asyncio.get_event_loop().create_task(self._reconnect())
                return
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error("receive event ERROR:", msg, caller=self)
            else:
                logger.warn("unhandled msg:", msg, caller=self)

    async def process(self, msg):
        """ 处理websocket上接收到的消息 text 类型
        * NOTE: 子类继承实现
        """
        raise NotImplementedError

    async def process_binary(self, msg):
        """ 处理websocket上接收到的消息 binary类型
        * NOTE: 子类继承实现
        """
        raise NotImplementedError

    async def _check_connection(self, *args, **kwargs):
        """ 检查连接是否正常
        """
        # 检查websocket连接是否关闭，如果关闭，那么立即重连
        # print(self.ws)
        if not self.ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        if self.ws.closed:
            await asyncio.get_event_loop().create_task(self._reconnect())
            return

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """ 发送心跳给服务器
        """
        if not self.ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        if self.heartbeat_msg:
            if isinstance(self.heartbeat_msg, dict):
                await self.ws.send_json(self.heartbeat_msg)
            elif isinstance(self.heartbeat_msg, str):
                await self.ws.send_str(self.heartbeat_msg)
            else:
                logger.error("send heartbeat msg failed! heartbeat msg:", self.heartbeat_msg, caller=self)
                return
            logger.debug("send ping message:", self.heartbeat_msg, caller=self)




class HeartBeat(object):
    """ 心跳
    """

    def __init__(self):
        self._count = 0  # 心跳次数
        self._interval = 1  # 服务心跳执行时间间隔(秒)
        self._print_interval = 0 # 心跳打印时间间隔(秒)，0为不打印
        self._tasks = {}  # 跟随心跳执行的回调任务列表，由 self.register 注册 {task_id: {...}}

    @property
    def count(self):
        return self._count

    def ticker(self):
        """ 启动心跳， 每秒执行一次
        """
        self._count += 1

        # 打印心跳次数
        if self._print_interval > 0:
            if self._count % self._print_interval == 0:
                logger.info("do server heartbeat, count:", self._count, caller=self)

        # 设置下一次心跳回调
        asyncio.get_event_loop().call_later(self._interval, self.ticker)

        # 执行任务回调
        for task_id, task in self._tasks.items():
            interval = task["interval"]
            if self._count % interval != 0:
                continue
            func = task["func"]
            args = task["args"]
            kwargs = task["kwargs"]
            kwargs["task_id"] = task_id
            kwargs["heart_beat_count"] = self._count
            asyncio.get_event_loop().create_task(func(*args, **kwargs))

    def register(self, func, interval=1, *args, **kwargs):
        """ 注册一个任务，在每次心跳的时候执行调用
        @param func 心跳的时候执行的函数
        @param interval 执行回调的时间间隔(秒)
        @return task_id 任务id
        """
        t = {
            "func": func,
            "interval": interval,
            "args": args,
            "kwargs": kwargs
        }
        task_id = "websocket_task"
        self._tasks[task_id] = t
        return task_id

    def unregister(self, task_id):
        """ 注销一个任务
        @param task_id 任务id
        """
        if task_id in self._tasks:
            self._tasks.pop(task_id)


heartbeat = HeartBeat()
