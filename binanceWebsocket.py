import asyncio
import json
from datetime import datetime
import os
import traceback
import aiohttp

from wechaty import Wechaty, Message, WechatyPlugin, Room,WechatyOptions
from wechaty_puppet_service import puppet
from wechaty_puppet import PuppetOptions, EventType, EventScanPayload, ScanStatus, EventLoginPayload
from wechaty_puppet.schemas.room import RoomQueryFilter
from wechaty_puppet.schemas.contact import ContactQueryFilter

from websocketAPI import heartbeat, Websocket
import logger



WECHATY_PUPPET_SERVICE_TOKEN = ''
WECHATY_PUPPET = 'wechaty-puppet-service'

os.environ['WECHATY_PUPPET_SERVICE_TOKEN'] = WECHATY_PUPPET_SERVICE_TOKEN
os.environ['WECHATY_PUPPET'] = WECHATY_PUPPET



class Binance(Websocket):
    """ Binance 行情数据
    """

    def __init__(self, to_wechat_id):
        self._platform = "BINANCE"
        self._url = "wss://stream.binance.com:9443"
        self._symbols = ['BTCUSDT', 'ETHUSDT']
        self._channels = ['kline']
        # print(self._symbols)
        self._c_to_s = {}  # {"channel": "symbol"}
        self._tickers = {}  # 最新行情 {"symbol": price_info}

        # self.my_wechat_id = my_wechat_id
        self.to_wechat_id = to_wechat_id
        self.bot: Wechaty = None
        asyncio.get_event_loop().run_until_complete(self.init_wechat_bot())

        self._make_url()
        super(Binance, self).__init__(self._url)
        self.initialize()

        self.volatility_threshold = 0.0001  # 波动大于 0.01%的时候发出提醒
        self.kline_init = False
        self.last_timestamp = None  # 上一个event k线的timestamp
        self.current_timestamp = None  # 当前event k线的timestamp
        self.has_sent_notification = {}


    async def init_wechat_bot(self):
        puppet_options = PuppetOptions()
        puppet_options.token = WECHATY_PUPPET_SERVICE_TOKEN

        options = WechatyOptions()
        # options.name = self.my_wechat_id
        options.puppet = WECHATY_PUPPET
        options.puppet_options = puppet_options

        self.bot = Wechaty(options)
        await self.bot.init_puppet()
        await self.bot.init_puppet_event_bridge(self.bot.puppet)
        self.bot.puppet._init_puppet()
        # print(self.bot.puppet.login_user_id)
        async for response in self.bot.puppet.puppet_stub.event():
            if response is not None:
                payload_data: dict = json.loads(response.payload)
                if response.type == int(EventType.EVENT_TYPE_SCAN):
                    logger.debug('receive scan info <%s>', payload_data)
                    # create qr_code
                    payload = EventScanPayload(
                        status=ScanStatus(payload_data['status']),
                        qrcode=payload_data.get('qrcode', None),
                        data=payload_data.get('data', None)
                    )
                    print('scan payload_data', payload_data)
                    self.bot.puppet._event_stream.emit('scan', payload)

                elif response.type == int(EventType.EVENT_TYPE_LOGIN):
                    logger.debug('receive login info <%s>', payload_data)
                    print('login payload_data', payload_data)
                    event_login_payload = EventLoginPayload(
                        contact_id=payload_data['contactId'])
                    self.bot.puppet.login_user_id = payload_data.get('contactId', None)
                    self.bot.puppet._event_stream.emit('login', event_login_payload)
                    break


    async def send_message(self, message):
        contact = await self.bot.Contact.find(self.to_wechat_id)
        if contact:
            await contact.say(message)

    def _make_url(self):
        """ 拼接请求url
        """
        cc = []
        for ch in self._channels:
            if ch == "kline":  # 订阅K线 1分钟
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "kline_5m")
                    cc.append(c)
            elif ch == "orderbook":  # 订阅订单薄 深度为5
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "depth20")
                    cc.append(c)
            elif ch == "trade":  # 订阅实时交易
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "trade")
                    cc.append(c)
            else:
                logger.error("channel error! channel:", ch, caller=self)
        self._url += "/stream?streams=" + "/".join(cc)

    async def process(self, msg):
        """ 处理websocket上接收到的消息
        """
        msg = json.loads(msg)
        logger.debug("msg:", msg, caller=self)
        if not isinstance(msg, dict):
            return

        channel = msg.get("stream")
        print(channel, self._c_to_s)
        if channel not in self._c_to_s:
            logger.warn("unkown channel, msg:", msg, caller=self)
            return

        symbol = self._c_to_s[channel]
        data = msg.get("data")
        e = data.get("e")  # 事件名称
        if e == "kline":  # K线

            kline = {
                "platform": self._platform,
                "symbol": symbol,
                "open": data.get("k").get("o"),  # 开盘价
                "high": data.get("k").get("h"),  # 最高价
                "low": data.get("k").get("l"),  # 最低价
                "close": data.get("k").get("c"),  # 收盘价
                "volume": data.get("k").get("q"),  # 交易量
                "timestamp": data.get("k").get("t"),  # 时间戳
            }
            # print("symbol:", symbol, "kline:", kline)
            if not self.kline_init:
                self.last_timestamp = data.get("k").get("t")
                self.current_timestamp = data.get("k").get("t")
                self.has_sent_notification = {str(self.current_timestamp): False}
                self.kline_init = True

            if self.current_timestamp != self.last_timestamp:
                self.has_sent_notification = {str(self.current_timestamp): False}

            logger.info("symbol:", symbol, "kline:", kline, caller=self)
            high, low = float(data.get("k").get("h")), float(data.get("k").get("l"))
            volatility = (high-low) / high
            if volatility > self.volatility_threshold and not self.has_sent_notification[str(self.current_timestamp)]:
                await self.send_message("{} 5分钟内波幅达到{:.4f}%!".format(symbol, volatility*100))
                self.has_sent_notification[str(self.current_timestamp)] = True


        elif channel.endswith("depth20"):  # 订单薄
            bids = []
            asks = []
            for bid in data.get("bids"):
                bids.append(bid[:2])
            for ask in data.get("asks"):
                asks.append(ask[:2])
            orderbook = {
                "platform": BINANCE,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": tools.get_cur_timestamp_ms()
            }
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)
        elif e == "trade":  # 实时成交信息
            trade = {
                "platform": self._platform,
                "symbol": symbol,
                "action":  ORDER_ACTION_SELL if data["m"] else ORDER_ACTION_BUY,
                "price": data.get("p"),
                "quantity": data.get("q"),
                "timestamp": data.get("T")
            }
            logger.info("symbol:", symbol, "trade:", trade, caller=self)
        else:
            logger.error("event error! msg:", msg, caller=self)

    def _symbol_to_channel(self, symbol, channel_type="ticker"):
        """ symbol转换到channel
        @param symbol symbol名字
        @param channel_type 频道类型 kline K线 / ticker 行情
        """
        channel = "{x}@{y}".format(x=symbol.replace("/", "").lower(), y=channel_type)
        self._c_to_s[channel] = symbol
        return channel


if __name__ == '__main__':
    binance_websocket = Binance(to_wechat_id='文件传输助手')
    loop = asyncio.get_event_loop()
    # loop.call_later(2, heartbeat.ticker)
    loop.run_forever()