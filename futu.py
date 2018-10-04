from __future__ import print_function
import futuquant as ft

import datetime as datetime
from futuquant import *
import threading
import yaml as yaml
import pymongo as pymongo

with open('config.yaml', 'r') as f:
    config = yaml.load(f)
    stocks = config["stocks"]
    stock_code_list = list(map(lambda x: x['symbol'], stocks))
    mongdb_connect = config["mongodb"]["host"]
    mongdb_dbname = config["mongodb"]["dbname"]
    mongodbclient = pymongo.MongoClient(mongdb_connect)
    stockdb = mongodbclient[mongdb_dbname]
    futud_host = config["futud"]["host"]
    futud_port = config["futud"]["port"]


class StockQuoteHandler(StockQuoteHandlerBase):

    def __init__(self, quote_ctx):
        StockQuoteHandlerBase.__init__(self)
        self._quote_ctx = quote_ctx
        return

    def on_recv_rsp(self, rsp_str):
        ret_code, data = super(StockQuoteHandler, self).on_recv_rsp(rsp_str)
        if ret_code != RET_OK:
            print("StockQuoteTest: error, msg: %s" % data)
            return RET_ERROR, data

        # fetch order book at the current time
        for row in data.itertuples():
            run_async(get_order_book, (self._quote_ctx, row.code))
            logger.info(row)

        # save data to db
        current_time = datetime.now()
        col_current_time = [current_time for _ in range(data.shape[0])]
        data = data.assign(current_time=col_current_time)
        json_list = json.loads(data.to_json(orient="records"))
        stockdb["stock_quote"].insert_many(json_list)
        # logger.info(data.iloc[0, :])
        logger.info("StockQuoteTest---------------")
        # 1. call get_rt_ticker
        # 2. call get_order_book
        return RET_OK, data


class RTDataHandler(RTDataHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, data = super(RTDataHandler, self).on_recv_rsp(rsp_str)
        if ret_code != RET_OK:
            print("RTDataTest: error, msg: %s" % data)
            return RET_ERROR, data

        # print("RTDataTest ", data)  # RTDataTest自己的处理逻辑
        # print(data.iloc[0, :])
        print("RTDataTest---------------")
        # 1. call get_rt_ticker
        # 2. call get_order_book
        return RET_OK, data


def get_rt_ticker(quote_ctx, code, num=500):
    time.sleep(3)
    print("get_rt_ticker:", code, num)
    return


def get_order_book(quote_ctx, code):
    # time.sleep(3)
    error, data = quote_ctx.get_order_book(code)
    current_time = datetime.now()
    data["current_time"] = current_time
    stockdb["order_book"].insert(data)

    logger.info(data)
    return


def run_async(fn, args):
    t = threading.Thread(target=fn, args=args)
    t.start()
    return

def main():
    quote_ctx = ft.OpenQuoteContext(host=futud_host, port=futud_port)
    # ret, data = quote_ctx.get_market_snapshot('HK.00700')
    # data.to_csv("tmp_0700.csv", index=False)
    # print(data.iloc[:, 0:4])

    ret_subscribe = quote_ctx.subscribe(stock_code_list, [
        SubType.QUOTE, SubType.TICKER, SubType.BROKER, SubType.ORDER_BOOK, SubType.RT_DATA
    ])
    print(ret_subscribe)
    quote_ctx.start()

    stock_quote_handler = StockQuoteHandler(quote_ctx)
    quote_ctx.set_handler(stock_quote_handler)
    # rt_data_handler = RTDataHandler()
    # quote_ctx.set_handler(rt_data_handler)

    print("...before sleeping")
    time.sleep(60 * 60)
    print("...end sleeping")

    quote_ctx.close()

    return


if __name__ == '__main__':
    main()
