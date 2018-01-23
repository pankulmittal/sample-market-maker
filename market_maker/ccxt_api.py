from __future__ import absolute_import

import random
import json
import decimal
import requests
import base64
import uuid
import sys

from time import sleep
from ccxt import bitmex
from datetime import datetime
from os.path import getmtime

from market_maker.utils import log, constants, errors, math
from market_maker.utils.math import toNearest
from future.utils import iteritems
from market_maker.settings import settings

# Used for reloading the bot - saves modified times of key files
import os
watched_files_mtimes = [(f, getmtime(f)) for f in settings.WATCHED_FILES]

logger = log.setup_custom_logger('root')


def XBt_to_XBT(XBt):
    return float(XBt) / constants.XBt_TO_XBT


class BitMEX(bitmex):

    def __init__(self, *args, **kwargs):
        super(BitMEX, self).__init__(*args, **kwargs)
        self.urls['api'] = "https://testnet.bitmex.com"
        self.urls['www'] = "https://testnet.bitmex.com"
        self.apiKey = settings.API_KEY
        self.secret = settings.API_SECRET
        self.symbol = settings.SYMBOL
        self.symbol_code = settings.SYMBOL_CODE
        self.dry_run = settings.DRY_RUN
        self.orderIDPrefix = kwargs["orderIDPrefix"] if "orderIDPrefix" in kwargs else "mm_bitmex_"
        self.postOnly = kwargs['postOnly'] if kwargs.get("postOnly") else False

    def init(self):
        if settings.DRY_RUN:
            logger.info("Initializing dry run. Orders printed below represent what would be posted to BitMEX.")
        else:
            logger.info("Order Manager initializing, connecting to BitMEX. Live run: executing real trades.")

        self.start_time = datetime.now()
        self.market = self.fetch_market()
        self.starting_qty = self.get_delta()
        self.running_qty = self.starting_qty
        self.reset()

    def reset(self):
        self.cancel_all_orders()
        self.sanity_check()
        self.print_status()

        # Create orders and converge.
        self.place_orders()

        if settings.DRY_RUN:
            sys.exit()

    def sign(self, path, api='public', method='GET', params={}, headers=None, body=None):
        query = '/api' + '/' + self.version + '/' + path
        if method != 'PUT':
            if params:
                query += '?' + self.urlencode(params)
        url = self.urls['api'] + query
        if api == 'private':
            self.check_required_credentials()
            nonce = str(self.nonce())
            auth = method + query + nonce
            if method in ['POST', 'PUT', "DELETE"]:
                if params:
                    auth += self.json(params)
                if body:
                    auth += self.encode(body)
            headers = {
                'Content-Type': 'application/json',
                'api-nonce': nonce,
                'api-key': self.apiKey,
                'api-signature': self.hmac(self.encode(auth), self.encode(self.secret)),
            }
        return {'url': url, 'method': method, 'body': body, 'headers': headers}

    def get_positions(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.request("position", api="private")

    def get_position(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        positions = self.get_positions(symbol)
        pos = [p for p in positions if p['symbol'] == symbol]
        if len(pos) == 0:
            # No position found; stub it
            return {'avgCostPrice': 0, 'avgEntryPrice': 0, 'currentQty': 0, 'symbol': symbol}
        return pos[0]

    def get_delta(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.get_position(symbol)['currentQty']

    def fetch_orders(self, symbol=None, since=None, limit=None, params={}):
        if not symbol:
            symbol = self.symbol
        if not since:
            since = 0
        if not limit:
            limit = since + 100
        return self.request("order", api="private", params={"start": since, "count": limit})

    def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}):
        if not symbol:
            symbol = self.symbol
        if not since:
            since = 0
        if not limit:
            limit = since + 100
        orders = self.fetch_orders(symbol, since, limit)
        return [o for o in orders if str(o['clOrdID']).startswith(self.orderIDPrefix) and o['leavesQty'] > 0]

    def get_highest_buy(self):
        buys = [o for o in self.fetch_open_orders() if o['side'] == 'Buy']
        if not len(buys):
            return {'price': -2**32}
        highest_buy = max(buys or [], key=lambda o: o['price'])
        return highest_buy if highest_buy else {'price': -2**32}

    def get_lowest_sell(self):
        sells = [o for o in self.fetch_open_orders() if o['side'] == 'Sell']
        if not len(sells):
            return {'price': 2**32}
        lowest_sell = min(sells or [], key=lambda o: o['price'])
        return lowest_sell if lowest_sell else {'price': 2**32}  # ought to be enough for anyone

    def fetch_market(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        markets = self.fetch_markets()
        market = [i for i in markets if i["symbol"] == self.symbol]
        if len(market) == 0:
            raise Exception("Unable to find market or index with symbol: " + symbol)
        market = market[0]
        market['info']['tickLog'] = decimal.Decimal(str(market['info']['tickSize'])).as_tuple().exponent * -1
        return market

    def check_market_open(self):
        market = self.fetch_market()
        if market["info"]["state"] != "Open" and market["info"]["state"] != "Closed":
            raise errors.MarketClosedError("The market %s is not open. State: %s" %
                                           (self.symbol, market["info"]["state"]))

            # Ensure market is still open.
            self.check_market_open()

    def check_if_orderbook_empty(self):
        """This function checks whether the order book is empty"""
        market = self.fetch_market()
        if market['info']['midPrice'] is None:
            raise errors.MarketEmptyError("Orderbook is empty, cannot quote")


    def ticker_data(self, symbol=None):
        '''Return a ticker object. Generated from market.'''

        if symbol is None:
            symbol = self.symbol

        market = self.fetch_market()

        # If this is an index, we have to get the data from the last trade.
        if market['info']['symbol'][0] == '.':
            ticker = {}
            ticker['mid'] = ticker['buy'] = ticker['sell'] = ticker['last'] = market["info"]['markPrice']
        # Normal market info
        else:
            bid = market['info']['bidPrice'] or market['info']['lastPrice']
            ask = market['info']['askPrice'] or market['info']['lastPrice']
            ticker = {
                "last": market['info']['lastPrice'],
                "buy": bid,
                "sell": ask,
                "mid": (bid + ask) / 2
            }

        # The market info has a tickSize. Use it to round values.
        return {k: toNearest(float(v or 0), market['info']['tickSize']) for k, v in iteritems(ticker)}

    def get_ticker(self):
        ticker = self.ticker_data()
        tickLog = self.fetch_market()['info']['tickLog']

        # Set up our buy & sell positions as the smallest possible unit above and below the current spread
        # and we'll work out from there. That way we always have the best price but we don't kill wide
        # and potentially profitable spreads.
        self.start_position_buy = ticker["buy"] + self.market['info']['tickSize']
        self.start_position_sell = ticker["sell"] - self.market['info']['tickSize']

        # If we're maintaining spreads and we already have orders in place,
        # make sure they're not ours. If they are, we need to adjust, otherwise we'll
        # just work the orders inward until they collide.
        if settings.MAINTAIN_SPREADS:
            if ticker['buy'] == self.get_highest_buy()['price']:
                self.start_position_buy = ticker["buy"]
            if ticker['sell'] == self.get_lowest_sell()['price']:
                self.start_position_sell = ticker["sell"]

        # Back off if our spread is too small.
        if self.start_position_buy * (1.00 + settings.MIN_SPREAD) > self.start_position_sell:
            self.start_position_buy *= (1.00 - (settings.MIN_SPREAD / 2))
            self.start_position_sell *= (1.00 + (settings.MIN_SPREAD / 2))

        # Midpoint, used for simpler order placement.
        self.start_position_mid = ticker["mid"]
        logger.info(
            "%s Ticker: Buy: %.*f, Sell: %.*f" %
            (self.market["info"]['symbol'], tickLog, ticker["buy"], tickLog, ticker["sell"])
        )
        logger.info('Start Positions: Buy: %.*f, Sell: %.*f, Mid: %.*f' %
                    (tickLog, self.start_position_buy, tickLog, self.start_position_sell,
                     tickLog, self.start_position_mid))
        return ticker

    def get_price_offset(self, index):
        """Given an index (1, -1, 2, -2, etc.) return the price for that side of the book.
           Negative is a buy, positive is a sell."""
        # Maintain existing spreads for max profit
        if settings.MAINTAIN_SPREADS:
            start_position = self.start_position_buy if index < 0 else self.start_position_sell
            # First positions (index 1, -1) should start right at start_position, others should branch from there
            index = index + 1 if index < 0 else index - 1
        else:
            # Offset mode: ticker comes from a reference exchange and we define an offset.
            start_position = self.start_position_buy if index < 0 else self.start_position_sell

            # If we're attempting to sell, but our sell price is actually lower than the buy,
            # move over to the sell side.
            if index > 0 and start_position < self.start_position_buy:
                start_position = self.start_position_sell
            # Same for buys.
            if index < 0 and start_position > self.start_position_sell:
                start_position = self.start_position_buy

        return math.toNearest(start_position * (1 + settings.INTERVAL) ** index, self.market['info']['tickSize'])

    def short_position_limit_exceeded(self):
        "Returns True if the short position limit is exceeded"
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.get_delta()
        return position <= settings.MIN_POSITION

    def long_position_limit_exceeded(self):
        "Returns True if the long position limit is exceeded"
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.get_delta()
        return position >= settings.MAX_POSITION

    def sanity_check(self):
        """Perform checks before placing orders."""

        # Check if OB is empty - if so, can't quote.
        self.check_if_orderbook_empty()

        # Ensure market is still open.
        self.check_market_open()

        # Get ticker, which sets price offsets and prints some debugging info.
        ticker = self.get_ticker()

        # Sanity check:
        if self.get_price_offset(-1) >= ticker["sell"] or self.get_price_offset(1) <= ticker["buy"]:
            logger.error("Buy: %s, Sell: %s" % (self.start_position_buy, self.start_position_sell))
            logger.error("First buy position: %s\nBitMEX Best Ask: %s\nFirst sell position: %s\nBitMEX Best Bid: %s" %
                         (self.get_price_offset(-1), ticker["sell"], self.get_price_offset(1), ticker["buy"]))
            logger.error("Sanity check failed, exchange data is inconsistent")
            self.exit()

        # Messanging if the position limits are reached
        if self.long_position_limit_exceeded():
            logger.info("Long delta limit exceeded")
            logger.info("Current Position: %.f, Maximum Position: %.f" %
                        (self.get_delta(), settings.MAX_POSITION))

        if self.short_position_limit_exceeded():
            logger.info("Short delta limit exceeded")
            logger.info("Current Position: %.f, Minimum Position: %.f" %
                        (self.get_delta(), settings.MIN_POSITION))

    def get_portfolio(self):
        contracts = settings.CONTRACTS
        portfolio = {}
        for symbol in contracts:
            position = self.get_position(symbol=symbol)

            if self.market['info']['isQuanto']:
                future_type = "Quanto"
            elif self.market['info']['isInverse']:
                future_type = "Inverse"
            elif not self.market['info']['isQuanto'] and not self.market['info']['isInverse']:
                future_type = "Linear"
            else:
                raise NotImplementedError("Unknown future type; not quanto or inverse: %s" % self.market['info']['symbol'])

            if self.market['info']['underlyingToSettleMultiplier'] is None:
                multiplier = float(self.market['info']['multiplier']) / float(self.market['info']['quoteToSettleMultiplier'])
            else:
                multiplier = float(self.market['info']['multiplier']) / float(self.market['info']['underlyingToSettleMultiplier'])

            portfolio[symbol] = {
                "currentQty": float(position['currentQty']),
                "futureType": future_type,
                "multiplier": multiplier,
                "markPrice": float(market['info']['markPrice']),
                "spot": float(market['info']['indicativeSettlePrice'])
            }

        return portfolio

    def calc_delta(self):
        """Calculate currency delta for portfolio"""
        portfolio = self.get_portfolio()
        spot_delta = 0
        mark_delta = 0
        for symbol in portfolio:
            item = portfolio[symbol]
            if item['futureType'] == "Quanto":
                spot_delta += item['currentQty'] * item['multiplier'] * item['spot']
                mark_delta += item['currentQty'] * item['multiplier'] * item['markPrice']
            elif item['futureType'] == "Inverse":
                spot_delta += (item['multiplier'] / item['spot']) * item['currentQty']
                mark_delta += (item['multiplier'] / item['markPrice']) * item['currentQty']
            elif item['futureType'] == "Linear":
                spot_delta += item['multiplier'] * item['currentQty']
                mark_delta += item['multiplier'] * item['currentQty']
        basis_delta = mark_delta - spot_delta
        delta = {
            "spot": spot_delta,
            "mark_price": mark_delta,
            "basis": basis_delta
        }
        return delta

    def print_status(self):
        """Print the current MM status."""

        margin = self.fetch_balance()
        position = self.get_position()
        self.running_qty = self.get_delta()
        tickLog = self.fetch_market()['info']['tickLog']
        self.start_XBt = margin['info'][0]["marginBalance"]

        logger.info("Current XBT Balance: %.6f" % XBt_to_XBT(self.start_XBt))
        logger.info("Current Contract Position: %d" % self.running_qty)
        if settings.CHECK_POSITION_LIMITS:
            logger.info("Position limits: %d/%d" % (settings.MIN_POSITION, settings.MAX_POSITION))
        if position['currentQty'] != 0:
            logger.info("Avg Cost Price: %.*f" % (tickLog, float(position['avgCostPrice'])))
            logger.info("Avg Entry Price: %.*f" % (tickLog, float(position['avgEntryPrice'])))
        logger.info("Contracts Traded This Run: %d" % (self.running_qty - self.starting_qty))
        logger.info("Total Contract Delta: %.4f XBT" % self.calc_delta()['spot'])

    def place_orders(self):
        """Create order items for use in convergence."""

        buy_orders = []
        sell_orders = []
        # Create orders from the outside in. This is intentional - let's say the inner order gets taken;
        # then we match orders from the outside in, ensuring the fewest number of orders are amended and only
        # a new order is created in the inside. If we did it inside-out, all orders would be amended
        # down and a new order would be created at the outside.
        for i in reversed(range(1, settings.ORDER_PAIRS + 1)):
            if not self.long_position_limit_exceeded():
                buy_orders.append(self.prepare_order(-i))
            if not self.short_position_limit_exceeded():
                sell_orders.append(self.prepare_order(i))

        return self.converge_orders(buy_orders, sell_orders)

    def amend_bulk_orders(self, orders):
        """Amend multiple orders."""
        # Note rethrow; if this fails, we want to catch it and re-tick
        data = {"orders": orders}
        return self.request("order/bulk", api="private", method="PUT", body=json.dumps(data))

    def create_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        for order in orders:
            order['clOrdID'] = self.orderIDPrefix + base64.b64encode(uuid.uuid4().bytes).decode('utf8').rstrip('=\n')
            order['symbol'] = self.symbol_code
            if self.postOnly:
                order['execInst'] = 'ParticipateDoNotInitiate'
        data = {"orders": orders}
        return self.request("order/bulk", api="private", method="POST", body=json.dumps(data))

    def cancel_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        orderID = [order['orderID'] for order in orders]
        data = {"orderID": orderID}
        return self.request("order", api="private", method="DELETE", body=json.dumps(data))

    def cancel_all_orders(self):
        if self.dry_run:
            return

        if self.symbol:
            data = {'symbol': self.symbol_code}
            self.request("order/all", api="private", method="DELETE", body=json.dumps(data))
        else:
            self.request("order/all", api="private", method="DELETE")

        sleep(settings.API_REST_INTERVAL)

    def prepare_order(self, index):
        """Create an order object."""

        if settings.RANDOM_ORDER_SIZE is True:
            quantity = random.randint(settings.MIN_ORDER_SIZE, settings.MAX_ORDER_SIZE)
        else:
            quantity = settings.ORDER_START_SIZE + ((abs(index) - 1) * settings.ORDER_STEP_SIZE)

        price = self.get_price_offset(index)

        return {'price': price, 'orderQty': quantity, 'side': "Buy" if index < 0 else "Sell"}

    def converge_orders(self, buy_orders, sell_orders):
        """Converge the orders we currently have in the book with what we want to be in the book.
           This involves amending any open orders and creating new ones if any have filled completely.
           We start from the closest orders outward."""

        tickLog = self.market['info']['tickLog']
        to_amend = []
        to_create = []
        to_cancel = []
        buys_matched = 0
        sells_matched = 0
        existing_orders = self.fetch_open_orders()

        # Check all existing orders and match them up with what we want to place.
        # If there's an open one, we might be able to amend it to fit what we want.
        for order in existing_orders:
            try:
                if order['side'] == 'Buy':
                    desired_order = buy_orders[buys_matched]
                    buys_matched += 1
                else:
                    desired_order = sell_orders[sells_matched]
                    sells_matched += 1

                # Found an existing order. Do we need to amend it?
                if desired_order['orderQty'] != order['leavesQty'] or (
                        # If price has changed, and the change is more than our RELIST_INTERVAL, amend.
                        desired_order['price'] != order['price'] and
                        abs((desired_order['price'] / order['price']) - 1) > settings.RELIST_INTERVAL):
                    to_amend.append({'orderID': order['orderID'], 'orderQty': order['cumQty'] + desired_order['orderQty'],
                                     'price': desired_order['price'], 'side': order['side']})
            except IndexError:
                # Will throw if there isn't a desired order to match. In that case, cancel it.
                to_cancel.append(order)

        while buys_matched < len(buy_orders):
            to_create.append(buy_orders[buys_matched])
            buys_matched += 1

        while sells_matched < len(sell_orders):
            to_create.append(sell_orders[sells_matched])
            sells_matched += 1

        if len(to_amend) > 0:
            for amended_order in reversed(to_amend):
                reference_order = [o for o in existing_orders if o['orderID'] == amended_order['orderID']][0]
                logger.info("Amending %4s: %d @ %.*f to %d @ %.*f (%+.*f)" % (
                    amended_order['side'],
                    reference_order['leavesQty'], tickLog, reference_order['price'],
                    (amended_order['orderQty'] - reference_order['cumQty']), tickLog, amended_order['price'],
                    tickLog, (amended_order['price'] - reference_order['price'])
                ))
            # This can fail if an order has closed in the time we were processing.
            # The API will send us `invalid ordStatus`, which means that the order's status (Filled/Canceled)
            # made it not amendable.
            # If that happens, we need to catch it and re-tick.
            try:
                self.amend_bulk_orders(to_amend)
            except requests.exceptions.HTTPError as e:
                errorObj = e.response.json()
                if errorObj['error']['message'] == 'Invalid ordStatus':
                    logger.warn("Amending failed. Waiting for order data to converge and retrying.")
                    sleep(0.5)
                    return self.place_orders()
                else:
                    logger.error("Unknown error on amend: %s. Exiting" % errorObj)
                    sys.exit(1)

        if len(to_create) > 0:
            logger.info("Creating %d orders:" % (len(to_create)))
            for order in reversed(to_create):
                logger.info("%4s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
            self.create_bulk_orders(to_create)

        # Could happen if we exceed a delta limit
        if len(to_cancel) > 0:
            logger.info("Canceling %d orders:" % (len(to_cancel)))
            for order in reversed(to_cancel):
                logger.info("%4s %d @ %.*f" % (order['side'], order['leavesQty'], tickLog, order['price']))
            self.cancel_bulk_orders(to_cancel)

    def check_file_change(self):
        """Restart if any files we're watching have changed."""
        for f, mtime in watched_files_mtimes:
            if getmtime(f) > mtime:
                self.restart()

    def exit(self):
        logger.info("Shutting down. All open orders will be cancelled.")
        try:
            self.cancel_all_orders()
        except errors.AuthenticationError as e:
            logger.info("Was not authenticated; could not cancel orders.")
        except Exception as e:
            logger.info("Unable to cancel orders: %s" % e)

        sys.exit()

    def restart(self):
        logger.info("Restarting the market maker...")
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def run_loop(self):
        while True:
            sys.stdout.write("-----\n")
            sys.stdout.flush()

            self.check_file_change()
            sleep(settings.LOOP_INTERVAL)

            self.reset()
            self.sanity_check()  # Ensures health of mm - several cut-out points here
            self.print_status()  # Print skew, delta, etc
            self.place_orders()  # Creates desired orders and converges to existing orders

def run():
    logger.info('BitMEX Market Maker Version: %s\n' % constants.VERSION)

    b = BitMEX()
    try:
        b.init()
        b.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
