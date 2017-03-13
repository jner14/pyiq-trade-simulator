# coding=utf-8

import iqfeed as iq
import numpy as np
import sys
import logging
from time import sleep
from pytz import timezone
from datetime import datetime, timedelta, time
from pandas import DataFrame, read_csv, Series, concat
from localconfig import dtn_product_id, dtn_login, dtn_password
from multiprocessing import Queue
from matplotlib import style as mplStyle, animation, pyplot as plt, dates as mdates, ticker as mticker
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
from matplotlib.finance import candlestick_ohlc
import threading


TIMEZONE = timezone('US/Eastern')
TIME_DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
TIME_FORMAT = '%H:%M:%S.%f'
DATE_FORMAT = '%Y-%m-%d'
LISTEN_LABELS = ['Symbol', 'Last', 'Bid', 'Ask', 'Tick', 'Size', 'Datetime', 'Open', 'Close',
                 'High', 'Low', 'UpTicks', 'DownTicks', 'TotalTicks', 'UpVol', 'DownVol', 'TotalVol']
UPDATES_LABELS = ['Symbol', 'Last', 'Bid', 'Ask', 'Size', 'Datetime', 'Open', 'High', 'Low',
                  'Close', 'UpVol', 'DownVol', 'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks']
mplStyle.use('dark_background')
lgr = logging.getLogger('simulator')
lgr.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = logging.FileHandler("simulator.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(FORMATTER)
lgr.addHandler(fh)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(FORMATTER)
lgr.addHandler(ch)


class HandyListener(iq.VerboseQuoteListener):

    def __init__(self, name: str, queue: DataFrame, lock: threading.Lock, output_type: str='queue'):
        super().__init__(name)
        self._lock = lock
        self.queue = queue
        self.output_type = output_type  # 'queue', 'console'
        self.lgr = logging.getLogger("HandyListener")
        self.lgr.setLevel(logging.DEBUG)
        fh2 = logging.FileHandler("simulator_iqfeed_updates.log")
        fh2.setLevel(logging.DEBUG)
        fh2.setFormatter(FORMATTER)
        self.lgr.addHandler(fh2)

    def process_update(self, update: np.array) -> None:
        assert len(update) == 1, "Received multiple updates. This is unexpected."
        update = update[0]
        data = Series(list(update) + [0] * 10, index=LISTEN_LABELS)

        # If trade occurs at ask, then add volume to UpVolume
        if data.Last == data.Ask:
            data.UpVol += data.Size
        # If trade occurs at bid, then add volume to DownVolume
        elif data.Last == data.Bid:
            data.DownVol -= data.Size

        # If Tick direction is 1, add to UpTicks
        if data.Tick == 1:
            data.UpTicks += 1
        # If Tick direction is -1, add to DownTicks
        elif data.Tick == -1:
            data.DownTicks -= 1

        # Every tick is added to TotalTicks
        data.TotalTicks = abs(data.Tick)
        data.TotalVol = abs(data.Size)

        # Open, Close, High, and Low are set to trade price in preparation for re-sampling into minute bars
        data.loc[['Open', 'Close', 'High', 'Low']] = data.Last

        # Configure datetime
        date = datetime.now(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        data.Datetime = date + timedelta(microseconds=int(data.Datetime))

        # Add update data to debug output
        t1 = data.Datetime.strftime(TIME_DATE_FORMAT)
        debug_labels = UPDATES_LABELS.copy()
        debug_labels.remove("Datetime")
        update_debug_str = "{}, {}, {}".format(t1, data.loc[debug_labels].values, update[6])
        self.lgr.debug("qsize: {}, update: {}".format(len(self.queue), update_debug_str))

        # Place update data in queue for retrieval from simulator
        # self.queue.put(data.loc[UPDATES_LABELS])
        with self._lock:
            i = 0 if len(self.queue) == 0 else self.queue.index[-1] + 1
            self.queue.loc[i] = data.loc[UPDATES_LABELS]


class Simulator(object):
    FIELD_NAMES = ['Symbol', 'Last', 'Bid', 'Ask', 'Tick', 'Last Size', 'Last Time']
    MARKET_OPEN = time(hour=8)
    MARKET_CLOSE = time(hour=16)

    def __init__(self, ticker: str, stop: float, target: float, signal_func_a, signal_func_b,
                 bar_cnt: int, days_back: int=1, backtest: bool=False, offline: bool=False):
        self.ticker = ticker
        self.stop = stop
        self.target = target
        self.signal_func_a = signal_func_a
        self.signal_func_b = signal_func_b
        self.bar_cnt = bar_cnt
        self.daysBack = days_back
        self.backtest = backtest
        self.offline = backtest
        self.backtest_period = 1
        self._bt_minutes = None
        self._watching = False
        self._queue = DataFrame(columns=UPDATES_LABELS)
        self.market_hours_only = True
        self.trades = DataFrame(columns=['Price', 'Type'])
        self._minute_bars = DataFrame(columns=['Open', 'High', 'Low', 'Close', 'UpVol', 'DownVol',
                                              'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks'])
        self._updates = DataFrame(columns=UPDATES_LABELS)
        self._received_updates = False
        self._connector = None
        self._quote_conn = None
        self._trade_listener = None
        lgr.info("Starting new session...\n"+"#"*90+"\n\n"+"#"*90)
        lgr.info("Market hours are set to 8AM - 4PM EST")
        self.load_minute_data()
        self.load_trades()
        self.chart_max_bars = 100
        self.chart_bar_width = .0004
        self._current_chart_time = None
        self._fig = plt.figure()
        self.bar_up_color = '#66f4f2'
        self.bar_down_color = '#7541d8'
        self.target_color = '#5CFF40'
        self.stop_color = '#FF4040'
        self._in_trade = False
        self._stop_price = 0.0
        self._target_price = 0.0
        self.charting_enabled = True
        self._lastChartX = None
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self._lock = threading.Lock()

    def start(self):

        # Chart animation code here
        if self.charting_enabled:
            self.thread.start()
            while True:
                self._update_chart()
        else:
            self._run()

    def _run(self):

        if not self.backtest:
            self.wait_market_hours()

            Simulator._launch_service()
            self._quote_conn = iq.QuoteConn(name="Simulator-trades_only")
            self._trade_listener = HandyListener("Trades Listener", self._queue, self._lock)
            self._quote_conn.add_listener(self._trade_listener)

            try:
                self._connector = iq.ConnConnector([self._quote_conn])
                self._quote_conn.connect()
            except Exception as e:
                lgr.critical("Failed to connect to with quote_conn!")
                lgr.critical(e)
                sys.exit()

            self._quote_conn.select_update_fieldnames(Simulator.FIELD_NAMES)
            self._quote_conn.timestamp_off()
            self._quote_conn.trades_watch(self.ticker)
            self._watching = True
            sleep(4)

        if not self.offline:
            self._download_missing(self.daysBack)
            self._update_minute_bars()

        mySignals = {}
        while True:

            # Grab the most recent number of bars as necessary for signal generation
            bars = self.get_minute_bars(count=self.bar_cnt)
            last_close = bars[-1][5]

            # Calculate signals based on custom functions
            mySignals['a'] = self.signal_func_a(bars)
            mySignals['b'] = self.signal_func_b(bars)
            finalSignal = Simulator.get_final_signal(mySignals)

            # Simulate order actions
            if finalSignal == 1:
                # Limit buy
                filled = self.limit_buy(last_close)
                # If limit long was filled, create limit sell
                if filled:
                    self.limit_sell(last_close + self.target, last_close - self.stop)
            elif finalSignal == -1:
                # Limit short
                filled = self.limit_short(last_close)
                # If limit short was filled, create limit cover
                if filled:
                    self.limit_cover(last_close - self.target, last_close + self.stop)

            # Wait for close of bar
            self.wait_next_bar()

    def stop(self):
        if self._watching:
            self._quote_conn.unwatch(self.ticker)
            self._quote_conn.disconnect()
            self._watching = False
            lgr.info("Done watching for trades.")

    @staticmethod
    def get_final_signal(signals):
        # Process all signals to create a final signal

        # Ex. If all signals are positive, return 1 (buy signal)
        if sum(signals.values()) == len(signals):
            return 1
        # Ex. If all signals are negative, return -1 (sell signal)
        if sum(signals.values()) == -len(signals):
            return -1
        # Ex. Otherwise, return 0 (hold signal)
        return 0

    def get_minute_bars(self, count: int, as_dataframe: bool=False):
        """
        Returns: npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
        """
        lgr.debug("Getting minute bars. count={}".format(count))
        if self.backtest:
            # if self.charting_enabled:
            #     self._update_chart()
            if len(self._minute_bars) < count:
                for _ in range(count - len(self._minute_bars)):
                    self.wait_next_bar()

        if not self.offline:
            self._update_minute_bars()
            ts = datetime.now(TIMEZONE).replace(tzinfo=None)
            timeSLT = ts - self._minute_bars.index[-1] - timedelta(minutes=1)  # time since last bar closed
            if timeSLT > timedelta(minutes=5):
                lgr.warning("It has been {} day(s), {:.0f} hour(s), and {:.0f} minute(s) since the last close on record!".
                            format(timeSLT.days, timeSLT.seconds / 3600, timeSLT.seconds % 3600 / 60))

        if as_dataframe:
            return self._minute_bars.tail(count).copy()
        else:
            to_send = self._minute_bars.tail(count).copy()
            # to_send.insert(0, 'Time', [datetime.strftime(x, TIME_FORMAT) for x in to_send.index])
            # to_send.insert(0, 'Date', [datetime.strftime(x, DATE_FORMAT) for x in to_send.index])
            to_send.insert(0, 'Time', [x.time() for x in to_send.index])
            to_send.insert(0, 'Date', [x.date() for x in to_send.index])
            return to_send.values

    def _update_minute_bars(self):
        lgr.debug("Updating minute bars. len(updates)={}, len(minutes)={}".format(len(self._updates),
                                                                                  len(self._minute_bars)))
        ts = datetime.now(TIMEZONE).replace(tzinfo=None)
        self._get_updates()
        if self._updates is None or len(self._updates) == 0:
            lgr.debug("No longer updating minute bars because no new updates are available to create them")
            return

        updateMask = None
        if len(self._minute_bars) != 0:
            ltt = self._updates.iloc[-1].Datetime  # last trade time
            startTime = (self._minute_bars.index[-1] + timedelta(minutes=1))
            delta = ltt - startTime
            if delta < timedelta(minutes=1):
                lgr.debug("Less than a minute({}s) has passed since {} until {}".format(delta, startTime, ltt))
                lgr.debug("No longer updating minute bars")
                return
            end_time = ltt.replace(second=0, microsecond=0)
            lgr.debug("Filtering updates({}) by start={}, end={}".format(len(self._updates), startTime, end_time))
            updateMask = (startTime < self._updates.Datetime) & (self._updates.Datetime < end_time)
            # try:
            #     assert updateMask.iloc[0], "Updates are being filtered at start of list."
            # except Exception as e:
            #     print(e)
            #     sys.exit()
            toMinutes = self._updates.loc[updateMask, :].copy()

            if len(toMinutes) > 0:
                # Add last minute bar to beginning of updates so that when resampling no gaps are created
                m = self._minute_bars.iloc[-1]
                self._minute_bars = self._minute_bars.iloc[:-1]
                toMinutes.loc[(toMinutes.index[-1] + 1), :] = [0] * 16
                toMinutes = toMinutes.shift(1)
                toMinutes.iloc[0] = Series(["", .0, .0, .0, 0, m.name, m.Open, m.High, m.Low, m.Close, m.UpVol,
                                            m.DownVol, m.TotalVol, m.UpTicks, m.DownTicks, m.TotalTicks],
                                           index=UPDATES_LABELS)
        else:
            # lgr.debug("No minute da")
            toMinutes = self._updates

        lgr.debug("Updates To Minutes: {}\n{}".format(len(toMinutes), glimpse(toMinutes)))
        if len(toMinutes) < 1:
            lgr.debug("No longer updating minute bars due to a lack of data")
            return

        resampled = DataFrame()
        resampled['Open'] = toMinutes.loc[:, ['Open', 'Datetime']].resample(
            'T', on='Datetime').first().Open
        resampled['High'] = toMinutes.loc[:, ['High', 'Datetime']].resample(
            'T', on='Datetime').max().High
        resampled['Low'] = toMinutes.loc[:, ['Low', 'Datetime']].resample(
            'T', on='Datetime').min().Low
        resampled['Close'] = toMinutes.loc[:, ['Close', 'Datetime']].resample(
            'T', on='Datetime').last().Close
        rem_labels = ['UpVol', 'DownVol', 'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks']
        resampled[rem_labels] = \
            toMinutes.loc[:, rem_labels + ['Datetime']].resample('T', on='Datetime').sum().loc[:, rem_labels]

        lgr.debug("Filtering for trade halts and zero volume for over 15 minutes.")
        # Resample minute bars to 15 minutes
        min15 = resampled.resample('15T').first()
        # Create mask where rows are not null and not equal to zero
        nanMask1 = ~(min15.Open.isnull()) & (min15.TotalVol != 0)
        # Append a row to mask that matches last minute bar of resampled in prep for upsampling
        nanMask1.loc[resampled.index[-1]] = nanMask1.iloc[-1]
        # Upsample the mask to minutes and fill values forward
        nanMask1 = nanMask1.resample('T').ffill()
        # Filter mask to only include rows in resampled
        nanMask1 = nanMask1.loc[resampled.index[0]:, ]
        # Filter minute bars by mask, keeping only those that have activity within fifteen minute time span
        resampled = resampled[nanMask1]

        # Fill empty bars
        nanMask2 = resampled.Open.isnull()
        resampled.loc[:, 'Close'] = resampled.loc[:, 'Close'].ffill()
        resampled.loc[nanMask2, 'Open'] = resampled.loc[nanMask2, 'Close']
        resampled.loc[nanMask2, 'High'] = resampled.loc[nanMask2, 'Close']
        resampled.loc[nanMask2, 'Low'] = resampled.loc[nanMask2, 'Close']
        resampled.fillna(0, inplace=True)

        if updateMask is None:
            self._updates = self._updates[[False] * len(self._updates)]
        else:
            self._updates = self._updates[~updateMask]
        lgr.debug("Adding {} bars to minute data. updates={}\n{}".format(len(resampled),
                                                                         len(self._updates), glimpse(resampled)))
        self._minute_bars = concat([self._minute_bars, resampled])

        if not self.offline:
            self.save_minute_data()

    def _get_updates(self):
        lgr.debug("Getting updates. qsize={}, current updates={}".format(len(self._queue), len(self._updates)))
        if len(self._queue) > 0:
            self._received_updates = True
            with self._lock:
                self._updates = concat([self._updates, self._queue], ignore_index=True)
                self._queue.drop(self._queue.index, axis=0, inplace=True)
        else:
            self._received_updates = False

    def wait_next_bar(self):
        if self.backtest:
            # Grab next bar from self.bt_minutes and add to self.minutes
            if len(self._bt_minutes) != 0:
                self._minute_bars = self._minute_bars.append(self._bt_minutes.iloc[0])
                self._bt_minutes = self._bt_minutes.iloc[1:]
                if self.charting_enabled:
                    sleep(.1)
            else:
                lgr.info("Finished backtesting!")
                sys.exit()
        else:
            lastBarTime = self._minute_bars.index[-1]
            endTime = lastBarTime + timedelta(minutes=2)
            sinceLast = datetime.now()
            while lastBarTime == self._minute_bars.index[-1]:
                sleep(.1)
                self._update_minute_bars()
                now = datetime.now()
                if now.second % 5 == 0 and now - sinceLast >= timedelta(seconds=5):
                    sinceLast = now
                    currTime = datetime.now(TIMEZONE).replace(tzinfo=None)
                    to_wait = (endTime - currTime).total_seconds()
                    lgr.info("Waiting {} seconds for next bar. updates={}, qsize={}".format(round(to_wait),
                                                                                            len(self._updates),
                                                                                            len(self._queue)))

    def _buy(self, price):
        if self.backtest:
            dt = self._minute_bars.iloc[-1].name
        else:
            dt = datetime.now(TIMEZONE).replace(tzinfo=None)
        self.trades.loc[dt] = [rnd(price), 'long-entry']
        lgr.info("Long entry at {}".format(rnd(price)))
        self.save_trades()
        self._in_trade = True

    def limit_buy(self, price, delay=0.5, timeout=1):
        lgr.info("Waiting to enter long position at price={}".format(rnd(price)))
        startTime = self._minute_bars.index[-1] if self.backtest else datetime.now()
        filled = False
        while not filled:

            if self.backtest:
                self.wait_next_bar()

                # Check for target price
                close = self._minute_bars.iloc[-1].Close
                if price >= close:
                    self._buy(close)
                    filled = True
                    break
                elapsedTime = self._minute_bars.index[-1] - startTime

            else:
                # Get updates
                self._get_updates()

                if self._received_updates:

                    # Check for target price
                    for k, row in self._updates.iterrows():
                        if price >= row.Last:
                            self._buy(row.Last)
                            filled = True
                            break

                    log_args = rnd(row.Last), rnd(price), len(self._updates), len(self._queue)
                    self._update_minute_bars()

                    if filled:
                        break

                    lgr.info("Awaiting long entry: [current price={}, target={}, updates={}, qsize={}]".
                             format(*log_args))

                # Update minutes
                sleep(delay)
                elapsedTime = datetime.now() - startTime

            # Check if limit order has timed out due to being unfilled
            if elapsedTime >= timedelta(minutes=timeout):
                lgr.info("Limit long order timed out after {} minute(s).".format(timeout))
                break

        if filled:
            return True
        else:
            return False

    def _cover(self, price):
        if self.backtest:
            dt = self._minute_bars.iloc[-1].name
        else:
            dt = datetime.now(TIMEZONE).replace(tzinfo=None)
        self.trades.loc[dt] = [rnd(price), 'short-exit']
        lgr.info("Short exit at {}".format(rnd(price)))
        self.save_trades()
        self._in_trade = False

    def limit_cover(self, target, stop, delay=0.5):
        lgr.info("Waiting to exit short position at target={}, stop={}".format(rnd(target), rnd(stop)))
        self._stop_price = stop
        self._target_price = target
        while True:
            if self.backtest:
                # Get next bar()
                self.wait_next_bar()

                # Check for target or stop
                close = self._minute_bars.iloc[-1].Close
                if target >= close or close >= stop:
                    self._cover(close)
                    return
            else:
                # Get updates
                self._get_updates()

                if self._received_updates:

                    # Check for target or stop
                    for k, row in self._updates.iterrows():
                        if target >= row.Last or row.Last >= stop:
                            self._cover(row.Last)
                            return

                    log_args = rnd(row.Last), rnd(target), rnd(stop), len(self._updates), len(self._queue)
                    lgr.info("Awaiting short exit: [price={}, target={}, stop={}, updates={}, qsize={}]".
                             format(*log_args))

                    self._update_minute_bars()

                # Update minutes
                sleep(delay)

    def limit_sell(self, target, stop, delay=0.5):
        lgr.info("Waiting to exit long position at target={} ,stop={}".format(rnd(target), rnd(stop)))
        self._stop_price = stop
        self._target_price = target
        while True:
            if self.backtest:
                # Get next bar()
                self.wait_next_bar()

                # Check for target or stop
                close = self._minute_bars.iloc[-1].Close
                if stop >= close or close >= target:
                    self._sell(close)
                    return
            else:
                # Get updates
                self._get_updates()

                if self._received_updates:

                    # Check for target or stop
                    for k, row in self._updates.iterrows():
                        if stop >= row.Last or row.Last >= target:
                            self._sell(row.Last)
                            return

                    log_args = rnd(row.Last), rnd(target), rnd(stop), len(self._updates), len(self._queue)
                    lgr.info("Awaiting long exit: [price={}, target={}, stop={}, updates={}, qsize={}]".
                             format(*log_args))
                    self._update_minute_bars()

                # Update minutes
                sleep(delay)

    def _sell(self, price):
        if self.backtest:
            dt = self._minute_bars.iloc[-1].name
        else:
            dt = datetime.now(TIMEZONE).replace(tzinfo=None)
        self.trades.loc[dt] = [rnd(price), 'long-exit']
        lgr.info("Long exit at {}".format(rnd(price)))
        self.save_trades()
        self._in_trade = False

    def _short(self, price):
        if self.backtest:
            dt = self._minute_bars.iloc[-1].name
        else:
            dt = datetime.now(TIMEZONE).replace(tzinfo=None)
        self.trades.loc[dt] = [rnd(price), 'short-entry']
        lgr.info("Short entry at {}".format(rnd(price)))
        self.save_trades()
        self._in_trade = True

    def limit_short(self, price, delay=0.5, timeout=1):
        lgr.info("Waiting to enter short position at price={}".format(rnd(price)))
        startTime = self._minute_bars.index[-1] if self.backtest else datetime.now()
        filled = False
        while not filled:
            if self.backtest:
                self.wait_next_bar()

                # Check for target price
                close = self._minute_bars.iloc[-1].Close
                if price <= close:
                    self._short(close)
                    filled = True
                    break
                elapsedTime = self._minute_bars.index[-1] - startTime

            else:
                # Get updates
                self._get_updates()

                if self._received_updates:

                    # Check for target price
                    for k, row in self._updates.iterrows():
                        if price <= row.Last:
                            self._short(row.Last)
                            filled = True
                            break

                    log_args = rnd(row.Last), rnd(price), len(self._updates), len(self._queue)
                    self._update_minute_bars()

                    if filled:
                        break

                    lgr.info("Awaiting short entry: [current price={}, target={}, updates={}, qsize={}]".
                             format(*log_args))

                    self._update_minute_bars()

                # Update minutes
                sleep(delay)
                elapsedTime = datetime.now() - startTime

            # Check if limit order has timed out due to being unfilled
            if elapsedTime >= timedelta(minutes=timeout):
                lgr.info("Limit short order timed out after {} minute(s).".format(timeout))
                break

        if filled:
            return True
        else:
            return False

    def save_minute_data(self):
        # print("minutes.csv[", end='')
        if self._minute_bars is not None:
            if not self.backtest:
                # self.minutes.Datetime = self.minutes.Datetime.apply(lambda x: datetime.strftime(x, TIME_DATE_FORMAT))
                self._minute_bars.to_csv("minute_bars.csv")
                lgr.debug("Saved minute data to minute_bars.csv")
        else:
            lgr.error("Failed to save minute data, value=None")
        # print("finished.")

    def load_minute_data(self):
        try:
            if self.backtest:
                self._bt_minutes = read_csv("minute_bars.csv", index_col=0, parse_dates=True)
            else:
                self._minute_bars = read_csv("minute_bars.csv", index_col=0, parse_dates=True)
            lgr.info("Loaded minute_bars.csv")
        except Exception as e:
            lgr.error("Failed to load minute_bars.csv! {} ".format(e.args))

    def save_trades(self):
        if self.trades is not None:
            if self.backtest:
                self.trades.to_csv("backtest_trades.csv")
            else:
                self.trades.to_csv("trades.csv")
        else:
            lgr.error("Failed to save trades: value is None ")

    def load_trades(self):
        if not self.backtest:
            try:
                self.trades = read_csv("trades.csv", index_col=0, parse_dates=True)
                lgr.info("Loaded previous trades from trades.csv")
            except Exception as e:
                lgr.error("Failed to read trades.csv! {} ".format(e.args))

    def _download_missing(self, max_days: int=1):
        if self.offline:
            lgr.info("Simulator is in offline mode...")
            return
        if len(self._minute_bars) == 0:
            startTime = datetime.now(TIMEZONE).replace(tzinfo=None) - timedelta(days=max_days)
        else:
            startTime = self._minute_bars.index[-1] + timedelta(seconds=60)
            # startTime = startTime.tz_localize(TIMEZONE)

        # endTime = datetime.now(TIMEZONE).replace(tzinfo=None)
        self._queue.drop(self._queue.index, axis=0, inplace=True)
        self._updates = self.get_ticks_for_period(start=startTime, end=None)
        # What if as part of update minute bars we check to see if there is any gap between currently loaded minute bars
        # and the current time stamp.
        # If there is a gap, then we download tick data for the period starting after loaded minutes end up until now

    def get_ticks_for_period(self, start: datetime, end: datetime):
        """Return tick data for specified period."""

        if self.offline:
            lgr.info("Simulator is in offline mode...")
            return np.array([])
        if end is None:
            periodLength = datetime.now(TIMEZONE).replace(tzinfo=None) - start
        else:
            periodLength = end - start
        days, hours, minutes = (periodLength.days,
                                int(periodLength.seconds / 3600),
                                int(periodLength.seconds % 3600 / 60))
        if days > 5:
            ans = input("Request tick data for {} days?[y]/n: ".format(days))
            if ans.lower() not in ['', 'y', 'yes']:
                lgr.critical("Consider deleting minute_bars.csv and trying again!")
                sys.exit()

        lgr.info("Downloading tick data for {} day(s), {} hour(s) and {} minute(s). Please wait...".format(
            days, hours, minutes))

        hist_conn = iq.HistoryConn(name="iqfeed-get-ticks-for-period")
        hist_listener = iq.VerboseIQFeedListener("History Tick Listener")
        hist_conn.add_listener(hist_listener)

        with iq.ConnConnector([hist_conn]) as connector:
            try:
                # Get all ticks between start time and end time
                tick_data = hist_conn.request_ticks_in_period(ticker=self.ticker,
                                                              bgn_prd=start,
                                                              end_prd=end)

                lgr.info("Download finished. Calculating volume and tick data...")
                df = DataFrame(np.flipud(tick_data))
                df.drop(['cond1', 'cond2', 'cond3', 'cond4', 'mkt_ctr', 'last_type', 'tick_id', 'tot_vlm'], axis=1, inplace=True)
                df.columns = ['date', 'time', 'Last', 'Size', 'Bid', 'Ask']
                df['prev'] = [0] * len(df)
                df.loc[1:, 'prev'] = list(df.iloc[:-1]['Last'])
                df.loc[:, 'change'] = df['Last'] - df['prev']
                df['UpVol'] = [0] * len(df)
                df['DownVol'] = [0] * len(df)
                df.loc[:, 'UpVol'] = df.Size.astype(dtype='int') * ((df['Last'] == df.Ask) & (df.Bid != df.Ask))
                df.loc[:, 'DownVol'] = -1 * df.Size.astype(dtype='int') * ((df['Last'] == df.Bid) & (df.Bid != df.Ask))
                df.loc[:, 'TotalVol'] = df.UpVol + df.DownVol.abs()
                df['UpTicks'] = [0] * len(df)
                df['DownTicks'] = [0] * len(df)
                df.loc[:, 'UpTicks'] = 1 * (df.change > 0)
                df.loc[:, 'DownTicks'] = -1 * (df.change < 0)
                df.loc[:, 'TotalTicks'] = df.UpTicks + df.DownTicks.abs()
                df.insert(0, 'Datetime', df.date + df.time)
                df.insert(0, 'Symbol', self.ticker)
                df.drop(['date', 'time', 'prev', 'change'], axis=1, inplace=True)
                for label in ['Close', 'Low', 'High', 'Open']:
                    df.insert(2, label, df.Last)
                lgr.info("Done calculating new fields.")
                return df

            except (iq.NoDataError, iq.UnauthorizedError) as err:
                lgr.critical("No data returned because {0}".format(err))
                sys.exit()

    @staticmethod
    def _launch_service():
        """Check if IQFeed.exe is running and start if not"""

        svc = iq.FeedService(product=dtn_product_id,
                             version="Debugging",
                             login=dtn_login,
                             password=dtn_password)
        svc.launch()

        # If you are running headless comment out the line above and uncomment
        # the line below instead. This runs IQFeed.exe using the xvfb X Framebuffer
        # server since IQFeed.exe runs under wine and always wants to create a GUI
        # window.
        # svc.launch(headless=True)

    def set_market_hours(self, start_hour: int=8, start_minute: int=0, end_hour: int=16, end_minute: int=0):
        Simulator.MARKET_OPEN = time(hour=start_hour, minute=start_minute)
        Simulator.MARKET_CLOSE = time(hour=end_hour, minute=end_minute)
        lgr.info("Market hours have been changed to start={}, end={}".format(Simulator.MARKET_OPEN,
                                                                             Simulator.MARKET_CLOSE))

    def wait_market_hours(self):
        firstTime = True
        if self.market_hours_only:
            waiting = True
        else:
            waiting = False

        while waiting:
            currentTime = datetime.now(TIMEZONE).replace(tzinfo=None)
            if Simulator.MARKET_OPEN < currentTime.time() < Simulator.MARKET_CLOSE:
                waiting = False
                break
            if firstTime:
                if currentTime.time() < Simulator.MARKET_OPEN:
                    timeTillStart = datetime.combine(currentTime.date(), Simulator.MARKET_OPEN) - currentTime
                else:
                    timeTillStart = datetime.combine(currentTime.date(),
                                                     Simulator.MARKET_OPEN) + timedelta(hours=24) - currentTime
                h, m, s = (timeTillStart.seconds / 3600,
                           timeTillStart.seconds % 3600 / 60,
                           timeTillStart.seconds % 60)
                lgr.info("Waiting on market hours to begin. {:.0f} hr(s), {:.0f} min(s), {:.1f} sec(s)".format(h, m, s))
                firstTime = False
            sleep(1)

    def _update_chart(self):
        # self.ax1.clear()
        # self.ax2.clear()

        st = datetime.now()  # Debug code

        if len(self._minute_bars) == 0:
            return
        st = datetime.now()
        ohlc = self._minute_bars.ix[-self.chart_max_bars:, ['Open', 'High', 'Low', 'Close']]
        ohlc.insert(0, 'Time', ohlc.index)
        ohlc.Time = ohlc.Time.apply(lambda t: mdates.date2num(t))

        volume = self._minute_bars.ix[-self.chart_max_bars:, ['UpVol', 'DownVol']]
        volume.insert(0, 'Time', volume.index)
        volume.Time = volume.Time.apply(lambda t: mdates.date2num(t))

        # If there are feed updates, create a partial minute bar to add to end of chart
        with self._lock:
            updatesCopy = self._updates.copy()
        if len(updatesCopy) > 0:
            # Use the time of the last recorded minute bar as a reference
            lastMinute = self._minute_bars.index[-1]
            # If this is the first run or the last minute bar has caught up to the partial minute bar
            if self._current_chart_time is None or self._current_chart_time <= lastMinute:
                # Create a new partial minute bar with the latest feed updates
                self._current_chart_time = lastMinute + timedelta(minutes=1)
                self.currentChartOpen = updatesCopy.iloc[0].Open
                self.currentChartHigh = updatesCopy.High.max()
                self.currentChartLow = updatesCopy.Low.min()
                self.currentChartClose = updatesCopy.iloc[-1].Close
                self.currentUpVol = updatesCopy.UpVol.sum()
                self.currentDownVol = updatesCopy.DownVol.sum()
            # Otherwise, just update the current partial minute bar
            else:
                self.currentChartHigh = updatesCopy.High.max()
                self.currentChartLow = updatesCopy.Low.min()
                self.currentChartClose = updatesCopy.iloc[-1].Close
                self.currentUpVol = updatesCopy.UpVol.sum()
                self.currentDownVol = updatesCopy.DownVol.sum()

            ohlc.loc[self._current_chart_time] = [mdates.date2num(self._current_chart_time), self.currentChartOpen,
                                                  self.currentChartHigh, self.currentChartLow, self.currentChartClose]
            volume.loc[self._current_chart_time] = [mdates.date2num(self._current_chart_time), self.currentUpVol,
                                                    self.currentDownVol]

        self.ax1 = plt.subplot2grid((7, 1), (0, 0), rowspan=5, facecolor='#000000')
        candlestick_ohlc(self.ax1, ohlc.values, width=self.chart_bar_width,
                         colorup=self.bar_up_color, colordown=self.bar_down_color)

        # Draw target and stop indicators when in a trade
        bbox_props = dict(boxstyle="round", fc="black", ec="black", alpha=0.8, pad=.1)
        if self._in_trade and len(self.ax1.lines) >= 50:
            start_x = ohlc.ix[-50, 'Time']  # mdates.num2date(self.ax1.lines[-50]._x)
            end_x = ohlc.ix[-1, 'Time']  # mdates.num2date(self.ax1.lines[-1]._x)

            self.ax1.plot([start_x, end_x], [self._stop_price, self._stop_price],
                          color=self.stop_color, linewidth=1)
            self.ax1.text(start_x, self._stop_price, 'Stop={}'.format(rnd(self._stop_price)), ha="left",
                          va="bottom", bbox=bbox_props, color=self.stop_color, size=8)

            self.ax1.plot([start_x, end_x], [self._target_price, self._target_price],
                          color=self.target_color, linewidth=1)
            self.ax1.text(start_x, self._target_price, 'Target={}'.format(rnd(self._target_price)), ha="left",
                          va="bottom", bbox=bbox_props, color=self.target_color, size=8)

        # Draw trade entry and exit indicators
        trades = self.trades.loc[(self.trades.index > ohlc.index[0])]  # & (self.trades.index < ohlc.index[-1]))]
        for k, row in trades.iterrows():
            xy = (k, row.Price)
            color, marker = ('red', 'v') if row.Type in ['short-entry'] else ('green', '^')
            if row.Type in ['long-exit', 'short-exit']:
                self.ax1.plot(k, row.Price, marker='*', markerfacecolor='white', markersize=7, color='white')
            else:
                self.ax1.plot(k, row.Price, marker=marker, markerfacecolor=color, markersize=7, color=color)

        self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        self.ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
        self.ax1.grid(color='#232323', linestyle='dashed')
        self.ax1.set_axisbelow(True)
        plt.ylabel('Price')

        self.ax2 = plt.subplot2grid((7, 1), (5, 0), rowspan=2, facecolor='#000000', sharex=self.ax1)

        self.ax2.bar(volume.Time.values, volume.UpVol.values, self.chart_bar_width, color=self.bar_up_color)
        self.ax2.bar(volume.Time.values, volume.DownVol.values, self.chart_bar_width, color=self.bar_down_color)

        plt.xlabel('Time')
        plt.ylabel('Volume')
        plt.suptitle(self.ticker)
        plt.setp(self.ax1.get_xticklabels(), visible=False)
        plt.subplots_adjust(left=0.16, bottom=0.20, right=0.94, top=0.90, wspace=0.2, hspace=0)
        plt.pause(1e-7)
        # print(datetime.now() - st)


def glimpse(df: DataFrame, size: int=5):
    aGlimpse = None
    if len(df) > size * 2:
        aGlimpse = concat([df.head(size), df.tail(size)])
    elif len(df) <= size * 2:
        aGlimpse = df
    return aGlimpse


def rnd(val, n: int=7):
    return round(val, n)


if __name__ == "__main__":
    # TODO: Consider implementing backtesting with trade data not just minute data
    # For debugging code
    sim1 = Simulator("@JY#")
    while True:
        sim1._update_chart()
        plt.pause(1)
    pass
