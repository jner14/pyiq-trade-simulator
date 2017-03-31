# coding=utf-8

import iqfeed as iq
import numpy as np
import sys
import logging
from time import sleep
from pytz import timezone
from datetime import datetime, timedelta, time
from pandas import DataFrame, read_csv, read_sql_table, Series, concat, cut as pdcut
from localconfig import dtn_product_id, dtn_login, dtn_password
from matplotlib import style as mplStyle, pyplot as plt, dates as mdates, ticker as mticker
from matplotlib.finance import candlestick_ohlc
import threading
import sqlite3 as lite
import sqlalchemy


TIMEZONE = timezone('US/Eastern')
TIME_DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
TIME_FORMAT = '%H:%M:%S.%f'
DATE_FORMAT = '%Y-%m-%d'

LISTEN_LABELS = ['Symbol', 'Last', 'Bid', 'Ask', 'Tick', 'Size', 'Datetime', 'Open', 'Close',
                 'High', 'Low', 'UpTicks', 'DownTicks', 'TotalTicks', 'UpVol', 'DownVol', 'TotalVol']

UPDATES_LABELS = ['Symbol', 'Last', 'Bid', 'Ask', 'Size', 'Datetime', 'Open', 'High', 'Low',
                  'Close', 'UpVol', 'DownVol', 'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks']

TICK_LABELS = ['Datetime', 'Last', 'Bid', 'Ask', 'UpVol', 'DownVol', 'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks']

MINUTE_LABELS = ['Open', 'High', 'Low', 'Close', 'UpVol', 'DownVol', 'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks']

QUOTE_CONN_FIELDS = ['Symbol', 'Last', 'Bid', 'Ask', 'Tick', 'Last Size', 'Last Time']

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
        self._tick = Series(index=UPDATES_LABELS)
        # self.lgr = logging.getLogger("HandyListener")
        # self.lgr.setLevel(logging.DEBUG)
        # fh2 = logging.FileHandler("simulator_iqfeed_updates.log")
        # fh2.setLevel(logging.DEBUG)
        # fh2.setFormatter(FORMATTER)
        # self.lgr.addHandler(fh2)

    def process_update(self, update: np.array) -> None:
        assert len(update) == 1, "Received multiple updates. This is unexpected."
        data = Series(list(update[0]) + [0] * 10, index=LISTEN_LABELS)

        # If trade occurs at ask, then add volume to UpVolume
        if data.Last >= data.Ask:
            data.UpVol += data.Size
        # If trade occurs at bid, then add volume to DownVolume
        elif data.Last <= data.Bid:
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

        # If this is an new tick, add previous tick to queue and start new tick
        if data.Tick != 0:

            # Place tick in queue for retrieval from simulator
            if self._tick.isnull().sum() == 0:
                with self._lock:
                    i = 0 if len(self.queue) == 0 else self.queue.index[-1] + 1
                    self.queue.loc[i] = self._tick

            self._tick = data.loc[UPDATES_LABELS]

        # If this is not a new tick, add volume to current tick
        else:
            self._tick.UpVol += data.UpVol
            self._tick.DownVol += data.DownVol
            self._tick.TotalVol += data.TotalVol

        # Add update data to debug output
        # t1 = data.Datetime.strftime(TIME_DATE_FORMAT)
        # debug_labels = UPDATES_LABELS.copy()
        # debug_labels.remove("Datetime")
        # update_debug_str = "{}, {}, {}".format(t1, data.loc[debug_labels].values, update[6])
        # self.lgr.debug("qsize: {}, update: {}".format(len(self.queue), update_debug_str))


class Simulator(object):
    MARKET_OPEN = time(hour=8)
    MARKET_CLOSE = time(hour=16)

    def __init__(self,
                 ticker: str,
                 stop: float,
                 target: float,
                 signal_funcs=None,
                 bar_cnt: int=None,
                 days_back: int=1,
                 backtest: bool=False,
                 offline: bool=False):
        self.ticker = ticker
        self.stop = stop
        self.target = target
        self.signal_funcs = signal_funcs
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
        self._minute_bars = DataFrame(columns=MINUTE_LABELS)
        self._ticks = DataFrame(columns=TICK_LABELS)
        self._updates = DataFrame(columns=UPDATES_LABELS)
        self._received_updates = False
        self._connector = None
        self._quote_conn = None
        self._trade_listener = None
        lgr.info("Starting new session...\n"+"#"*90+"\n\n"+"#"*90)
        lgr.info("Market hours are set to 8AM - 4PM EST")
        self.db_con1 = sqlalchemy.create_engine("sqlite:///tick_data.sqlite3")
        self.load_tick_data()
        self._ticksSaved = False
        self._ticksDownloaded = False
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
        self.set_loop_func()
        self.set_final_signal_func()
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self._lock = threading.Lock()

    def start(self):

        if self.charting_enabled:
            self.thread.start()

            while not self._ticksDownloaded:
                lgr.info("Downloading tick data...")
                sleep(5)
                if not self.thread.is_alive():
                    sys.exit("Failed to download tick data!")

            # If we are appending tick data, filter out duplicate values
            ts = datetime.now()
            if len(self._minute_bars) > 0:
                msk2 = self._updates.Datetime > self._ticks.iloc[-1].Datetime
                # Append downloaded ticks memory and db
                self._ticks = concat([self._ticks, self._updates.loc[msk2, TICK_LABELS]], ignore_index=True)
                self._updates.loc[msk2, TICK_LABELS].to_sql(name=self.ticker, con=self.db_con1, if_exists='append', index=False)
            else:
                self._ticks = self._updates.loc[:, TICK_LABELS]
                self._updates.loc[:, TICK_LABELS].to_sql(name=self.ticker, con=self.db_con1, if_exists='append', index=False)

            lgr.info("Tick data saved, Rows={}, Time={}".format(len(self._updates), datetime.now() - ts))

            self._ticksSaved = True
            while True:
                self._update_chart()
        else:
            self._run()

    def _run(self):
        self.db_con2 = lite.connect("tick_data.sqlite3")

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

            self._quote_conn.select_update_fieldnames(QUOTE_CONN_FIELDS)
            self._quote_conn.timestamp_off()
            self._quote_conn.trades_watch(self.ticker)
            self._watching = True
            sleep(4)

        if not self.offline:
            self._download_missing(self.daysBack)
            self._ticksDownloaded = True
            while not self._ticksSaved:
                lgr.info("Storing tick data...")
                sleep(5)
            self._update_minute_bars()

        while True:
            self.loop_func(self)

    def set_loop_func(self, func=None):
        if func is not None:
            self.loop_func = func
        else:
            self.loop_func = self._default_loop_func

    def _default_loop_func(self):
        mySignals = {}

        # Grab the most recent number of bars as necessary for signal generation
        bars = self.get_minute_bars(count=self.bar_cnt)
        last_close = bars[-1][5]

        # Calculate signals based on custom functions
        for sig_func in self.signal_funcs:
            mySignals[sig_func.__name__] = sig_func(bars)
        finalSignal = self.get_final_signal(mySignals)

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

    def stop_iqfeed(self):
        if self._watching:
            self._quote_conn.unwatch(self.ticker)
            self._quote_conn.disconnect()
            self._watching = False
            lgr.info("Done watching for trades.")

    @staticmethod
    def _default_final_signal_func(signals):
        # Process all signals to create a final signal

        # Ex. If all signals are positive, return 1 (buy signal)
        if sum(signals.values()) == len(signals):
            return 1
        # Ex. If all signals are negative, return -1 (sell signal)
        if sum(signals.values()) == -len(signals):
            return -1
        # Ex. Otherwise, return 0 (hold signal)
        return 0

    def set_final_signal_func(self, func=None):
        if func is not None:
            self._final_signal_func = func
        else:
            self._final_signal_func = Simulator._default_final_signal_func

    def get_final_signal(self, signals):
        return self._final_signal_func(signals)

    def get_tick_range_bars(self, tick_range: int, tick_size: int, count: int=0,
                            span_seconds: int=0, as_dataframe: bool=False):
        """
        tick_range:   int,          the span of ticks for each bar
        tick_size:    int,          the currency value of one tick, e.g., .25 for S&P e-mini futures
        count:        int,          the number of bars returned
        time_seconds: int=0,        the time span in seconds
        as_dataframe: bool=False,   whether a pandas DataFrame should be returned instead of a numpy array

        *Note: both count and time_seconds can be passed, but the shorter of the two will be returned

        Returns: npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
        """

        lgr.info("Getting {}-tick range bars. count={}, tick_size={}, time_seconds={},".
                 format(tick_range, count, tick_size, span_seconds))

        # assert both count and time_seconds are not zero because one or both must be passed
        assert (count != 0 or span_seconds != 0), "must pass non-zero value for either count or time_seconds"

        # assert parameters passed are not less than zero
        assert (count >= 0 and span_seconds >= 0 and tick_size >= 0 and tick_range >= 0), "must pass positive values"

        # todo: consider implementing backtesting with ticks
        assert not self.backtest, "backtesting is not implemented for get_tick_bars"

        # Update minute bars and warn if there is a gap in minutes which could indicate feed issues
        if not self.offline:

            self._update_minute_bars()
            ts = datetime.now(TIMEZONE).replace(tzinfo=None)
            timeSLT = ts - self._minute_bars.index[-1] - timedelta(minutes=1)  # time since last bar closed

            if timeSLT > timedelta(minutes=5):
                lgr.warning('''It has been {} day(s), {:.0f} hour(s), and {:.0f} minute(s) since the last close on 
                            record!'''.format(timeSLT.days, timeSLT.seconds / 3600, timeSLT.seconds % 3600 / 60))

        # Get the reference point in time that is time_seconds back since last tick
        if span_seconds > 0:
            timePast = self._ticks.iloc[-1].Datetime - timedelta(seconds=span_seconds)
        else:
            timePast = 0

        # ticks = self._ticks.copy()
        # # TODO: use the first tick as a basis point for range, but how do we get a minimum
        # bins = np.arange(ticks.iloc[0].Last, ticks.Last.max(), tick_size * tick_range)
        # bIDs = DataFrame()
        # bIDs['bins'] = pdcut(ticks.Last, bins, labels=range(len(bins)-1))
        # bIDs['binsShift'] = bIDs.bins.shift()
        # msk = bIDs.bins != bIDs.binsShift
        # bIDs.loc[msk, 'bID'] = range(msk.sum())
        #
        # ticks['rangeID'] = bIDs
        # gTicks = ticks.groupby('rangeID')
        # bars = DataFrame()
        # bars['Datetime'] = gTicks.Datetime.first()
        # bars['Open'] = gTicks.Last.first()
        # bars['High'] = gTicks.Last.max()
        # bars['Low'] = gTicks.Last.min()
        # bars['Close'] = gTicks.Last.last()
        # bars['UpVol'] = gTicks.UpVol.sum()
        # bars['DownVol'] = gTicks.DownVol.sum()
        # bars['TotalVol'] = gTicks.TotalVol.sum()
        # bars['UpTicks'] = gTicks.UpTicks.sum()
        # bars['DownTicks'] = gTicks.DownTicks.sum()
        # bars['TotalTicks'] = gTicks.TotalTicks.sum()

        upVol, downVol, totalVol, upTicks, downTicks, totalTicks = [0]*6

        # Get first tick timestamp and use as basis for all tick range bars

        # 45.0, 1.25,

        # Iterate in reverse through ticks building bars along the way
        span = tick_size * tick_range
        close = None
        ticks = DataFrame(columns=['Datetime', 'Open', 'High', 'Low', 'Close', 'UpVol', 'DownVol', 'TotalVol',
                                   'UpTicks', 'DownTicks', 'TotalTicks'])
        for tid in reversed(self._ticks.index):
            thisTick = self._ticks.loc[tid]
            # Todo: fix issue where range bars are based off of last tick instead of first tick
            # Todo: fix issue where range bars start and end separated by tick size. Open = prevClose
            if close is None:
                close = thisTick.Last
                upVol, downVol, totalVol, upTicks, downTicks, totalTicks = [0]*6

            elif abs(thisTick.Last - close) >= span:
                locID = len(ticks)
                ticks.loc[locID, ['Datetime', 'Open', 'High', 'Low', 'Close', 'UpVol', 'DownVol', 'TotalVol', 'UpTicks',
                                  'DownTicks', 'TotalTicks']] = [thisTick.Datetime,
                                                                 thisTick.Last,
                                                                 max(thisTick.Last, close),
                                                                 min(thisTick.Last, close),
                                                                 close,
                                                                 upVol + thisTick.UpVol,
                                                                 downVol + thisTick.DownVol,
                                                                 totalVol + thisTick.TotalVol,
                                                                 upTicks + thisTick.UpTicks,
                                                                 downTicks + thisTick.DownTicks,
                                                                 totalTicks + thisTick.TotalTicks]
                close = None

                # Break from loop once bar count or time_seconds has been met
                if (count != 0 and len(ticks) >= count) or (timePast != 0 and timePast > thisTick.Datetime):
                    break

            else:
                upVol += thisTick.UpVol
                downVol += thisTick.DownVol
                totalVol += thisTick.TotalVol
                upTicks += thisTick.UpTicks
                downTicks += thisTick.DownTicks
                totalTicks += thisTick.TotalTicks

        # Reverse bars so Datetime is ascending
        ticks = ticks.iloc[::-1]

        lgr.info("End Tick Bar Range Creation...")

        # Return values as either DataFrame or numpy array
        if as_dataframe:
            return ticks
        else:
            ticks.insert(0, 'Time', [x.time() for x in ticks.Datetime])
            ticks.insert(0, 'Date', [x.date() for x in ticks.Datetime])
            return ticks.drop('Datetime', axis=1).values

    def get_tick_bars(self, period: int=5, count: int=0, time_seconds: int=0, as_dataframe: bool=False):
        """
        period:       int=5,      the period of tick bars returned
        count:        int=0,      the number of bars returned
        time_seconds: int=0,      the time span beginning from last tick in seconds
        as_dataframe: bool=False, whether a pandas DataFrame should be returned instead of a numpy array

        *Note: both count and time_seconds can be passed, but the shorter of the two will be returned

        Returns: npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
        """

        lgr.debug("Getting {}-tick bars. count={}, time_seconds={}".format(period, count, time_seconds))

        # assert both count and time_seconds are not zero because one or both must be passed
        assert (count != 0 or time_seconds != 0), "must pass non-zero value for either count or time_seconds"

        # assert parameters passed are not less than zero
        assert (count >= 0 and time_seconds >= 0 and period >= 0), "must pass positive values"

        # todo: consider implementing backtesting with ticks
        assert not self.backtest, "backtesting is not implemented for get_tick_bars"

        # Update minute count and warn if there is a gap in minutes which could indicate feed issues
        if not self.offline:

            self._update_minute_bars()
            ts = datetime.now(TIMEZONE).replace(tzinfo=None)
            timeSLT = ts - self._minute_bars.index[-1] - timedelta(minutes=1)  # time since last bar closed

            if timeSLT > timedelta(minutes=5):
                lgr.warning("It has been {} day(s), {:.0f} hour(s), and {:.0f} minute(s) since the last close on record!".
                            format(timeSLT.days, timeSLT.seconds / 3600, timeSLT.seconds % 3600 / 60))

        # Get the number of tick count to resample
        tickBarCnt1 = period * count
        tickBarCnt2 = 0

        if time_seconds > 0:

            # Get the reference point in time that is time_seconds back since last tick
            timePast = self._ticks.iloc[-1].Datetime - timedelta(seconds=time_seconds)

            # Get the number of ticks since timePast
            tickBarCnt2 = (self._ticks.Datetime > timePast).sum()

            # Reduce count to a divisible value of period
            tickBarCnt2 = tickBarCnt2 // period * period

        # Select the lowest tick bar count between time and count based
        if tickBarCnt1 == 0:
            tickBarCnt = tickBarCnt2
        elif tickBarCnt2 == 0:
            tickBarCnt = tickBarCnt1
        elif tickBarCnt1 < tickBarCnt2:
            tickBarCnt = tickBarCnt1
        elif tickBarCnt1 > tickBarCnt2:
            tickBarCnt = tickBarCnt2
        else:
            tickBarCnt = 0

        # Re-sample tick count into N tick count
        toResample = self._ticks.tail(tickBarCnt).copy()
        binCnt = len(toResample) // period
        assert binCnt > 0, "period={} is too high for given max parameters, count={}, time_span={}".\
            format(period, count, time_seconds)
        toResample['bins'] = pdcut(toResample.index, binCnt)
        toResample = toResample.groupby('bins')
        resampled = DataFrame()
        resampled['Datetime'] = toResample.first().Datetime
        resampled['Open']     = toResample.first().Last
        resampled['High']     = toResample.max().Last
        resampled['Low']      = toResample.min().Last
        resampled['Close']    = toResample.last().Last
        rem_labels = ['UpVol', 'DownVol', 'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks']
        resampled[rem_labels] = toResample.sum().loc[:, rem_labels]
        # resampled['Count']    = toResample.count().Datetime

        # Reindex to a simple range
        resampled.index = range(len(resampled))

        # Return values as either DataFrame or numpy array
        if as_dataframe:
            return resampled
        else:
            resampled.insert(0, 'Time', [x.time() for x in resampled.Datetime])
            resampled.insert(0, 'Date', [x.date() for x in resampled.Datetime])
            return resampled.drop('Datetime', axis=1).values

    def get_ticks(self, count: int=0, time_seconds: int=0, as_dataframe: bool=False):
        """
        count:        int=0,      the number of ticks returned
        time_seconds: int=0,      the max time span in seconds returned
        as_dataframe: bool=False, whether a pandas DataFrame should be returned instead of a numpy array

        *Note: both count and time_seconds can be passed, but the shorter of the two will be returned

        Returns: npArray[Date, Time, Last, Bid, Ask, Direction, UpVol, DownVol, TotalVol]
        """

        lgr.debug("Getting tick bars. count={}, time_seconds={}".format(count, time_seconds))

        # assert both max_bars and time_seconds are not zero
        assert (count != 0 or time_seconds != 0), "must pass non-zero value for either max_bars or time_seconds"

        # assert parameters passed are not less than zero
        assert (count >= 0 and time_seconds >= 0), "must pass positive values"

        # todo: consider implementing backtesting with count
        assert not self.backtest, "backtesting is not implemented for get_tick_bars"

        # Update minute bars and warn if there is a gap in minutes which could indicate feed issues
        if not self.offline:
            self._update_minute_bars()
            ts = datetime.now(TIMEZONE).replace(tzinfo=None)
            timeSLT = ts - self._minute_bars.index[-1] - timedelta(minutes=1)  # time since last bar closed
            if timeSLT > timedelta(minutes=5):
                lgr.warning("It's been {} day(s), {:.0f} hour(s), and {:.0f} minute(s) since the last close on record!".
                            format(timeSLT.days, timeSLT.seconds / 3600, timeSLT.seconds % 3600 / 60))

        # Get the number of tick bars to resample
        tickBarCnt1 = count
        tickBarCnt2 = 0
        if time_seconds > 0:
            # Get the reference point in time that is time_seconds back since last tick
            timePast = self._ticks.iloc[-1].Datetime - timedelta(seconds=time_seconds)
            # Get the number of count since timePast
            tickBarCnt2 = (self._ticks.Datetime > timePast).sum()

        # Select the lowest tick bar count between time_seconds and max_bars based
        if tickBarCnt1 == 0:
            tickBarCnt = tickBarCnt2
        elif tickBarCnt2 == 0:
            tickBarCnt = tickBarCnt1
        elif tickBarCnt1 < tickBarCnt2:
            tickBarCnt = tickBarCnt1
        elif tickBarCnt1 > tickBarCnt2:
            tickBarCnt = tickBarCnt2
        else:
            tickBarCnt = 0

        count = self._ticks.tail(tickBarCnt).copy()
        count.insert(4, 'Direction', count.UpTicks + count.DownTicks)
        count.drop(['UpTicks', 'DownTicks', 'TotalTicks'], axis=1, inplace=True)

        # Reindex to a simple range
        count.index = range(len(count))

        # Return values as either DataFrame or numpy array
        if as_dataframe:
            return count
        else:
            count.insert(0, 'Time', [x.time() for x in count.Datetime])
            count.insert(0, 'Date', [x.date() for x in count.Datetime])
            return count.drop('Datetime', axis=1).values

    # def get_minute_bars(self, count: int, as_dataframe: bool=False):
    #     """
    #     Returns: npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
    #     """
    #     lgr.debug("Getting minute bars. count={}".format(count))
    #     if self.backtest:
    #         # if self.charting_enabled:
    #         #     self._update_chart()
    #         if len(self._minute_bars) < count:
    #             for _ in range(count - len(self._minute_bars)):
    #                 self.wait_next_bar()
    #
    #     if not self.offline:
    #         self._update_minute_bars()
    #         ts = datetime.now(TIMEZONE).replace(tzinfo=None)
    #         timeSLT = ts - self._minute_bars.index[-1] - timedelta(minutes=1)  # time since last bar closed
    #         if timeSLT > timedelta(minutes=5):
    #             lgr.warning("It's been {} day(s), {:.0f} hour(s), and {:.0f} minute(s) since the last close on record!".
    #                         format(timeSLT.days, timeSLT.seconds / 3600, timeSLT.seconds % 3600 / 60))
    #
    #     if as_dataframe:
    #         return self._minute_bars.tail(count).copy()
    #     else:
    #         to_send = self._minute_bars.tail(count).copy()
    #         # to_send.insert(0, 'Time', [datetime.strftime(x, TIME_FORMAT) for x in to_send.index])
    #         # to_send.insert(0, 'Date', [datetime.strftime(x, DATE_FORMAT) for x in to_send.index])
    #         to_send.insert(0, 'Time', [x.time() for x in to_send.index])
    #         to_send.insert(0, 'Date', [x.date() for x in to_send.index])
    #         return to_send.values

    def get_minute_bars(self, count: int, period: int=1, as_dataframe: bool=False):
        """
        Returns: npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
        """
        lgr.debug("Getting {}-minute bars. count={}".format(period, count))

        # todo: consider checking whether request covers more time than we have, then downloading data or notifying user
        minuteBarCount = period * (count + 1)

        # If backtesting check for and then wait if there are not enough bars available
        if self.backtest:
            # todo: test this
            if len(self._minute_bars) < minuteBarCount:
                for _ in range(minuteBarCount - len(self._minute_bars)):
                    self.wait_next_bar()

        # Update minute bars and warn if there is a gap in minutes which could indicate feed issues
        if not self.offline:
            self._update_minute_bars()
            ts = datetime.now(TIMEZONE).replace(tzinfo=None)
            timeSLT = ts - self._minute_bars.index[-1] - timedelta(minutes=1)  # time since last bar closed
            if timeSLT > timedelta(minutes=5):
                lgr.warning("It's been {} day(s), {:.0f} hour(s), and {:.0f} minute(s) since the last close on record!".
                            format(timeSLT.days, timeSLT.seconds / 3600, timeSLT.seconds % 3600 / 60))

        # Re-sample minute bars into N minute bars
        toResample = self._minute_bars.iloc[-minuteBarCount:]
        resampled = DataFrame()
        resampled['Bars']  = toResample.Open.resample('%sT' % period).count()
        resampled['Open']  = toResample.Open.resample('%sT' % period).first()
        resampled['High']  = toResample.High.resample('%sT' % period).max()
        resampled['Low']   = toResample.Low.resample('%sT' % period).min()
        resampled['Close'] = toResample.Close.resample('%sT' % period).last()
        rem_labels = ['UpVol', 'DownVol', 'TotalVol', 'UpTicks', 'DownTicks', 'TotalTicks']
        resampled[rem_labels] = toResample.loc[:, rem_labels].resample('%sT' % period).sum()

        # Resample minute bars to 15 minutes in order to remove empty areas
        min15 = resampled.resample('15T').first()
        # Create mask where rows are not null and not equal to zero
        nanMask1 = ~(min15.Open.isnull()) & (min15.TotalVol != 0)
        # Append a row to mask that matches last minute bar of resampled in prep for upsampling
        nanMask1.loc[resampled.index[-1]] = nanMask1.iloc[-1]
        # Upsample the mask to minutes and fill values forward
        nanMask1 = nanMask1.resample('%sT' % period).ffill()
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

        # Extract only full bars
        # todo: consider not filtering out partial 30 or 60 minute intervals during close since they won't have enough
        resampled = resampled.loc[resampled.Bars == period, MINUTE_LABELS].tail(count)

        # Return values as either DataFrame or numpy array
        if as_dataframe:
            return resampled
        else:
            resampled.insert(0, 'Time', [x.time() for x in resampled.index])
            resampled.insert(0, 'Date', [x.date() for x in resampled.index])
            return resampled.values

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
            # updateMask = (startTime < self._updates.Datetime) & (self._updates.Datetime < end_time)
            updateMask = self._updates.Datetime < end_time
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
        # lgr.debug("Getting updates. qsize={}, current updates={}".format(len(self._queue), len(self._updates)))
        if len(self._queue) > 0 and self._ticksSaved:
            self._received_updates = True
            with self._lock:
                self._updates = concat([self._updates, self._queue], ignore_index=True)
                st = datetime.now()
                self._queue.loc[:, TICK_LABELS].to_sql(name=self.ticker, con=self.db_con2, if_exists='append', index=False)
                delta = (datetime.now() - st).total_seconds()
                self._ticks = concat([self._ticks, self._queue.loc[:, TICK_LABELS]], ignore_index=True)
                self._queue.drop(self._queue.index, axis=0, inplace=True)
            lgr.debug("Received updates. qsize={}, current updates={}, dbUpdated={:.4f}s".format(len(self._queue),
                                                                                                 len(self._updates),
                                                                                                 delta))
        else:
            self._received_updates = False

    # TODO: change wait_next_bar so that it supports multiple periods
    def wait_next_bar(self, delay=.5):
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
                sleep(delay)
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

    def load_tick_data(self):
        ts = datetime.now()
        try:
            self._ticks = read_sql_table(self.ticker, self.db_con1, parse_dates='Datetime')
            lgr.info("Loaded tick data, Time={}".format(datetime.now() - ts))
        except Exception as e:
            lgr.error("Failed to load tick data! {}, {}".format(self.ticker, e.args))

    def save_minute_data(self):
        # print("minutes.csv[", end='')
        if self._minute_bars is not None:
            if not self.backtest:
                # self.minutes.Datetime = self.minutes.Datetime.apply(lambda x: datetime.strftime(x, TIME_DATE_FORMAT))
                self._minute_bars.to_csv("{}_minute_bars.csv".format(self.ticker))
                lgr.debug("Saved minute data to {}_minute_bars.csv".format(self.ticker))
        else:
            lgr.error("Failed to save {}_minute_bars.csv, value=None".format(self.ticker))
        # print("finished.")

    def load_minute_data(self):
        try:
            if self.backtest:
                self._bt_minutes = read_csv("{}_minute_bars.csv".format(self.ticker), index_col=0,
                                            parse_dates=True)
            else:
                self._minute_bars = read_csv("{}_minute_bars.csv".format(self.ticker), index_col=0,
                                             parse_dates=True)
            lgr.info("Loaded {}_minute_bars.csv".format(self.ticker))
        except Exception as e:
            lgr.error("Failed to load {}_minute_bars.csv! {} ".format(self.ticker, e.args))

    def save_trades(self):
        if self.trades is not None:
            if self.backtest:
                self.trades.to_csv("{}_backtest_trades.csv".format(self.ticker))
            else:
                self.trades.to_csv("{}_trades.csv".format(self.ticker))
        else:
            lgr.error("Failed to {}_trades.csv: value is None ".format(self.ticker))

    def load_trades(self):
        if not self.backtest:
            try:
                self.trades = read_csv("{}_trades.csv".format(self.ticker), index_col=0,
                                       parse_dates=True)
                lgr.info("Loaded previous trades from {}_trades.csv".format(self.ticker))
            except Exception as e:
                lgr.error("Failed to read {}_trades.csv! {} ".format(self.ticker, e.args))

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

        tick_data = None
        with iq.ConnConnector([hist_conn]) as conn:
            try:
                # Get all feed updates between start time and end time
                tick_data = hist_conn.request_ticks_in_period(ticker=self.ticker, bgn_prd=start, end_prd=end)

            except (iq.NoDataError, iq.UnauthorizedError) as err:
                lgr.critical("No data returned because {}".format(err))
                sys.exit()

        if tick_data is not None:
            # Generate fields for [up ticks, down ticks, total ticks, up volume, down volume, and total volume]
            lgr.info("Downloaded %s quotes. Calculating volume and tick data..." % len(tick_data))
            df = DataFrame(np.flipud(tick_data))
            df.drop(['cond1', 'cond2', 'cond3', 'cond4', 'mkt_ctr', 'last_type', 'tick_id', 'tot_vlm'], axis=1, inplace=True)
            df.columns = ['date', 'time', 'Last', 'Size', 'Bid', 'Ask']
            df['prev'] = [0] * len(df)
            df.loc[1:, 'prev'] = list(df.iloc[:-1]['Last'])
            df.loc[:, 'change'] = df['Last'] - df['prev']
            df['UpVol'] = [0] * len(df)
            df['DownVol'] = [0] * len(df)
            df.loc[:, 'UpVol'] = df.Size.astype(dtype='int') * ((df['Last'] >= df.Ask) & (df.Bid != df.Ask))
            df.loc[:, 'DownVol'] = -1 * df.Size.astype(dtype='int') * ((df['Last'] <= df.Bid) & (df.Bid != df.Ask))
            df.loc[:, 'TotalVol'] = df.Size
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
            lgr.info("Done calculating volume and tick fields.")

            # Get ticks from trade quotes
            msk = df.TotalTicks > 0
            df.loc[msk, 'tID'] = range(len(msk))
            df.loc[~msk, 'tID'] = np.nan
            df.tID = df.tID.ffill()
            groupedTrades = df.groupby('tID', sort=False)
            ticks = groupedTrades.first()
            ticks.Size = groupedTrades.Size.sum()
            ticks.UpVol = groupedTrades.UpVol.sum()
            ticks.DownVol = groupedTrades.DownVol.sum()
            ticks.TotalVol = groupedTrades.TotalVol.sum()

            return ticks

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
        tdelta = datetime.now() - st
        lgr.debug("Chart updated. tdelta={}, last update={}".format(tdelta, ohlc.index[-1]))
        # print(datetime.now() - st)

    @staticmethod
    def set_logging_level(level: str):
        if level.lower() == "info":
            fh.setLevel(logging.INFO)
        elif level.lower() == "debug":
            fh.setLevel(logging.DEBUG)


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