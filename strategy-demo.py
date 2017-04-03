from simulator import Simulator
from random import random
import argparse


def get_ticks_signal(ticks):
    # ticks = npArray[Date, Time, Last, Bid, Ask, Direction, UpVol, DownVol, TotalVol]
    # Process ticks to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


def get_tick_bar_signal(tick_bars):
    # tick_bars = npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
    # Process bars to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


def get_minute_bars_signal(min_bars):
    # min_bars = npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
    # Process bars to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


def get_tick_range_bar_signal(tick_range_bars):
    # range_bars = npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
    # Process bars to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


def example_loop_func(my_sim):
    """
    Examples of data retrieval
    --------------------------
    
    50 1-minute bars:
    minute_bars_1 = my_sim.get_minute_bars(count=50)
    
    50 5-minute bars:
    minute_bars_5 = my_sim.get_minute_bars(count=50, period=5)
    
    100 ticks:
    ticks_by_count = my_sim.get_ticks(count=100)
    
    100s of ticks:
    ticks_by_time = my_sim.get_ticks(time_seconds=100)
    
    50 5-tick bars:
    tick_bars_by_count_5 = my_sim.get_tick_bars(count=50, period=5)
    
    300s of 5-tick bars:
    tick_bars_by_time_5 = my_sim.get_tick_bars(time_seconds=300, period=5)
    
    50 5-tick range bars for @ESM17 ticks size=.25
    tick_range_bars = my_sim.get_tick_range_bars(tick_range=5, tick_size=.25, count=50)
    
    6000s 5-tick range bars for @ESM17 ticks size=.25
    tick_range_bars = my_sim.get_tick_range_bars(tick_range=5, tick_size=.25, time_span=6000)
    
    50 1-minute bars as dataframe:
    minute_bars_as_dataframe = my_sim.get_minute_bars(count=50, as_dataframe=True)
    
    100 ticks as dataframe:
    ticks_as_dataframe = my_sim.get_ticks(count=100, as_dataframe=True)
    
    50 5-tick bars as dataframe:
    tick_bars_as_dataframe = my_sim.get_tick_bars(count=50, period=5, as_dataframe=True)
    """

    mySignals = {}

    # Retrieve data for signal processing
    minute_bars = my_sim.get_minute_bars(count=50, period=5)
    ticks = my_sim.get_ticks(count=100)
    tick_bars = my_sim.get_tick_bars(span_seconds=300, period=5)
    tick_range_bars = my_sim.get_tick_range_bars(tick_range=2, tick_size=.25, count=20)

    # Process data and generate signals using custom functions
    mySignals["minBarSignal"] = get_minute_bars_signal(minute_bars)
    mySignals["tickSignal"] = get_ticks_signal(ticks)
    mySignals["tickBarSignal"] = get_tick_bar_signal(tick_bars)
    mySignals["tickRangeBarSignal"] = get_tick_range_bar_signal(tick_range_bars)
    finalSignal = my_sim.get_final_signal(mySignals)

    last_close = minute_bars[-1][5]

    # Simulate order actions
    if finalSignal == 1:

        # Limit buy
        fillPrice = my_sim.limit_buy(last_close)

        # If limit long was filled, create limit sell
        if fillPrice > 0:
            my_sim.limit_sell(fillPrice + my_sim.target, fillPrice - my_sim.stop)

    elif finalSignal == -1:

        # Limit short
        fillPrice = my_sim.limit_short(last_close)

        # If limit short was filled, create limit cover
        if fillPrice > 0:
            my_sim.limit_cover(fillPrice - my_sim.target, fillPrice + my_sim.stop)

    # Wait until the specified bar type has closed
    # bar_type=[minute, tick, n-tick range(eg. 2-tick range, 3-tick range, ...)]
    my_sim.wait_next_bar(bar_type='tick')


def example_final_signal_func(signals):
    # Process all signals to create a final signal

    # This simple strategy checks that all signals align as either buy or sell

    # Ex. If all signals are positive, return 1 (buy signal)
    if sum(signals.values()) == len(signals):
        return 1
    # Ex. If all signals are negative, return -1 (sell signal)
    if sum(signals.values()) == -len(signals):
        return -1
    # Ex. Otherwise, return 0 (hold signal)
    return 0


if __name__ == "__main__":
    # Arg parsing allows passing parameters via the command line
    parser = argparse.ArgumentParser(description="This demonstrates use of pyiq-trade-simulator to test custom\n" +
                                                 "trading strategies with a live data feed through iqFeed using\n" +
                                                 "pyiqfeed.")
    parser.add_argument('-t', dest='ticker', help="ticker", default="@ESM17")  # @ESM17  @JY#  QCL#
    parser.add_argument('-b', dest='backtest', help="enable backtesting", action='store_true')
    p_args = parser.parse_args()

    # Simulation Parameters
    ticker        = p_args.ticker    # symbol to simulate trades for
    days_back     = 1                # days of historical data to download if minute_bars.csv is missing
    stop          = 3 * .25          # tick count * tick value (e.g. $.25 is for s&p emini) .0000005
    target        = 2 * .25          # tick count * tick value
    backtest      = p_args.backtest  # whether or not to backtest

    sim = Simulator(ticker        = ticker,
                    days_back     = days_back,
                    stop          = stop,
                    target        = target,
                    backtest      = p_args.backtest)

    # Examples of chart adjustments
    # sim.bar_up_color    = '#66f4f2'  # Use color picker online, e.g. http://www.colorpicker.com/
    # sim.bar_down_color  = '#7541d8'
    # sim.chart_max_bars  = 120
    # sim.chart_bar_width = .0004

    # Set logging level to INFO when not debugging to improve performance
    # Simulator.set_logging_level("INFO")

    # Set custom trade simulation loop function
    sim.set_loop_func(example_loop_func)

    # Set custom final signal function
    sim.set_final_signal_func(example_final_signal_func)

    # Default market hours are 8AM - 4PM EST. Times are set using:
    #     sim.set_market_hours(start_hour=int, start_minute=int, end_hour=int, end_minute=int)
    sim.market_hours_only = False  # Limit feed updates to market hours?

    # Enable or disable charting
    sim.charting_enabled = True

    # Begin simulation
    sim.start()
