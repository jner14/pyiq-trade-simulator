from simulator import Simulator
from random import random
import argparse


def get_signal_a(bars):
    # bars = npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
    # Process bars to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


def get_signal_b(bars):
    # bars = npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
    # Process bars to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


if __name__ == "__main__":
    # Arg parsing allows passing parameters via the command line
    parser = argparse.ArgumentParser(description="This demonstrates use of pyiq-trade-simulator to test custom\n" +
                                                 "trading strategies with a live data feed through iqFeed using\n" +
                                                 "pyiqfeed.")
    parser.add_argument('-t', dest='ticker', help="ticker", default="@JY#")  # @ESH17
    parser.add_argument('-b', dest='backtest', help="enable backtesting", action='store_true')
    p_args = parser.parse_args()

    # Simulation Parameters
    ticker        = p_args.ticker    # symbol to simulate trades for
    days_back     = 3                # days of historical data to download if minute_bars.csv is missing
    stop          = 3 * .0000005     # tick count * tick value (e.g. $.25 is for s&p emini) .0000005
    target        = 3 * .0000005     # tick count * tick value
    signal_func_a = get_signal_a     # function name for generating a signal
    signal_func_b = get_signal_b     # function name for generating a signal
    bar_cnt       = 30               # how many bars will be passed to signal functions
    backtest      = p_args.backtest  # whether or not to backtest

    sim = Simulator(ticker        = ticker,
                    days_back     = days_back,
                    stop          = stop,
                    target        = target,
                    signal_func_a = signal_func_a,
                    signal_func_b = signal_func_b,
                    bar_cnt       = bar_cnt,
                    backtest      = p_args.backtest)

    # Examples of chart adjustments
    # sim.bar_up_color    = '#66f4f2'  # Use color picker online, e.g. http://www.colorpicker.com/
    # sim.bar_down_color  = '#7541d8'
    # sim.chart_max_bars  = 120
    # sim.chart_bar_width = .0004

    # Default market hours are 8AM - 4PM EST. Times are set using:
    #     sim.set_market_hours(start_hour=int, start_minute=int, end_hour=int, end_minute=int)
    sim.market_hours_only = False  # Limit feed updates to market hours?

    # Enable or disable charting
    sim.charting_enabled = True

    # Begin simulation
    sim.start()
