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


def example_loop_func(my_sim):
    mySignals = {}

    # Grab the most recent number of bars as necessary for signal generation
    bars = my_sim.get_minute_bars(count=my_sim.bar_cnt)
    last_close = bars[-1][5]

    # Calculate signals based on custom functions
    for sig_func in my_sim.signal_funcs:
        mySignals[sig_func.__name__] = sig_func(bars)
    finalSignal = my_sim.get_final_signal(mySignals)

    # Simulate order actions
    if finalSignal == 1:
        # Limit buy
        filled = my_sim.limit_buy(last_close)
        # If limit long was filled, create limit sell
        if filled:
            my_sim.limit_sell(last_close + my_sim.target, last_close - my_sim.stop)
    elif finalSignal == -1:
        # Limit short
        filled = my_sim.limit_short(last_close)
        # If limit short was filled, create limit cover
        if filled:
            my_sim.limit_cover(last_close - my_sim.target, last_close + my_sim.stop)

    my_sim.wait_next_bar()


def example_final_signal_func(signals):
    # Process all signals to create a final signal

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
    days_back     = 2                # days of historical data to download if minute_bars.csv is missing
    stop          = 3 * .25          # tick count * tick value (e.g. $.25 is for s&p emini) .0000005
    target        = 2 * .25          # tick count * tick value
    signal_funcs  = (get_signal_a,   # function names for generating signals
                     get_signal_b)
    bar_cnt       = 30               # how many bars will be passed to signal functions
    backtest      = p_args.backtest  # whether or not to backtest

    sim = Simulator(ticker        = ticker,
                    period        = 60,
                    days_back     = days_back,
                    stop          = stop,
                    target        = target,
                    signal_funcs  = signal_funcs,
                    bar_cnt       = bar_cnt,
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
    sim.market_hours_only = True  # Limit feed updates to market hours?

    # Enable or disable charting
    sim.charting_enabled = True

    # Begin simulation
    sim.start()
