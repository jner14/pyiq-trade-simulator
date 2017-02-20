from simulator import Simulator
from random import random
import argparse


def get_signal_a(bars):
    # Process bars to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


def get_signal_b(bars):
    # Process bars to create a signal

    # Just a random signal generator
    signal = 1 if random() > .5 else -1
    return signal


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


if __name__ == "__main__":
    # Arg parsing allows passing parameters via the command line
    parser = argparse.ArgumentParser(description="Run strategy-demo which uses LiveTradeSimulator to interface iqfeed")
    parser.add_argument('-t', dest='ticker', help="Ticker", default="@JY#")  # @ESH17
    parser.add_argument('-b', dest='backtest', help="Backtest Enabled: yes/no", default="n")
    pargs = parser.parse_args()

    # Create simulator as live or backtesting
    daysBack = 5  # Days of historical data to download if minute_bars.csv is missing
    if pargs.backtest.lower() in ['t', 'true', 'y', 'yes']:
        sim = Simulator(pargs.ticker, days_back=daysBack, backtest=True, offline=True)
    else:
        sim = Simulator(pargs.ticker, days_back=daysBack)

    sim.marketHoursOnly = True  # Limit feed updates to market hours?
    sim.start()
    mySignals = {}
    stop = 2 * .0000005  # tick count * tick value (e.g. $.25 is for s&p emini)
    target = 2 * .0000005  # tick count * tick value

    while True:
        # Grab the most recent number of bars as necessary for signal generation
        # bars = npArray[Date, Time, Open, High, Low, Close, UpVol, DownVol, TotalVol, UpTicks, DownTicks, TotalTicks]
        bars = sim.get_minute_bars(count=10)
        last_close = bars[-1][5]

        # Calculate signals based on custom functions
        mySignals['a'] = get_signal_a(bars)
        mySignals['b'] = get_signal_b(bars)
        finalSignal = get_final_signal(mySignals)

        # Simulate order actions
        if finalSignal == 1:
            sim.buy(last_close)
            sim.wait_on_sell(last_close + target, last_close - stop)
        elif finalSignal == -1:
            sim.short(last_close)
            sim.wait_on_cover(last_close - target, last_close + stop)

        # Wait for close of next bar
        sim.wait_next_bar()


