import pendulum
from py_alpaca_daily_losers.daily_losers import DailyLosers

current_time = pendulum.now("America/New_York")


def main():
    daily_losers = DailyLosers()
    if current_time.hour >= 8 and current_time.hour <= 10:
        daily_losers.run()
    elif current_time.hour > 10 and current_time.hour <= 16:
        daily_losers.sell_positions_from_criteria()
    else:
        print("Market is closed.")


if __name__ == "__main__":
    main()
