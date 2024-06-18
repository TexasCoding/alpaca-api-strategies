import pendulum
from rich.console import Console
from alpaca_daily_losers.daily_losers import DailyLosers


console = Console()

current_time = pendulum.now("America/New_York")


def main():
    daily_losers = DailyLosers()
    with console.status("[bold green]Daily Losers is running, please wait..."):
        if current_time.hour >= 8 and current_time.hour <= 10:
            daily_losers.run(buy_limit=5, article_limit=3)
        elif current_time.hour > 10 and current_time.hour <= 16:
            daily_losers.close.sell_positions_from_criteria()
        else:
            print("Market is closed.")
            return


if __name__ == "__main__":
    main()
