from src.strategies.daily_losers import DailyLosers

def main():
    daily_losers = DailyLosers()
    daily_losers.run()

if __name__ == '__main__':
    main()