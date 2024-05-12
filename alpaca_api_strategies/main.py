#from src.strategies.daily_losers import DailyLosers
from src.strategies.daily_losers import DailyLosers

from datetime import datetime
from pytz import timezone
tz = timezone('US/Eastern')

def main():
    ctime = datetime.now(tz)
    daily_losers = DailyLosers()

    # If it is after 4pm, save the previous day losers
    #if ctime.hour > 16:
    #print("Saving previous day losers.")
    #daily_losers.save_previous_day_losers()
    # If it is between 8am and 11am, run the script
    # Should only be ran at market open, so 9:30am NY time
    #elif ctime.hour > 7 and ctime.hour < 11:
    daily_losers.run()
    #else:
        #print("Not the right time to run the script.")

if __name__ == '__main__':
    main()