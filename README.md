# Alpaca API Strategies

## Overview
Alpaca API Strategies is a project that aims to provide a collection of trading strategies for the Alpaca API. It is designed to help traders automate their trading decisions and optimize their investment strategies. This project is in no way to be expected or trusted to make real money. The goal is to provide a resource to help developers access the Alpaca API and easily develop strategies. I intend on keeping this project open and continue developing different strategies. It is nowwhere near a complete project. Just started with the idea on 04/28/2024 and have much to do.

The only service this project uses that cost any money is OpenAi API, but it is very cheap to use. Also I can not access crypto trading from my location, so the project will only focus on stock trading for now. Possibly trading options will be added.

## Current Strategies
- **Daily Losers**: This strategy pulls previous day losers from Yahoo Finance. Then uses yfinance to access tickers data for each loser. Sorts through Yahoo Finance buy recommendations to filter stocks more likely to rise. Ta indicators are used to help filter stocks that are oversold range. 3 articles of each are then pulled from Yahoo Finance and fed to OpenAi for sentiment analysis. The script should only be run once per day at market open. It will look for sells of current positions by filtering for overbought signals from ta indicators. Before buying for the day, it verifies at least 10% of account value is in cash, if not it will liquidate top positions to make cash available. Look through the code for more details.

    I still have some functionality seperation to do in this strategy. Some comes from the Yahoo class when it may be better to reside in the DailyLoser class. But the strategy is functioning and working as intended. Starting and fresh $1000.00 Alpaca Paper account on 05/06/2024 to test for errors, functionality and profitability. Will post updates weekly on the project.

## Getting Started
To get started with Alpaca API Strategies, follow these steps:

1. Poetry is used for dependency management, so install [Python Poetry](https://python-poetry.org/) on your OS of choice
2. The project uses OpenAi for sentiment analysis, so you will need an [OpenAi API Key](https://platform.openai.com/)
3. I also use Slack for message notifications, but this is not neccesary for script operations. If no slack api key is provided, it will just print messages.
4. Clone the repository
   ```bash
    git clone https://github.com/TexasCoding/alpaca-api-strategies.git
   ```
5. Install dependencies with poetry
   ```bash
    poetry install
   ```
6. Project uses dotenv for development enviroment variables, so create a .env file with the following details:
   ```properties
    PRODUCTION=False
    APCA_API_KEY_ID=your_alpaca_api_key
    APCA_API_SECRET_KEY=your_alpaca_secret_key
    APCA_PAPER=True
    OPENAI_API_KEY=your_open_ai_api_key
    SLACK_ACCESS_TOKEN=this_one_can_be_ignored
    SLACK_USERNAME=this_one_can_be_ignored
   ```
7. Run the script in your terminal.
 
   Using poetry.
   ```bash
   poetry run python alpaca_api_strategies/main.py
   ```
   Or Python
   ```bash
   python alpaca_api_strategies/main.py
   ```
8. This script will buy and sell automatically, so please only use in a paper account until proven to work and be profitable.
   
## Contributing
Contributions to Alpaca API Strategies are welcome! If you have a new strategy to add or want to improve an existing one, please submit a pull request. Make sure to follow the contribution guidelines outlined in the `CONTRIBUTING.md` file.

## License
Alpaca API Strategies is licensed under the MIT License. See the `LICENSE` file for more information.

## Contact
If you have any questions or suggestions regarding Alpaca API Strategies, feel free to reach out to us by posting in [Issues](https://github.com/TexasCoding/alpaca-api-strategies/issues)
