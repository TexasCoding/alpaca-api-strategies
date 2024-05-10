from alpaca_api_strategies.src.alpaca import AlpacaAPI

class TestClass:
    def test_alpaca_init(self):
        alpaca = AlpacaAPI()
        assert alpaca is not None

    def test_alpaca_get_account(self):
        alpaca = AlpacaAPI()
        account = alpaca.get_account()
        assert account is not None
        assert 'id' in account
        assert 'cash' in account
        assert 'buying_power' in account
        assert 'last_equity' in account



    def test_alpaca_get_positions(self):
        alpaca = AlpacaAPI()
        positions = alpaca.get_current_positions()
        assert positions is not None
        assert 'asset' in positions