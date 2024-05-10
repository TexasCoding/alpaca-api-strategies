from dataclasses import dataclass
from datetime import datetime
from textwrap import fill

@dataclass
class MarketOrderClass:
    status: str
    symbol: str
    qty: float
    notional: float
    side: str

@dataclass
class MarketClockClass:
    timestamp: datetime
    is_open: bool
    next_open: datetime
    next_close: datetime

@dataclass
class AssetClass:
    id: str
    class_type: str
    easy_to_borrow: bool
    exchange: str
    fractionable: bool
    maintenance_margin_requirement: float
    marginable: bool
    name: str
    shortable: bool
    status: str
    symbol: str
    tradable: bool

@dataclass
class AccountClass:
    account_blocked: bool
    account_number: str
    accrued_fees: float
    admin_configurations: object
    balance_asof: str
    bod_dtbp: float
    buying_power: float
    cash: float
    created_at: datetime
    crypto_status: str
    crypto_tier: int
    currency: str
    daytrade_count: int
    daytrading_buying_power: float
    effective_buying_power: float
    equity: float
    id: str
    initial_margin: float
    intraday_adjustments: int
    last_equity: float
    last_maintenance_margin: float
    long_market_value: float
    maintenance_margin: float
    multiplier: int
    non_marginable_buying_power: float
    options_approved_level: int
    options_buying_power: float
    options_trading_level: int
    pattern_day_trader: bool
    pending_reg_taf_fees: float
    pending_transfer_in: float
    portfolio_value: float
    position_market_value: float
    regt_buying_power: float
    short_market_value: float
    shorting_enabled: bool
    sma: float
    status: str
    trade_suspended_by_user: bool
    trading_blocked: bool
    transfers_blocked: bool
    user_configurations: object