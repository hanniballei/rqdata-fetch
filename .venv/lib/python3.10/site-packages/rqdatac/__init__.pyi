import rqdatac.services.market_data
import rqdatac.services.concept
import rqdatac.services.factor
import rqdatac.services.ksh_auction_info
import rqdatac.services.get_price
import rqdatac.client
import rqdatac.services.financial
import rqdatac.services.options
import rqdatac.services.get_capital_flow
import rqdatac.services.consensus
import rqdatac.services.constant
import rqdatac.services.xueqiu
import rqdatac.services.calendar
import rqdatac.services.async_live_md_client
import rqdatac.services.extra
import rqdatac.services.stock_status
import rqdatac.services.structured_fund
import rqdatac.services.live
import rqdatac.services.shenwan
import rqdatac.services.tmall
import rqdatac.services.index
import rqdatac.services.live_md_client
import rqdatac.services.convertible
import rqdatac.services.basic
import rqdatac.services.future

__version__: str = ...
init = rqdatac.client.init
reset = rqdatac.client.reset
initialized = rqdatac.client.initialized
concept_list = rqdatac.services.concept.concept_list
concept = rqdatac.services.concept.concept
concept_names = rqdatac.services.concept.concept_names
get_concept_list = rqdatac.services.concept.get_concept_list
get_concept = rqdatac.services.concept.get_concept
get_stock_concept = rqdatac.services.concept.get_stock_concept
shenwan_industry = rqdatac.services.shenwan.shenwan_industry
shenwan_instrument_industry = rqdatac.services.shenwan.shenwan_instrument_industry
zx_industry = rqdatac.services.shenwan.zx_industry
zx_instrument_industry = rqdatac.services.shenwan.zx_instrument_industry
get_industry = rqdatac.services.shenwan.get_industry
get_industry_change = rqdatac.services.shenwan.get_industry_change
get_instrument_industry = rqdatac.services.shenwan.get_instrument_industry
get_industry_mapping = rqdatac.services.shenwan.get_industry_mapping
industry_code = rqdatac.services.constant.IndustryCode
IndustryCode = rqdatac.services.constant.IndustryCode
sector_code = rqdatac.services.constant.SectorCode
SectorCode = rqdatac.services.constant.SectorCode
get_trading_dates = rqdatac.services.calendar.get_trading_dates
get_next_trading_date = rqdatac.services.calendar.get_next_trading_date
get_previous_trading_date = rqdatac.services.calendar.get_previous_trading_date
get_latest_trading_date = rqdatac.services.calendar.get_latest_trading_date
get_future_latest_trading_date = rqdatac.services.calendar.get_future_latest_trading_date
trading_date_offset = rqdatac.services.calendar.trading_date_offset
is_trading_date = rqdatac.services.calendar.is_trading_date
has_night_trading = rqdatac.services.calendar.has_night_trading
id_convert = rqdatac.services.basic.id_convert
get_tick_size = rqdatac.services.basic.get_tick_size
instruments = rqdatac.services.basic.instruments
all_instruments = rqdatac.services.basic.all_instruments
sector = rqdatac.services.basic.sector
industry = rqdatac.services.basic.industry
get_future_contracts = rqdatac.services.basic.get_future_contracts


class futures:
    get_basis = rqdatac.services.future.get_basis
    get_commission_margin = rqdatac.services.future.get_commission_margin
    get_contract_multiplier = rqdatac.services.future.get_contract_multiplier
    get_contracts = rqdatac.services.basic.get_contracts
    get_current_basis = rqdatac.services.future.get_current_basis
    get_dominant = rqdatac.services.future.get_dominant
    get_dominant_price = rqdatac.services.future.get_dominant_price
    get_ex_factor = rqdatac.services.future.get_ex_factor
    get_exchange_daily = rqdatac.services.future.get_exchange_daily
    get_member_rank = rqdatac.services.future.get_member_rank
    get_trading_parameters = rqdatac.services.future.get_trading_parameters
    get_warehouse_stocks = rqdatac.services.future.get_warehouse_stocks
    get_roll_yield = rqdatac.services.future.get_roll_yield


jy_instrument_industry = rqdatac.services.basic.jy_instrument_industry


class econ:
    get_factors = rqdatac.services.basic.get_factors
    get_money_supply = rqdatac.services.basic.get_money_supply
    get_reserve_ratio = rqdatac.services.basic.get_reserve_ratio


get_main_shareholder = rqdatac.services.basic.get_main_shareholder
get_current_news = rqdatac.services.basic.get_current_news
get_trading_hours = rqdatac.services.basic.get_trading_hours
get_trading_periods = rqdatac.services.basic.get_trading_periods
get_private_placement = rqdatac.services.basic.get_private_placement
get_share_transformation = rqdatac.services.basic.get_share_transformation


class user:
    get_quota = rqdatac.services.basic.get_quota


get_update_status = rqdatac.services.basic.get_update_status
info = rqdatac.services.basic.info
get_basic_info = rqdatac.services.basic.get_basic_info
get_spot_benchmark_price = rqdatac.services.basic.get_spot_benchmark_price
LiveMarketDataClient = rqdatac.services.live_md_client.LiveMarketDataClient
AsyncLiveMarketDataClient = rqdatac.services.async_live_md_client.AsyncLiveMarketDataClient


class consensus:
    all_industries = rqdatac.services.consensus.all_industries
    get_analyst_momentum = rqdatac.services.consensus.get_analyst_momentum
    get_comp_indicators = rqdatac.services.consensus.get_comp_indicators
    get_expect_appr_exceed = rqdatac.services.consensus.get_expect_appr_exceed
    get_expect_prob = rqdatac.services.consensus.get_expect_prob
    get_factor = rqdatac.services.consensus.get_factor
    get_indicator = rqdatac.services.consensus.get_indicator
    get_industry_rating = rqdatac.services.consensus.get_industry_rating
    get_market_estimate = rqdatac.services.consensus.get_market_estimate
    get_price = rqdatac.services.consensus.get_price
    get_security_change = rqdatac.services.consensus.get_security_change


get_consensus_indicator = rqdatac.services.consensus.get_indicator
get_consensus_price = rqdatac.services.consensus.get_price
get_consensus_comp_indicators = rqdatac.services.consensus.get_comp_indicators
all_consensus_industries = rqdatac.services.consensus.all_industries
get_consensus_industry_rating = rqdatac.services.consensus.get_industry_rating
get_consensus_market_estimate = rqdatac.services.consensus.get_market_estimate
get_dominant_future = rqdatac.services.future.get_dominant_future
future_commission_margin = rqdatac.services.future.future_commission_margin
get_future_member_rank = rqdatac.services.future.get_future_member_rank
current_stock_connect_quota = rqdatac.services.stock_status.current_stock_connect_quota
get_stock_connect_quota = rqdatac.services.stock_status.get_stock_connect_quota
is_st_stock = rqdatac.services.stock_status.is_st_stock
_is_st_stock = rqdatac.services.stock_status._is_st_stock
is_suspended = rqdatac.services.stock_status.is_suspended
get_stock_connect = rqdatac.services.stock_status.get_stock_connect
get_securities_margin = rqdatac.services.stock_status.get_securities_margin
get_margin_stocks = rqdatac.services.stock_status.get_margin_stocks
get_eligible_securities_margin = rqdatac.services.stock_status.get_eligible_securities_margin
get_shares = rqdatac.services.stock_status.get_shares
get_allotment = rqdatac.services.stock_status.get_allotment
get_symbol_change_info = rqdatac.services.stock_status.get_symbol_change_info
get_special_treatment_info = rqdatac.services.stock_status.get_special_treatment_info
get_incentive_plan = rqdatac.services.stock_status.get_incentive_plan
get_investor_ra = rqdatac.services.stock_status.get_investor_ra
get_announcement = rqdatac.services.stock_status.get_announcement
get_holder_number = rqdatac.services.stock_status.get_holder_number
get_audit_opinion = rqdatac.services.stock_status.get_audit_opinion
get_restricted_shares = rqdatac.services.stock_status.get_restricted_shares
st_warning = rqdatac.services.stock_status.st_warning
get_staff_count = rqdatac.services.stock_status.get_staff_count
current_snapshot = rqdatac.services.live.current_snapshot
get_ticks = rqdatac.services.live.get_ticks
current_minute = rqdatac.services.live.current_minute
get_live_ticks = rqdatac.services.live.get_live_ticks
get_price = rqdatac.services.get_price.get_price
get_stock_connect_holding_details = rqdatac.services.get_price.get_stock_connect_holding_details
get_vwap = rqdatac.services.get_price.get_vwap


class convertible:
    all_instruments = rqdatac.services.convertible.all_instruments
    get_accrued_interest_eod = rqdatac.services.convertible.get_accrued_interest_eod
    get_call_announcement = rqdatac.services.convertible.get_call_announcement
    get_call_info = rqdatac.services.convertible.get_call_info
    get_cash_flow = rqdatac.services.convertible.get_cash_flow
    get_close_price = rqdatac.services.convertible.get_close_price
    get_conversion_info = rqdatac.services.convertible.get_conversion_info
    get_conversion_price = rqdatac.services.convertible.get_conversion_price
    get_coupon_rate_table = rqdatac.services.convertible.get_coupon_rate_table
    get_credit_rating = rqdatac.services.convertible.get_credit_rating
    get_indicators = rqdatac.services.convertible.get_indicators
    get_industry = rqdatac.services.convertible.get_industry
    get_instrument_industry = rqdatac.services.convertible.get_instrument_industry
    get_latest_rating = rqdatac.services.convertible.get_latest_rating
    get_put_info = rqdatac.services.convertible.get_put_info
    get_std_discount = rqdatac.services.convertible.get_std_discount
    instruments = rqdatac.services.convertible.instruments
    is_suspended = rqdatac.services.convertible.is_suspended
    rating = rqdatac.services.convertible.rating


current_freefloat_turnover = rqdatac.services.extra.current_freefloat_turnover
get_live_minute_price_change_rate = rqdatac.services.extra.get_live_minute_price_change_rate
get_all_factor_names = rqdatac.services.factor.get_all_factor_names
get_factor = rqdatac.services.factor.get_factor
get_factor_return = rqdatac.services.factor.get_factor_return
get_live_factor_return = rqdatac.services.factor.get_live_factor_return
get_factor_exposure = rqdatac.services.factor.get_factor_exposure
get_style_factor_exposure = rqdatac.services.factor.get_style_factor_exposure
get_descriptor_exposure = rqdatac.services.factor.get_descriptor_exposure
get_stock_beta = rqdatac.services.factor.get_stock_beta
get_factor_covariance = rqdatac.services.factor.get_factor_covariance
get_specific_return = rqdatac.services.factor.get_specific_return
get_specific_risk = rqdatac.services.factor.get_specific_risk
get_index_factor_exposure = rqdatac.services.factor.get_index_factor_exposure
get_pit_financials_ex = rqdatac.services.financial.get_pit_financials_ex
current_performance = rqdatac.services.financial.current_performance
performance_forecast = rqdatac.services.financial.performance_forecast
get_capital_flow = rqdatac.services.get_capital_flow.get_capital_flow
current_capital_flow_minute = rqdatac.services.get_capital_flow.current_capital_flow_minute
get_open_auction_info = rqdatac.services.get_capital_flow.get_open_auction_info
get_close_auction_info = rqdatac.services.get_capital_flow.get_close_auction_info
index_components = rqdatac.services.index.index_components
index_weights = rqdatac.services.index.index_weights
index_indicator = rqdatac.services.index.index_indicator
index_weights_ex = rqdatac.services.index.index_weights_ex
get_ksh_auction_info = rqdatac.services.ksh_auction_info.get_ksh_auction_info
get_auction_info = rqdatac.services.ksh_auction_info.get_auction_info
get_split = rqdatac.services.market_data.get_split
get_dividend = rqdatac.services.market_data.get_dividend
get_dividend_info = rqdatac.services.market_data.get_dividend_info
get_dividend_amount = rqdatac.services.market_data.get_dividend_amount
get_ex_factor = rqdatac.services.market_data.get_ex_factor
get_turnover_rate = rqdatac.services.market_data.get_turnover_rate
get_price_change_rate = rqdatac.services.market_data.get_price_change_rate
get_yield_curve = rqdatac.services.market_data.get_yield_curve
get_block_trade = rqdatac.services.market_data.get_block_trade
get_exchange_rate = rqdatac.services.market_data.get_exchange_rate
get_temporary_code = rqdatac.services.market_data.get_temporary_code
get_interbank_offered_rate = rqdatac.services.market_data.get_interbank_offered_rate
get_abnormal_stocks = rqdatac.services.market_data.get_abnormal_stocks
get_abnormal_stocks_detail = rqdatac.services.market_data.get_abnormal_stocks_detail
get_buy_back = rqdatac.services.market_data.get_buy_back
get_forecast_report_date = rqdatac.services.market_data.get_forecast_report_date
get_leader_shares_change = rqdatac.services.market_data.get_leader_shares_change


class options:
    get_atm_option = rqdatac.services.options.get_atm_option
    get_commission = rqdatac.services.options.get_commission
    get_contract_property = rqdatac.services.options.get_contract_property
    get_contracts = rqdatac.services.options.get_contracts
    get_dominant_month = rqdatac.services.options.get_dominant_month
    get_greeks = rqdatac.services.options.get_greeks
    get_pcr_ratio = rqdatac.services.options.get_pcr_ratio



class fenji:
    get = rqdatac.services.structured_fund.get
    get_a_by_interest_rule = rqdatac.services.structured_fund.get_a_by_interest_rule
    get_a_by_yield = rqdatac.services.structured_fund.get_a_by_yield
    get_all = rqdatac.services.structured_fund.get_all


ecommerce = rqdatac.services.tmall.ecommerce


class xueqiu:
    history = rqdatac.services.xueqiu.history
    top_stocks = rqdatac.services.xueqiu.top_stocks

