# -*- coding: utf-8 -*-
import datetime
import bisect
import re
import warnings
from collections import defaultdict

import numpy as np
import six
import pandas as pd

from rqdatac.client import get_client
from rqdatac.utils import to_date, datetime_to_int14, to_date_str, to_time, int8_to_date, to_date_int
from rqdatac.validators import (
    ensure_list_of_string,
    ensure_date_int,
    check_type,
    ensure_date_str,
    ensure_order_book_id,
    ensure_order_book_ids,
    check_items_in_container,
    ensure_date_range,
    ensure_int,
    ensure_string,
    ensure_date_or_today_int,
    ensure_string_in,
)
from rqdatac.services.concept import concept_names as get_concept_names
from rqdatac.services.shenwan import get_instrument_industry
from rqdatac.services.constant import SectorCode, SectorCodeItem, IndustryCode, IndustryCodeItem
from rqdatac.services.calendar import get_previous_trading_date, is_trading_date, has_night_trading, get_trading_dates
from rqdatac.decorators import export_as_api, ttl_cache, compatible_with_parm, may_trim_bjse
from rqdatac.hk_decorators import support_hk_order_book_id
from dateutil.relativedelta import relativedelta
from rqdatac.rqdatah_helper import (
    rqdatah_serialize, http_conv_list_to_csv,
    http_conv_trading_hours, http_conv_dict_to_csv,
    http_conv_instruments, rqdatah_no_index_mark
)

_wind_exchange_map = {
    'CFFEX': 'CFE',
    'SHFE': 'SHF',
    'INE': 'INE',
    'DCE': 'DCE',
    'CZCE': 'CZC',
    'GFEX': 'GFE',
}


_wind_index_map = {
    '000902.CSI': '000902.XSHG',
    '000904.CSI': '000904.XSHG',
    '000907.CSI': '000907.XSHG',
    '000980.CSI': '000980.XSHG',
    '000985.CSI': '000985.XSHG',
    'h30455.CSI': 'H30455.XSHG',
    '921395.CSI': '921395.INDX',
    '921396.CSI': '921396.INDX',
    '921459.SH': '921459.INDX',
    '921460.SH': '921460.INDX',
    'h00300.CSI': 'H00300.INDX',
    'h00902.CSI': 'H00902.INDX',
    'h00903.CSI': 'H00903.INDX',
    'h00904.CSI': 'H00904.INDX',
    'h00905.CSI': 'H00905.INDX',
    'h00906.CSI': 'H00906.INDX',
    'h00907.CSI': 'H00907.INDX',
    'h00980.CSI': 'H00980.INDX',
    'h00985.CSI': 'H00985.INDX',
    'h20748.CSI': 'H20748.INDX',
    'h20749.CSI': 'H20749.INDX',
    'h20750.CSI': 'H20750.INDX',
    'h20751.CSI': 'H20751.INDX',
    'h20752.CSI': 'H20752.INDX',
    'h20753.CSI': 'H20753.INDX',
    'h20903.CSI': 'H20903.INDX',
    'h30310.CSI': 'H30310.XSHG',
    '000908.CSI': '000908.XSHG',
    '000909.CSI': '000909.XSHG',
    '000910.CSI': '000910.XSHG',
    '000911.CSI': '000911.XSHG',
    '000912.CSI': '000912.XSHG',
    '000913.CSI': '000913.XSHG',
    '000915.CSI': '000915.XSHG',
    '000916.CSI': '000916.XSHG',
    '000917.CSI': '000917.XSHG',
    '000951.CSI': '000951.XSHG',
    '000952.CSI': '000952.XSHG',
    '000957.CSI': '000957.XSHG',
    'h00908.CSI': 'H00908.INDX',
    'h00909.CSI': 'H00909.INDX',
    'h00910.CSI': 'H00910.INDX',
    'h00911.CSI': 'H00911.INDX',
    'h00912.CSI': 'H00912.INDX',
    'h00913.CSI': 'H00913.INDX',
    'h00915.CSI': 'H00915.INDX',
    'h00916.CSI': 'H00916.INDX',
    'h00917.CSI': 'H00917.INDX',
    'h00952.CSI': 'H00952.INDX',
    'h30034.CSI': 'H30034.XSHG',
    'h00951.CSI': 'H00951.INDX',
    'h00957.CSI': 'H00957.INDX',
    'h20034.CSI': 'H20034.INDX',
    'h30250.CSI': 'H30250.XSHG',
    'h30253.CSI': 'H30253.XSHG',
    'h30254.CSI': 'H30254.XSHG',
    'h30255.CSI': 'H30255.XSHG',
    'h30258.CSI': 'H30258.XSHG',
    'h30259.CSI': 'H30259.XSHG',
    'h20250.CSI': 'H20250.INDX',
    'h20251.CSI': 'H20251.INDX',
    'h20252.CSI': 'H20252.INDX',
    'h20253.CSI': 'H20253.INDX',
    'h20254.CSI': 'H20254.INDX',
    'h20255.CSI': 'H20255.INDX',
    'h20257.CSI': 'H20257.INDX',
    'h20258.CSI': 'H20258.INDX',
    'h20259.CSI': 'H20259.INDX',
    'h20673.CSI': 'H20673.INDX',
    'h20670.CSI': 'H20670.INDX',
    'h20671.CSI': 'H20671.INDX',
    'h20674.CSI': 'H20674.INDX',
    'h20675.CSI': 'H20675.INDX',
    'h20676.CSI': 'H20676.INDX',
    'h20677.CSI': 'H20677.INDX',
    'h20679.CSI': 'H20679.INDX',
    'h20680.CSI': 'H20680.INDX',
    'h20681.CSI': 'H20681.INDX',
    'h20682.CSI': 'H20682.INDX',
    'h20683.CSI': 'H20683.INDX',
    'h20684.CSI': 'H20684.INDX',
    'h20685.CSI': 'H20685.INDX',
    'h20694.CSI': 'H20694.INDX',
    'h20695.CSI': 'H20695.INDX',
    'h20686.CSI': 'H20686.INDX',
    'h20687.CSI': 'H20687.INDX',
    'h20688.CSI': 'H20688.INDX',
    'h20689.CSI': 'H20689.INDX',
    'h20690.CSI': 'H20690.INDX',
    'h20691.CSI': 'H20691.INDX',
    'h20692.CSI': 'H20692.INDX',
    'h20693.CSI': 'H20693.INDX',
    '000929.CSI': '000929.XSHG',
    '000930.CSI': '000930.XSHG',
    '000931.CSI': '000931.XSHG',
    '000936.CSI': '000936.XSHG',
    '000937.CSI': '000937.XSHG',
    'h30086.CSI': 'H30086.XSHG',
    'h00928.CSI': 'H00928.INDX',
    'h00929.CSI': 'H00929.INDX',
    'h00930.CSI': 'H00930.INDX',
    'h00931.CSI': 'H00931.INDX',
    'h00932.CSI': 'H00932.INDX',
    'h00933.CSI': 'H00933.INDX',
    'h00935.CSI': 'H00935.INDX',
    'h00936.CSI': 'H00936.INDX',
    'h00937.CSI': 'H00937.INDX',
    'h20025.CSI': 'H20025.INDX',
    'h20086.CSI': 'H20086.INDX',
    '000841.CSI': '000841.XSHG',
    'h30010.CSI': 'H30010.XSHG',
    'h30013.CSI': 'H30013.XSHG',
    'h30014.CSI': 'H30014.XSHG',
    'h30016.CSI': 'H30016.XSHG',
    'h30017.CSI': 'H30017.XSHG',
    'h30018.CSI': 'H30018.XSHG',
    'h30019.CSI': 'H30019.XSHG',
    'h30020.CSI': 'H30020.XSHG',
    'h30022.CSI': 'H30022.XSHG',
    'h30023.CSI': 'H30023.XSHG',
    'h30024.CSI': 'H30024.XSHG',
    'h30026.CSI': 'H30026.XSHG',
    'h30028.CSI': 'H30028.XSHG',
    'h30029.CSI': 'H30029.XSHG',
    'h30031.CSI': 'H30031.XSHG',
    'h00841.CSI': 'H00841.INDX',
    'h20010.CSI': 'H20010.INDX',
    'h20013.CSI': 'H20013.INDX',
    'h20014.CSI': 'H20014.INDX',
    'h20016.CSI': 'H20016.INDX',
    'h20017.CSI': 'H20017.INDX',
    'h20018.CSI': 'H20018.INDX',
    'h20019.CSI': 'H20019.INDX',
    'h20020.CSI': 'H20020.INDX',
    'h20022.CSI': 'H20022.INDX',
    'h20023.CSI': 'H20023.INDX',
    'h20024.CSI': 'H20024.INDX',
    'h20026.CSI': 'H20026.INDX',
    'h20028.CSI': 'H20028.INDX',
    'h20029.CSI': 'H20029.INDX',
    'h20031.CSI': 'H20031.INDX',
    '931775.CSI': '931775.INDX',
    'h00986.CSI': 'H00986.INDX',
    'h00987.CSI': 'H00987.INDX',
    'h00988.CSI': 'H00988.INDX',
    'h00989.CSI': 'H00989.INDX',
    'h00990.CSI': 'H00990.INDX',
    'h00991.CSI': 'H00991.INDX',
    'h00993.CSI': 'H00993.INDX',
    'h00994.CSI': 'H00994.INDX',
    'h00995.CSI': 'H00995.INDX',
    'h30166.CSI': 'H30166.XSHG',
    'h30170.CSI': 'H30170.XSHG',
    'h30171.CSI': 'H30171.XSHG',
    'h30173.CSI': 'H30173.XSHG',
    'h30174.CSI': 'H30174.XSHG',
    'h30175.CSI': 'H30175.XSHG',
    'h30177.CSI': 'H30177.XSHG',
    'h30179.CSI': 'H30179.XSHG',
    'h30182.CSI': 'H30182.XSHG',
    'h30183.CSI': 'H30183.XSHG',
    'h30184.CSI': 'H30184.XSHG',
    'h30186.CSI': 'H30186.XSHG',
    'h30187.CSI': 'H30187.XSHG',
    'h30196.CSI': 'H30196.XSHG',
    'h30211.CSI': 'H30211.XSHG',
    'h20164.CSI': 'H20164.INDX',
    'h20166.CSI': 'H20166.INDX',
    'h20170.CSI': 'H20170.INDX',
    'h20171.CSI': 'H20171.INDX',
    'h20173.CSI': 'H20173.INDX',
    'h20174.CSI': 'H20174.INDX',
    'h20175.CSI': 'H20175.INDX',
    'h20177.CSI': 'H20177.INDX',
    'h20179.CSI': 'H20179.INDX',
    'h20180.CSI': 'H20180.INDX',
    'h20182.CSI': 'H20182.INDX',
    'h20183.CSI': 'H20183.INDX',
    'h20184.CSI': 'H20184.INDX',
    'h20186.CSI': 'H20186.INDX',
    'h20187.CSI': 'H20187.INDX',
    'h20196.CSI': 'H20196.INDX',
    'h20211.CSI': 'H20211.INDX',
    'h20910.CSI': 'H20910.INDX',
    '930697.CSI': '930697.INDX',
    'h30192.CSI': 'H30192.XSHG',
    'h30193.CSI': 'H30193.XSHG',
    'h30194.CSI': 'H30194.XSHG',
    'h30195.CSI': 'H30195.XSHG',
    'h30197.CSI': 'H30197.XSHG',
    'h30199.CSI': 'H30199.XSHG',
    'h30200.CSI': 'H30200.XSHG',
    'h30203.CSI': 'H30203.XSHG',
    'h30204.CSI': 'H30204.XSHG',
    'h30206.CSI': 'H30206.XSHG',
    'h30208.CSI': 'H30208.XSHG',
    'h30209.CSI': 'H30209.XSHG',
    'h30214.CSI': 'H30214.XSHG',
    'h30215.CSI': 'H30215.XSHG',
    'h30217.CSI': 'H30217.XSHG',
    'h30218.CSI': 'H30218.XSHG',
    'h30219.CSI': 'H30219.XSHG',
    'h30220.CSI': 'H30220.XSHG',
    'h30221.CSI': 'H30221.XSHG',
    'h30222.CSI': 'H30222.XSHG',
    'h20192.CSI': 'H20192.INDX',
    'h20193.CSI': 'H20193.INDX',
    'h20194.CSI': 'H20194.INDX',
    'h20195.CSI': 'H20195.INDX',
    'h20197.CSI': 'H20197.INDX',
    'h20199.CSI': 'H20199.INDX',
    'h20200.CSI': 'H20200.INDX',
    'h20203.CSI': 'H20203.INDX',
    'h20204.CSI': 'H20204.INDX',
    'h20206.CSI': 'H20206.INDX',
    'h20208.CSI': 'H20208.INDX',
    'h20209.CSI': 'H20209.INDX',
    'h20214.CSI': 'H20214.INDX',
    'h20215.CSI': 'H20215.INDX',
    'h20217.CSI': 'H20217.INDX',
    'h20218.CSI': 'H20218.INDX',
    'h20219.CSI': 'H20219.INDX',
    'h20220.CSI': 'H20220.INDX',
    'h20221.CSI': 'H20221.INDX',
    'h20222.CSI': 'H20222.INDX',
    'h20697.CSI': 'H20697.INDX',
    'h20911.CSI': 'H20911.INDX',
    'h30201.CSI': 'H30201.XSHG',
    'h30207.CSI': 'H30207.XSHG',
    'h30216.CSI': 'H30216.XSHG',
    'h30223.CSI': 'H30223.XSHG',
    'h20168.CSI': 'H20168.INDX',
    'h20201.CSI': 'H20201.INDX',
    'h20207.CSI': 'H20207.INDX',
    'h20216.CSI': 'H20216.INDX',
    'h20223.CSI': 'H20223.INDX',
    'h11030.CSI': 'H11030.XSHG',
    'h11031.CSI': 'H11031.XSHG',
    'h11041.CSI': 'H11041.XSHG',
    'h11042.CSI': 'H11042.XSHG',
    'h11043.CSI': 'H11043.XSHG',
    'h11044.CSI': 'H11044.XSHG',
    'h11045.CSI': 'H11045.XSHG',
    'h11046.CSI': 'H11046.XSHG',
    'h11047.CSI': 'H11047.XSHG',
    'h11049.CSI': 'H11049.XSHG',
    'h11050.CSI': 'H11050.XSHG',
    'h30036.CSI': 'H30036.XSHG',
    'h30037.CSI': 'H30037.XSHG',
    'h30038.CSI': 'H30038.XSHG',
    'h30039.CSI': 'H30039.XSHG',
    'h30040.CSI': 'H30040.XSHG',
    'h30041.CSI': 'H30041.XSHG',
    'h30042.CSI': 'H30042.XSHG',
    'h30043.CSI': 'H30043.XSHG',
    'h30044.CSI': 'H30044.XSHG',
    'h30045.CSI': 'H30045.XSHG',
    'h30046.CSI': 'H30046.XSHG',
    'h30047.CSI': 'H30047.XSHG',
    'h30048.CSI': 'H30048.XSHG',
    'h30049.CSI': 'H30049.XSHG',
    'h30050.CSI': 'H30050.XSHG',
    'h30051.CSI': 'H30051.XSHG',
    'h30052.CSI': 'H30052.XSHG',
    'h30053.CSI': 'H30053.XSHG',
    'h30054.CSI': 'H30054.XSHG',
    'h30055.CSI': 'H30055.XSHG',
    'h30056.CSI': 'H30056.XSHG',
    'h30057.CSI': 'H30057.XSHG',
    'h30058.CSI': 'H30058.XSHG',
    'h30059.CSI': 'H30059.XSHG',
    'h30060.CSI': 'H30060.XSHG',
    'h30061.CSI': 'H30061.XSHG',
    'h30062.CSI': 'H30062.XSHG',
    'h30063.CSI': 'H30063.XSHG',
    'h30064.CSI': 'H30064.XSHG',
    'h30065.CSI': 'H30065.XSHG',
    'h30066.CSI': 'H30066.XSHG',
    'h30067.CSI': 'H30067.XSHG',
    '000925.CSI': '000925.XSHG',
    '000965.CSI': '000965.XSHG',
    '000966.CSI': '000966.XSHG',
    '000967.CSI': '000967.XSHG',
    '930723.CSI': '930723.INDX',
    'h11111.CSI': 'H11111.XSHG',
    'h30362.CSI': 'H30362.XSHG',
    'h30363.CSI': 'H30363.XSHG',
    'h00925.CSI': 'H00925.INDX',
    'h00965.CSI': 'H00965.INDX',
    'h00966.CSI': 'H00966.INDX',
    'h00967.CSI': 'H00967.INDX',
    '000842.CSI': '000842.XSHG',
    '000971.CSI': '000971.XSHG',
    '000981.CSI': '000981.XSHG',
    '000982.CSI': '000982.XSHG',
    '000984.CSI': '000984.XSHG',
    'h30238.CSI': 'H30238.XSHG',
    'h30239.CSI': 'H30239.XSHG',
    'h30248.CSI': 'H30248.XSHG',
    'h30249.CSI': 'H30249.XSHG',
    'h30422.CSI': 'H30422.XSHG',
    'h30438.CSI': 'H30438.XSHG',
    'h00842.CSI': 'H00842.INDX',
    'h00971.CSI': 'H00971.INDX',
    'h00981.CSI': 'H00981.INDX',
    'h00982.CSI': 'H00982.INDX',
    'h00984.CSI': 'H00984.INDX',
    '000843.CSI': '000843.XSHG',
    '000844.CSI': '000844.XSHG',
    'h30087.CSI': 'H30087.XSHG',
    'h30088.CSI': 'H30088.XSHG',
    'h00843.CSI': 'H00843.INDX',
    'h00844.CSI': 'H00844.INDX',
    'h20087.CSI': 'H20087.INDX',
    'h20088.CSI': 'H20088.INDX',
    '000828.CSI': '000828.XSHG',
    '000829.CSI': '000829.XSHG',
    '000830.CSI': '000830.XSHG',
    '000831.CSI': '000831.XSHG',
    'h00828.CSI': 'H00828.INDX',
    'h00829.CSI': 'H00829.INDX',
    'h00830.CSI': 'H00830.INDX',
    'h00831.CSI': 'H00831.INDX',
    '000803.CSI': '000803.XSHG',
    '000804.CSI': '000804.XSHG',
    'h00803.CSI': 'H00803.INDX',
    'h00804.CSI': 'H00804.INDX',
    'h30082.CSI': 'H30082.XSHG',
    'h30083.CSI': 'H30083.XSHG',
    'h30084.CSI': 'H30084.XSHG',
    'h11180.CSI': 'H11180.XSHG',
    '000920.CSI': '000920.XSHG',
    '000921.CSI': '000921.XSHG',
    'h30090.CSI': 'H30090.XSHG',
    'h30091.CSI': 'H30091.XSHG',
    'h30092.CSI': 'H30092.XSHG',
    'h30093.CSI': 'H30093.XSHG',
    'h30094.CSI': 'H30094.XSHG',
    'h30095.CSI': 'H30095.XSHG',
    'h30096.CSI': 'H30096.XSHG',
    'h30097.CSI': 'H30097.XSHG',
    'h30098.CSI': 'H30098.XSHG',
    'h30099.CSI': 'H30099.XSHG',
    'h20090.CSI': 'H20090.INDX',
    'h20091.CSI': 'H20091.INDX',
    'h20092.CSI': 'H20092.INDX',
    'h20093.CSI': 'H20093.INDX',
    'h20094.CSI': 'H20094.INDX',
    'h20095.CSI': 'H20095.INDX',
    'h20096.CSI': 'H20096.INDX',
    'h20097.CSI': 'H20097.INDX',
    'h20098.CSI': 'H20098.INDX',
    'h20099.CSI': 'H20099.INDX',
    '000810.CSI': '000810.XSHG',
    '000811.CSI': '000811.XSHG',
    '000812.CSI': '000812.XSHG',
    '000813.CSI': '000813.XSHG',
    '000814.CSI': '000814.XSHG',
    '000815.CSI': '000815.XSHG',
    '000818.CSI': '000818.XSHG',
    'h00810.CSI': 'H00810.INDX',
    'h00811.CSI': 'H00811.INDX',
    'h00812.CSI': 'H00812.INDX',
    'h00813.CSI': 'H00813.INDX',
    'h00814.CSI': 'H00814.INDX',
    'h00815.CSI': 'H00815.INDX',
    'h00816.CSI': 'H00816.INDX',
    'h00818.CSI': 'H00818.INDX',
    'h30001.CSI': 'H30001.XSHG',
    'h30002.CSI': 'H30002.XSHG',
    'h30004.CSI': 'H30004.XSHG',
    'h30005.CSI': 'H30005.XSHG',
    'h30006.CSI': 'H30006.XSHG',
    'h20001.CSI': 'H20001.INDX',
    'h20002.CSI': 'H20002.INDX',
    'h20004.CSI': 'H20004.INDX',
    'h20005.CSI': 'H20005.INDX',
    'h20006.CSI': 'H20006.INDX',
    'h11051.CSI': 'H11051.XSHG',
    'h11052.CSI': 'H11052.XSHG',
    'h11053.CSI': 'H11053.XSHG',
    'h11054.CSI': 'H11054.XSHG',
    'h11055.CSI': 'H11055.XSHG',
    'h11057.CSI': 'H11057.XSHG',
    'h11058.CSI': 'H11058.XSHG',
    'h11059.CSI': 'H11059.XSHG',
    'h11060.CSI': 'H11060.XSHG',
    'h01051.CSI': 'H01051.INDX',
    'h01052.CSI': 'H01052.INDX',
    'h01053.CSI': 'H01053.INDX',
    'h01054.CSI': 'H01054.INDX',
    'h01055.CSI': 'H01055.INDX',
    'h01057.CSI': 'H01057.INDX',
    'h01058.CSI': 'H01058.INDX',
    'h01059.CSI': 'H01059.INDX',
    'h01060.CSI': 'H01060.INDX',
    'h01113.CSI': 'H01113.INDX',
    'h30137.CSI': 'H30137.XSHG',
    'h30138.CSI': 'H30138.XSHG',
    'h30139.CSI': 'H30139.XSHG',
    'h30140.CSI': 'H30140.XSHG',
    'h30141.CSI': 'H30141.XSHG',
    'h30142.CSI': 'H30142.XSHG',
    'h30143.CSI': 'H30143.XSHG',
    'h20137.CSI': 'H20137.INDX',
    'h20138.CSI': 'H20138.INDX',
    'h20139.CSI': 'H20139.INDX',
    'h20140.CSI': 'H20140.INDX',
    'h20141.CSI': 'H20141.INDX',
    'h20142.CSI': 'H20142.INDX',
    'h20143.CSI': 'H20143.INDX',
    '000942.CSI': '000942.XSHG',
    '000943.CSI': '000943.XSHG',
    '000945.CSI': '000945.XSHG',
    '000947.CSI': '000947.XSHG',
    '000948.CSI': '000948.XSHG',
    '000949.CSI': '000949.XSHG',
    'h00942.CSI': 'H00942.INDX',
    'h00943.CSI': 'H00943.INDX',
    'h00944.CSI': 'H00944.INDX',
    'h00945.CSI': 'H00945.INDX',
    'h00947.CSI': 'H00947.INDX',
    'h00948.CSI': 'H00948.INDX',
    'h00949.CSI': 'H00949.INDX',
    '000821.CSI': '000821.XSHG',
    '000822.CSI': '000822.XSHG',
    '000824.CSI': '000824.XSHG',
    '000825.CSI': '000825.XSHG',
    '000826.CSI': '000826.XSHG',
    'h30073.CSI': 'H30073.XSHG',
    'h30089.CSI': 'H30089.XSHG',
    'h00821.CSI': 'H00821.INDX',
    'h00822.CSI': 'H00822.INDX',
    'h00824.CSI': 'H00824.INDX',
    'h00825.CSI': 'H00825.INDX',
    'h00826.CSI': 'H00826.INDX',
    'h00922.CSI': 'H00922.INDX',
    '000839.CSI': '000839.XSHG',
    '000840.CSI': '000840.XSHG',
    '000926.CSI': '000926.XSHG',
    '000927.CSI': '000927.XSHG',
    '000938.CSI': '000938.XSHG',
    '000953.CSI': '000953.XSHG',
    '000954.CSI': '000954.XSHG',
    '000955.CSI': '000955.XSHG',
    '000956.CSI': '000956.XSHG',
    '930746.CSI': '930746.INDX',
    'h11154.CSI': 'H11154.XSHG',
    'h11155.CSI': 'H11155.XSHG',
    'h30368.CSI': 'H30368.XSHG',
    'h50052.CSI': 'H50052.XSHG',
    'h00839.CSI': 'H00839.INDX',
    'h00840.CSI': 'H00840.INDX',
    'h00926.CSI': 'H00926.INDX',
    'h00927.CSI': 'H00927.INDX',
    'h00938.CSI': 'H00938.INDX',
    'h00939.CSI': 'H00939.INDX',
    'h00953.CSI': 'H00953.INDX',
    'h00954.CSI': 'H00954.INDX',
    'h00955.CSI': 'H00955.INDX',
    'h00956.CSI': 'H00956.INDX',
    'h00958.CSI': 'H00958.INDX',
    '000838.CSI': '000838.XSHG',
    '000846.CSI': '000846.XSHG',
    '000850.CSI': '000850.XSHG',
    '000859.CSI': '000859.XSHG',
    '000860.CSI': '000860.XSHG',
    '000861.CSI': '000861.XSHG',
    '000891.CSI': '000891.XSHG',
    '000922.CSI': '000922.XSHG',
    '000939.CSI': '000939.XSHG',
    '000941.CSI': '000941.XSHG',
    '000950.CSI': '000950.XSHG',
    '000958.CSI': '000958.XSHG',
    '000959.CSI': '000959.XSHG',
    '000961.CSI': '000961.XSHG',
    '000962.CSI': '000962.XSHG',
    '000963.CSI': '000963.XSHG',
    '000964.CSI': '000964.XSHG',
    '000968.CSI': '000968.XSHG',
    '000969.CSI': '000969.XSHG',
    '000970.CSI': '000970.XSHG',
    '000972.CSI': '000972.XSHG',
    '000975.CSI': '000975.XSHG',
    '000977.CSI': '000977.XSHG',
    '000978.CSI': '000978.XSHG',
    '000979.CSI': '000979.XSHG',
    '000998.CSI': '000998.XSHG',
    '930599.CSI': '930599.INDX',
    '930629.CSI': '930629.INDX',
    '930641.CSI': '930641.INDX',
    '930648.CSI': '930648.INDX',
    '930651.CSI': '930651.INDX',
    '930652.CSI': '930652.INDX',
    '930654.CSI': '930654.INDX',
    '930700.CSI': '930700.INDX',
    '930701.CSI': '930701.INDX',
    '930709.CSI': '930709.INDX',
    '930713.CSI': '930713.INDX',
    '930719.CSI': '930719.INDX',
    '930720.CSI': '930720.INDX',
    '930721.CSI': '930721.INDX',
    '930726.CSI': '930726.INDX',
    '930734.CSI': '930734.INDX',
    '930738.CSI': '930738.INDX',
    '930743.CSI': '930743.INDX',
    '930792.CSI': '930792.INDX',
    '930838.CSI': '930838.INDX',
    '930875.CSI': '930875.INDX',
    '930915.CSI': '930915.INDX',
    '930917.CSI': '930917.INDX',
    '930997.CSI': '930997.INDX',
    '930999.CSI': '930999.INDX',
    '931000.CSI': '931000.INDX',
    '931008.CSI': '931008.INDX',
    '931009.CSI': '931009.INDX',
    '931023.CSI': '931023.INDX',
    '931029.CSI': '931029.INDX',
    '931030.CSI': '931030.INDX',
    '931031.CSI': '931031.INDX',
    '931032.CSI': '931032.INDX',
    '931033.CSI': '931033.INDX',
    '931068.CSI': '931068.INDX',
    '931071.CSI': '931071.INDX',
    '931079.CSI': '931079.INDX',
    '931087.CSI': '931087.INDX',
    '931136.CSI': '931136.INDX',
    '931139.CSI': '931139.INDX',
    '931140.CSI': '931140.INDX',
    '931141.CSI': '931141.INDX',
    '931144.CSI': '931144.INDX',
    '931152.CSI': '931152.INDX',
    '931159.CSI': '931159.INDX',
    '931160.CSI': '931160.INDX',
    '931186.CSI': '931186.INDX',
    '931187.CSI': '931187.INDX',
    '931268.CSI': '931268.INDX',
    '931357.CSI': '931357.INDX',
    '931373.CSI': '931373.INDX',
    '931380.CSI': '931380.INDX',
    '931406.CSI': '931406.INDX',
    '950082.CSI': '950082.INDX',
    '950096.CSI': '950096.INDX',
    'h11102.CSI': 'H11102.XSHG',
    'h11103.CSI': 'H11103.XSHG',
    'h11104.CSI': 'H11104.XSHG',
    'h11105.CSI': 'H11105.XSHG',
    'h11106.CSI': 'H11106.XSHG',
    'h11112.CSI': 'H11112.XSHG',
    'h11113.CSI': 'H11113.XSHG',
    'h11114.CSI': 'H11114.XSHG',
    'h11115.CSI': 'H11115.XSHG',
    'h11116.CSI': 'H11116.XSHG',
    'h11121.CSI': 'H11121.XSHG',
    'h11125.CSI': 'H11125.XSHG',
    'h11126.CSI': 'H11126.XSHG',
    'h11136.CSI': 'H11136.XSHG',
    'h11137.CSI': 'H11137.XSHG',
    'h11141.CSI': 'H11141.XSHG',
    'h11142.CSI': 'H11142.XSHG',
    'h11145.CSI': 'H11145.XSHG',
    'h11146.CSI': 'H11146.XSHG',
    'h11147.CSI': 'H11147.XSHG',
    'h11148.CSI': 'H11148.XSHG',
    'h11149.CSI': 'H11149.XSHG',
    'h11150.CSI': 'H11150.XSHG',
    'h11151.CSI': 'H11151.XSHG',
    'h11160.CSI': 'H11160.XSHG',
    'h11161.CSI': 'H11161.XSHG',
    'h11162.CSI': 'H11162.XSHG',
    'h11163.CSI': 'H11163.XSHG',
    'h11166.CSI': 'H11166.XSHG',
    'h11167.CSI': 'H11167.XSHG',
    'h11170.CSI': 'H11170.XSHG',
    'h11171.CSI': 'H11171.XSHG',
    'h11183.CSI': 'H11183.XSHG',
    'h11184.CSI': 'H11184.XSHG',
    'h30007.CSI': 'H30007.XSHG',
    'h30011.CSI': 'H30011.XSHG',
    'h30012.CSI': 'H30012.XSHG',
    'h30015.CSI': 'H30015.XSHG',
    'h30035.CSI': 'H30035.XSHG',
    'h30068.CSI': 'H30068.XSHG',
    'h30074.CSI': 'H30074.XSHG',
    'h30079.CSI': 'H30079.XSHG',
    'h30080.CSI': 'H30080.XSHG',
    'h30081.CSI': 'H30081.XSHG',
    'h30085.CSI': 'H30085.XSHG',
    'h30100.CSI': 'H30100.XSHG',
    'h30101.CSI': 'H30101.XSHG',
    'h30102.CSI': 'H30102.XSHG',
    'h30103.CSI': 'H30103.XSHG',
    'h30104.CSI': 'H30104.XSHG',
    'h30105.CSI': 'H30105.XSHG',
    'h30106.CSI': 'H30106.XSHG',
    'h30107.CSI': 'H30107.XSHG',
    'h30109.CSI': 'H30109.XSHG',
    'h30110.CSI': 'H30110.XSHG',
    'h30111.CSI': 'H30111.XSHG',
    'h30112.CSI': 'H30112.XSHG',
    'h30113.CSI': 'H30113.XSHG',
    'h30114.CSI': 'H30114.XSHG',
    'h30115.CSI': 'H30115.XSHG',
    'h30116.CSI': 'H30116.XSHG',
    'h30117.CSI': 'H30117.XSHG',
    'h30119.CSI': 'H30119.XSHG',
    'h30131.CSI': 'H30131.XSHG',
    'h30132.CSI': 'H30132.XSHG',
    'h30169.CSI': 'H30169.XSHG',
    'h30172.CSI': 'H30172.XSHG',
    'h30176.CSI': 'H30176.XSHG',
    'h30178.CSI': 'H30178.XSHG',
    'h30188.CSI': 'H30188.XSHG',
    'h30190.CSI': 'H30190.XSHG',
    'h30202.CSI': 'H30202.XSHG',
    'h30213.CSI': 'H30213.XSHG',
    'h30256.CSI': 'H30256.XSHG',
    'h30275.CSI': 'H30275.XSHG',
    'h30276.CSI': 'H30276.XSHG',
    'h30277.CSI': 'H30277.XSHG',
    'h30334.CSI': 'H30334.XSHG',
    'h30335.CSI': 'H30335.XSHG',
    'h30336.CSI': 'H30336.XSHG',
    'h30337.CSI': 'H30337.XSHG',
    'h30338.CSI': 'H30338.XSHG',
    'h30339.CSI': 'H30339.XSHG',
    'h30340.CSI': 'H30340.XSHG',
    'h30341.CSI': 'H30341.XSHG',
    'h30342.CSI': 'H30342.XSHG',
    'h30344.CSI': 'H30344.XSHG',
    'h30350.CSI': 'H30350.XSHG',
    'h30360.CSI': 'H30360.XSHG',
    'h30361.CSI': 'H30361.XSHG',
    'h30365.CSI': 'H30365.XSHG',
    'h30366.CSI': 'H30366.XSHG',
    'h30372.CSI': 'H30372.XSHG',
    'h30401.CSI': 'H30401.XSHG',
    'h30402.CSI': 'H30402.XSHG',
    'h30456.CSI': 'H30456.XSHG',
    'h30457.CSI': 'H30457.XSHG',
    'h30458.CSI': 'H30458.XSHG',
    'h30459.CSI': 'H30459.XSHG',
    'h30460.CSI': 'H30460.XSHG',
    'h30461.CSI': 'H30461.XSHG',
    'h30462.CSI': 'H30462.XSHG',
    'h30463.CSI': 'H30463.XSHG',
    'h30464.CSI': 'H30464.XSHG',
    'h30465.CSI': 'H30465.XSHG',
    'h30466.CSI': 'H30466.XSHG',
    'h30467.CSI': 'H30467.XSHG',
    'h30468.CSI': 'H30468.XSHG',
    'h30469.CSI': 'H30469.XSHG',
    'h30470.CSI': 'H30470.XSHG',
    'h30471.CSI': 'H30471.XSHG',
    'h30472.CSI': 'H30472.XSHG',
    'h30473.CSI': 'H30473.XSHG',
    'h30474.CSI': 'H30474.XSHG',
    'h30475.CSI': 'H30475.XSHG',
    'h30476.CSI': 'H30476.XSHG',
    'h30477.CSI': 'H30477.XSHG',
    'h30478.CSI': 'H30478.XSHG',
    'h30479.CSI': 'H30479.XSHG',
    'h30480.CSI': 'H30480.XSHG',
    'h30481.CSI': 'H30481.XSHG',
    'h30482.CSI': 'H30482.XSHG',
    'h30483.CSI': 'H30483.XSHG',
    'h30484.CSI': 'H30484.XSHG',
    'h30485.CSI': 'H30485.XSHG',
    'h30486.CSI': 'H30486.XSHG',
    'h30487.CSI': 'H30487.XSHG',
    'h30488.CSI': 'H30488.XSHG',
    'h30489.CSI': 'H30489.XSHG',
    'h30490.CSI': 'H30490.XSHG',
    'h30491.CSI': 'H30491.XSHG',
    'h30492.CSI': 'H30492.XSHG',
    'h30493.CSI': 'H30493.XSHG',
    'h30494.CSI': 'H30494.XSHG',
    'h30495.CSI': 'H30495.XSHG',
    'h30496.CSI': 'H30496.XSHG',
    'h30497.CSI': 'H30497.XSHG',
    'h30498.CSI': 'H30498.XSHG',
    'h30499.CSI': 'H30499.XSHG',
    'h30500.CSI': 'H30500.XSHG',
    'h30501.CSI': 'H30501.XSHG',
    'h30502.CSI': 'H30502.XSHG',
    'h30503.CSI': 'H30503.XSHG',
    'h30504.CSI': 'H30504.XSHG',
    'h30505.CSI': 'H30505.XSHG',
    'h30506.CSI': 'H30506.XSHG',
    'h30507.CSI': 'H30507.XSHG',
    'h30508.CSI': 'H30508.XSHG',
    'h30509.CSI': 'H30509.XSHG',
    'h30510.CSI': 'H30510.XSHG',
    'h30511.CSI': 'H30511.XSHG',
    'h30512.CSI': 'H30512.XSHG',
    'h30513.CSI': 'H30513.XSHG',
    'h30514.CSI': 'H30514.XSHG',
    'h30515.CSI': 'H30515.XSHG',
    'h30516.CSI': 'H30516.XSHG',
    'h30517.CSI': 'H30517.XSHG',
    'h30518.CSI': 'H30518.XSHG',
    'h30519.CSI': 'H30519.XSHG',
    'h30520.CSI': 'H30520.XSHG',
    'h30531.CSI': 'H30531.XSHG',
    'h30532.CSI': 'H30532.XSHG',
    'h30533.CSI': 'H30533.XSHG',
    'h30534.CSI': 'H30534.XSHG',
    'h30535.CSI': 'H30535.XSHG',
    'h30537.CSI': 'H30537.XSHG',
    'h30544.CSI': 'H30544.XSHG',
    'h30545.CSI': 'H30545.XSHG',
    'h30546.CSI': 'H30546.XSHG',
    'h30547.CSI': 'H30547.XSHG',
    'h30548.CSI': 'H30548.XSHG',
    'h30550.CSI': 'H30550.XSHG',
    'h30551.CSI': 'H30551.XSHG',
    'h30552.CSI': 'H30552.XSHG',
    'h30554.CSI': 'H30554.XSHG',
    'h30555.CSI': 'H30555.XSHG',
    'h30556.CSI': 'H30556.XSHG',
    'h30557.CSI': 'H30557.XSHG',
    'h30558.CSI': 'H30558.XSHG',
    'h30559.CSI': 'H30559.XSHG',
    'h30560.CSI': 'H30560.XSHG',
    'h30561.CSI': 'H30561.XSHG',
    'h30562.CSI': 'H30562.XSHG',
    'h30563.CSI': 'H30563.XSHG',
    'h30564.CSI': 'H30564.XSHG',
    'h30565.CSI': 'H30565.XSHG',
    'h30566.CSI': 'H30566.XSHG',
    'h30567.CSI': 'H30567.XSHG',
    'h30568.CSI': 'H30568.XSHG',
    'h30569.CSI': 'H30569.XSHG',
    'h30570.CSI': 'H30570.XSHG',
    'h30572.CSI': 'H30572.XSHG',
    'h30573.CSI': 'H30573.XSHG',
    'h30574.CSI': 'H30574.XSHG',
    'h30576.CSI': 'H30576.XSHG',
    'h30577.CSI': 'H30577.XSHG',
    'h30578.CSI': 'H30578.XSHG',
    'h30579.CSI': 'H30579.XSHG',
    'h30580.CSI': 'H30580.XSHG',
    'h30581.CSI': 'H30581.XSHG',
    'h30582.CSI': 'H30582.XSHG',
    'h30583.CSI': 'H30583.XSHG',
    'h30584.CSI': 'H30584.XSHG',
    'h30585.CSI': 'H30585.XSHG',
    'h30586.CSI': 'H30586.XSHG',
    'h30587.CSI': 'H30587.XSHG',
    'h30588.CSI': 'H30588.XSHG',
    'h30590.CSI': 'H30590.XSHG',
    'h30597.CSI': 'H30597.XSHG',
    'h50036.CSI': 'H50036.XSHG',
    'h50043.CSI': 'H50043.XSHG',
    'h50044.CSI': 'H50044.XSHG',
    'h50053.CSI': 'H50053.XSHG',
    'h50054.CSI': 'H50054.XSHG',
    'h50055.CSI': 'H50055.XSHG',
    'h50056.CSI': 'H50056.XSHG',
    'h50059.CSI': 'H50059.XSHG',
    'h50060.CSI': 'H50060.XSHG',
    'h50066.CSI': 'H50066.XSHG',
    'h50069.CSI': 'H50069.XSHG',
    'h00801.CSI': 'H00801.INDX',
    'h00802.CSI': 'H00802.INDX',
    'h00805.CSI': 'H00805.INDX',
    'h00806.CSI': 'H00806.INDX',
    'h00827.CSI': 'H00827.INDX',
    'h00838.CSI': 'H00838.INDX',
    'h00941.CSI': 'H00941.INDX',
    'h00950.CSI': 'H00950.INDX',
    'h00961.CSI': 'H00961.INDX',
    'h00962.CSI': 'H00962.INDX',
    'h00963.CSI': 'H00963.INDX',
    'h00964.CSI': 'H00964.INDX',
    'h00968.CSI': 'H00968.INDX',
    'h00969.CSI': 'H00969.INDX',
    'h00970.CSI': 'H00970.INDX',
    'h00972.CSI': 'H00972.INDX',
    'h00977.CSI': 'H00977.INDX',
    'h00978.CSI': 'H00978.INDX',
    'h00979.CSI': 'H00979.INDX',
    'h00998.CSI': 'H00998.INDX',
    'h20007.CSI': 'H20007.INDX',
    'h20033.CSI': 'H20033.INDX',
    'h20035.CSI': 'H20035.INDX',
    'h20068.CSI': 'H20068.INDX',
    'h20073.CSI': 'H20073.INDX',
    'h20074.CSI': 'H20074.INDX',
    'h20079.CSI': 'H20079.INDX',
    'h20080.CSI': 'H20080.INDX',
    'h20081.CSI': 'H20081.INDX',
    'h20089.CSI': 'H20089.INDX',
    'h30230.CSI': 'H30230.XSHG',
    'h30231.CSI': 'H30231.XSHG',
    'h30232.CSI': 'H30232.XSHG',
    'h30233.CSI': 'H30233.XSHG',
    'h30234.CSI': 'H30234.XSHG',
    'h30235.CSI': 'H30235.XSHG',
    'h30236.CSI': 'H30236.XSHG',
    'h30237.CSI': 'H30237.XSHG',
    'h30240.CSI': 'H30240.XSHG',
    'h30241.CSI': 'H30241.XSHG',
    'h30242.CSI': 'H30242.XSHG',
    'h30243.CSI': 'H30243.XSHG',
    'h30244.CSI': 'H30244.XSHG',
    'h30245.CSI': 'H30245.XSHG',
    'h30246.CSI': 'H30246.XSHG',
    'h30247.CSI': 'H30247.XSHG',
    'h11172.CSI': 'H11172.XSHG',
    'h11173.CSI': 'H11173.XSHG',
    'h11174.CSI': 'H11174.XSHG',
    'h11175.CSI': 'H11175.XSHG',
    'h11176.CSI': 'H11176.XSHG',
    'h11177.CSI': 'H11177.XSHG',
    'h11178.CSI': 'H11178.XSHG',
    'h11179.CSI': 'H11179.XSHG',
    'h01172.CSI': 'H01172.INDX',
    'h01173.CSI': 'H01173.INDX',
    'h01174.CSI': 'H01174.INDX',
    'h01175.CSI': 'H01175.INDX',
    'h01176.CSI': 'H01176.INDX',
    'h01177.CSI': 'H01177.INDX',
    'h01178.CSI': 'H01178.INDX',
    'h01179.CSI': 'H01179.INDX',
    'h20100.CSI': 'H20100.INDX',
    'h20101.CSI': 'H20101.INDX',
    'h20102.CSI': 'H20102.INDX',
    'h20103.CSI': 'H20103.INDX',
    'h20104.CSI': 'H20104.INDX',
    'h20105.CSI': 'H20105.INDX',
    'h20106.CSI': 'H20106.INDX',
    'h20107.CSI': 'H20107.INDX',
    'h20109.CSI': 'H20109.INDX',
    'h20110.CSI': 'H20110.INDX',
    'h20111.CSI': 'H20111.INDX',
    'h20112.CSI': 'H20112.INDX',
    'h20113.CSI': 'H20113.INDX',
    'h20114.CSI': 'H20114.INDX',
    'h20115.CSI': 'H20115.INDX',
    'h20116.CSI': 'H20116.INDX',
    'h20117.CSI': 'H20117.INDX',
    'h20119.CSI': 'H20119.INDX',
    'h01142.CSI': 'H01142.INDX',
    'h01143.CSI': 'H01143.INDX',
    'h01144.CSI': 'H01144.INDX',
    'h01145.CSI': 'H01145.INDX',
    'h01146.CSI': 'H01146.INDX',
    'h01147.CSI': 'H01147.INDX',
    'h01148.CSI': 'H01148.INDX',
    'h01149.CSI': 'H01149.INDX',
    'h01150.CSI': 'H01150.INDX',
    'h01151.CSI': 'H01151.INDX',
    'h01102.CSI': 'H01102.INDX',
    'h01103.CSI': 'H01103.INDX',
    'h01104.CSI': 'H01104.INDX',
    'h01105.CSI': 'H01105.INDX',
    'h01106.CSI': 'H01106.INDX',
    'h01112.CSI': 'H01112.INDX',
    'h01114.CSI': 'H01114.INDX',
    'h01115.CSI': 'H01115.INDX',
    'h01116.CSI': 'H01116.INDX',
    'h01138.CS': 'H01138.INDX',
    'h11101.CSI': 'H11101.XSHG',
    'h11108.CSI': 'H11108.XSHG',
    'h11118.CSI': 'H11118.XSHG',
    'h11128.CSI': 'H11128.XSHG',
    'h11138.CSI': 'H11138.XSHG',
    'h11164.CSI': 'H11164.XSHG',
    'h11165.CSI': 'H11165.XSHG',
    'h11168.CSI': 'H11168.XSHG',
    'h11169.CSI': 'H11169.XSHG',
    'h01108.CSI': 'H01108.INDX',
    'h01118.CSI': 'H01118.INDX',
    'h01120.CSI': 'H01120.INDX',
    'h01123.CSI': 'H01123.INDX',
    'h01124.CSI': 'H01124.INDX',
    'h01125.CSI': 'H01125.INDX',
    'h01126.CSI': 'H01126.INDX',
    'h01128.CSI': 'H01128.INDX',
    'h01164.CSI': 'H01164.INDX',
    'h01165.CSI': 'H01165.INDX',
    'h01166.CSI': 'H01166.INDX',
    'h01167.CSI': 'H01167.INDX',
    'h01168.CSI': 'H01168.INDX',
    'h01169.CSI': 'H01169.INDX',
    'h01110.CSI': 'H01110.INDX',
    'h01111.CSI': 'H01111.INDX',
    'h11156.CSI': 'H11156.XSHG',
    'h11157.CSI': 'H11157.XSHG',
    'h11158.CSI': 'H11158.XSHG',
    'h11159.CSI': 'H11159.XSHG',
    'h01134.CSI': 'H01134.INDX',
    'h01135.CSI': 'H01135.INDX',
    'h01136.CSI': 'H01136.INDX',
    'h01137.CSI': 'H01137.INDX',
    'h01140.CSI': 'H01140.INDX',
    'h01141.CSI': 'H01141.INDX',
    'h01152.CSI': 'H01152.INDX',
    'h01153.CSI': 'H01153.INDX',
    'h01154.CSI': 'H01154.INDX',
    'h01155.CSI': 'H01155.INDX',
    'h01156.CSI': 'H01156.INDX',
    'h01157.CSI': 'H01157.INDX',
    'h01158.CSI': 'H01158.INDX',
    'h01159.CSI': 'H01159.INDX',
    'h01160.CSI': 'H01160.INDX',
    'h01161.CSI': 'H01161.INDX',
    'h01162.CSI': 'H01162.INDX',
    'h01163.CSI': 'H01163.INDX',
    'h20131.CSI': 'H20131.INDX',
    'h20132.CSI': 'H20132.INDX',
    'h11134.CSI': 'H11134.XSHG',
    'h11135.CSI': 'H11135.XSHG',
    'h01181.CSI': 'H01181.INDX',
    'h01182.CSI': 'H01182.INDX',
    'h01183.CSI': 'H01183.INDX',
    'h01184.CSI': 'H01184.INDX',
    'h20133.CSI': 'H20133.INDX',
    'h20134.CSI': 'H20134.INDX',
    'h20135.CSI': 'H20135.INDX',
    'h20136.CSI': 'H20136.INDX',
    'h11181.CSI': 'H11181.XSHG',
    'h11182.CSI': 'H11182.XSHG',
    'h30133.CSI': 'H30133.XSHG',
    'h30134.CSI': 'H30134.XSHG',
    'h30135.CSI': 'H30135.XSHG',
    'h30136.CSI': 'H30136.XSHG',
    'h11020.CSI': 'H11020.XSHG',
    'h11021.CSI': 'H11021.XSHG',
    'h11022.CSI': 'H11022.XSHG',
    'h11023.CSI': 'H11023.XSHG',
    'h11024.CSI': 'H11024.XSHG',
    'h11025.CSI': 'H11025.XSHG',
    'h11026.CSI': 'H11026.XSHG',
    'h11027.CSI': 'H11027.XSHG',
    'h11028.CSI': 'H11028.XSHG',
    'h20267.CSI': 'H20267.INDX',
    'h20268.CSI': 'H20268.INDX',
    'h30267.CSI': 'H30267.XSHG',
    'h30268.CSI': 'H30268.XSHG',
    '921374.CSI': '921374.INDX',
    '921496.CSI': '921496.INDX',
    '921496HKD.CSI': '921496HKD.INDX',
    'h11001.CSI': 'H11001.XSHG',
    'h11002.CSI': 'H11002.XSHG',
    'h11003.CSI': 'H11003.XSHG',
    'h11004.CSI': 'H11004.XSHG',
    'h11005.CSI': 'H11005.XSHG',
    'h11009.CSI': 'H11009.XSHG',
    'h11010.CSI': 'H11010.XSHG',
    'h11015.CSI': 'H11015.XSHG',
    'h11016.CSI': 'H11016.XSHG',
    'h11076.CSI': 'H11076.XSHG',
    '930871.CSI': '930871.INDX',
    '930872.CSI': '930872.INDX',
    '930873.CSI': '930873.INDX',
    '930874.CSI': '930874.INDX',
    'h11006.CSI': 'H11006.XSHG',
    'h11017.CSI': 'H11017.XSHG',
    'h11071.CSI': 'H11071.XSHG',
    'h11075.CSI': 'H11075.XSHG',
    'h11099.CSI': 'H11099.XSHG',
    '000833.CSI': '000833.XSHG',
    '000845.CSI': '000845.XSHG',
    '930780.CSI': '930780.INDX',
    'h11007.CSI': 'H11007.XSHG',
    'h11008.CSI': 'H11008.XSHG',
    'h11014.CSI': 'H11014.XSHG',
    'h11018.CSI': 'H11018.XSHG',
    'h11019.CSI': 'H11019.XSHG',
    'h11070.CSI': 'H11070.XSHG',
    'h11072.CSI': 'H11072.XSHG',
    'h11073.CSI': 'H11073.XSHG',
    'h11074.CSI': 'H11074.XSHG',
    'h11078.CSI': 'H11078.XSHG',
    'h11079.CSI': 'H11079.XSHG',
    'h11087.CSI': 'H11087.XSHG',
    'h11088.CSI': 'H11088.XSHG',
    'h11089.CSI': 'H11089.XSHG',
    'h11090.CSI': 'H11090.XSHG',
    'h11091.CSI': 'H11091.XSHG',
    'h11092.CSI': 'H11092.XSHG',
    'h11093.CSI': 'H11093.XSHG',
    'h11094.CSI': 'H11094.XSHG',
    'h11096.CSI': 'H11096.XSHG',
    'h11097.CSI': 'H11097.XSHG',
    'h11185.CSI': 'H11185.XSHG',
    'h30396.CSI': 'H30396.XSHG',
    'h30521.CSI': 'H30521.XSHG',
    '000832.CSI': '000832.XSHG',
    '000923.CSI': '000923.XSHG',
    '930849.CSI': '930849.INDX',
    '930865.CSI': '930865.INDX',
    '930866.CSI': '930866.INDX',
    '930916.CSI': '930916.INDX',
    '930954.CSI': '930954.INDX',
    '931018.CSI': '931018.INDX',
    '931078.CSI': '931078.INDX',
    '931162.CSI': '931162.INDX',
    '931172.CSI': '931172.INDX',
    '931175.CSI': '931175.INDX',
    '950045.CSI': '950045.INDX',
}


_to_wind_index_map = {j: i for i, j in _wind_index_map.items()}


@ttl_cache(8 * 3600)
def _all_futures():
    # cache futures
    df = all_instruments('Future')
    r = {i.upper(): i for i in df['order_book_id'].tolist()}
    s = df.set_index('order_book_id')['trading_code']
    s = s[~s.index.str.endswith(('88', '99', '888', '889', '88A2', '88A3'))]
    s = s.sort_index().drop_duplicates(keep='last').str.upper()
    r.update({v: k for k, v in s.items()})
    # cache commodity options
    df = all_instruments('Option')
    df = df[~df.exchange.isin(('XSHG', 'XSHE'))]
    r.update({i.upper(): i for i in df['order_book_id'].tolist()})
    s = df.set_index('order_book_id')['trading_code']
    s = s.sort_index().drop_duplicates(keep='last').str.upper()
    r.update({v: k for k, v in s.items()})
    return r


def _convert_to_wind(order_book_id):
    if order_book_id.endswith(".XSHE"):
        return order_book_id[:-4] + "SZ"
    elif order_book_id.endswith(".XSHG"):
        return order_book_id[:-4] + "SH"
    elif order_book_id.endswith(".XHKG"):
        return order_book_id[:-4] + "HK"
    elif order_book_id.endswith(".BJSE"):
        return order_book_id[:-4] + "BJ"

    inst = instruments(order_book_id)
    inst_type, exchange = inst.type, inst.exchange
    if inst_type == 'Future':
        if exchange != 'CZCE' or order_book_id.endswith(('88', '99', '888', '889')):
            return order_book_id + '.' + _wind_exchange_map[exchange]
        return order_book_id[:-4] + order_book_id[-3:] + '.CZC'
    if inst_type == 'Spot':
        return order_book_id[:-1]
    if inst_type == 'Option':
        if exchange == 'XSHE':
            return order_book_id + '.SZ'
        if exchange == 'XSHG':
            return order_book_id + '.SH'
        return inst.trading_code.upper() + '.' + _wind_exchange_map[exchange]

    if inst_type == 'INDX':
        if order_book_id[0] == 'C':
            return order_book_id[:-4] + 'WI'
        if order_book_id[0] == '8':
            return order_book_id[:-4] + 'SI'
        if order_book_id in _to_wind_index_map:
            return _to_wind_index_map[order_book_id]
        if exchange == 'XSHE':
            return order_book_id[:-4].lower() + 'SZ'
        return order_book_id[:-4].lower() + 'SH'
    return order_book_id


_wind_reg = re.compile(r'((?P<future>[A-Z]+\d{3,4})|((?P<option_id>((\d{8})|((?P<option>[A-Z]{1,2})(?P<option_suffix>\d{3,4}\-?(P|C)\-?\d{3,}))))))\.?(?P<ex>(CFE|SHF|INE|DCE|CZC|SH|SZ))')
_future_re = re.compile(r'^([A-Z]+\d+F?((MS)?[PC]\w+)?)')
_current_year = str((datetime.date.today().year % 100) // 10)


def _id_convert_one(order_book_id):  # noqa: C901
    # hard code
    if order_book_id in {"T00018", "T00018.SH", "T00018.XSHG", "SH.T00018"}:
        return "990018.XSHG"
    if order_book_id.endswith(".XHKG"):
        return order_book_id
    inst = instruments(order_book_id)
    if inst is not None:
        return inst.order_book_id

    # WIND Future & Option
    r = _wind_reg.match(order_book_id)
    if r:
        d = r.groupdict()
        if d['future']:
            return _all_futures().get(d['future'].upper(), d['future'])
        if not d['option']:
            return d['option_id']

        if d['ex'] != 'CZC':
            return r['option_id'].replace('-', '')
        return d['option'] + _current_year + d['option_suffix'].replace('-', '')

    # WIND ZZ INDX
    if order_book_id in _wind_index_map:
        return _wind_index_map[order_book_id]

    # WIND SH INDX
    if order_book_id[-3:] == '.SH':
        if order_book_id[:2] in ('h0', 'h4'):
            return order_book_id[:-2].upper() + 'INDX'

    # WIND SW, ZX INDEX
    if (order_book_id[-3:] == '.WI' and order_book_id[0] == 'C') or (
            order_book_id[0] == '8' and order_book_id[-3:] == '.SI'):
        return order_book_id[:-2] + 'INDX'

    # WIND Spot
    if order_book_id[-4:] == '.SGE':
        return order_book_id + 'X'

    if order_book_id.isdigit():
        # 北交所合约原先8/4开头的现在都变更为920开头 这三个退市合约没变更，保留转换
        if order_book_id.startswith('920') or order_book_id in ['832317', '833874', '833994']:
            return order_book_id + '.BJSE'
        if order_book_id.startswith(("0", "3", "15")):
            return order_book_id + ".XSHE"
        elif order_book_id.startswith(("5", "6", "9")):
            return order_book_id + ".XSHG"
        else:
            raise ValueError("order_book_ids should be str like 000001, 600000")

    order_book_id = order_book_id.upper()
    if order_book_id.endswith(".XSHG") or order_book_id.endswith(".XSHE"):
        return order_book_id

    if order_book_id.startswith(("SZ", "SH", "BJ")):
        suffix = order_book_id.replace(".", "")[2:]
        prefix = order_book_id[:2]   # it's SZ or SH
        # maybe CS order_book_id
        if len(suffix) == 6 and suffix.isdigit():
            return suffix + (".XSHG" if prefix == "SH" else (".XSHE" if prefix == "SZ" else ".BJSE"))
    elif order_book_id.endswith("SZ"):
        return order_book_id.replace(".", "")[:-2] + ".XSHE"
    elif order_book_id.endswith("SH"):
        return order_book_id.replace(".", "")[:-2] + ".XSHG"
    elif order_book_id.endswith("BJ"):
        return order_book_id.replace(".", "")[:-2] + ".BJSE"

    # 期货 & 商品期权
    order_book_id = order_book_id.replace('-', '').split(".")[0]
    m = _future_re.match(order_book_id)
    if m:
        i = m.groups()[0]
        return _all_futures().get(i, order_book_id)

    raise ValueError("unknown order_book_id: {}".format(order_book_id))


@export_as_api
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def id_convert(order_book_ids, to=None):
    """
    将交易所和其他平台的股票代码转换成米筐的标准合约代码，目前仅支持 A 股、期货和期权代码转换。

    例如, 支持转换类型包括 000001.SZ, 000001SZ, SZ000001 转换为 000001.XSHE

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码(来自米筐或交易所或其他平台)
    to : str, optional
        'normal'：由米筐代码转化为交易所和其他平台的代码
        不填：由交易所和其他平台的代码转化为米筐代码

    Returns
    -------
    str | list[str]
        传入一个 order_book_ids，函数会返回一个标准化合约代码字符串。
        传入一个 order_book_ids 列表，函数会返回一个标准化合约代码字符串 list。

    Examples
    --------
    获取其他平台标准合约代码:

    >>> id_convert('000001.XSHE', to='normal')
    '000001.SZ'

    获取多个股票的米筐标准合约代码:

    >>> id_convert(['000001.SZ', '000935.SH'])
    ['000001.XSHE', '000935.XSHG']

    """
    if to == 'normal' or to == 'wind':
        _convert_one = lambda o: _convert_to_wind(_id_convert_one(o))
    elif to is None or to == 'ricequant':
        _convert_one = _id_convert_one
    else:
        raise ValueError('Unsupported destination: {}'.format(to))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if isinstance(order_book_ids, six.string_types):
            return _convert_one(order_book_ids)
        elif isinstance(order_book_ids, list):
            return [_convert_one(o) for o in order_book_ids]
        else:
            raise ValueError("order_book_ids should be str or list")


def _id_compatible(order_book_id):
    if order_book_id.endswith("XSHE"):
        return order_book_id[:-4] + "SZ"
    elif order_book_id.endswith("XSHG"):
        return order_book_id[:-4] + "SH"
    else:
        return order_book_id


def _all_instruments_list(type_, market):
    ins = [
        Instrument(i)
        for i in get_client().execute(
            "all_instruments_by_type", instrument_type=type_, market=market
        )
    ]
    ins.sort(key=lambda i: i.order_book_id)
    return ins


@ttl_cache(3 * 3600)
def _all_cached_instruments_list(type_, market):
    return _all_instruments_list(type_, market)


@ttl_cache(3 * 3600)
def _all_obid_to_type(market):
    simple_insts = get_client().execute("all_obid_type_list", market)
    result = {}
    for inst in simple_insts:
        # 统一一下值的类型, 后面使用起来简单点
        if market == "hk":
            result[inst["unique_id"]] = (inst["type"], inst["unique_id"])
        result[inst["order_book_id"]] = (inst["type"], inst["order_book_id"])
        result[inst["symbol"]] = (inst["type"], inst["order_book_id"])
    return result


@ttl_cache(3 * 3600)
def _all_instruments_dict(type_, market):
    ins = _all_cached_instruments_list(type_, market)
    result = dict()
    for i in ins:
        if i.type == "Convertible":
            result[_id_compatible(i.order_book_id)] = i

        if getattr(i, "unique_id", None):  # 对港股 unique_id 作为 key 添加到 result dict
            result[i.unique_id] = i

        if i.order_book_id in result:  # 对港股存在退市后 order_book_id 复用的情况只存上市日期最晚的信息
            if i.listed_date > result[i.order_book_id].listed_date:
                result[i.order_book_id] = i
        else:
            result[i.order_book_id] = i

    try:
        result["沪深300"] = result["000300.XSHG"]
        result["中证500"] = result["000905.XSHG"]
        result[result["SSE180.INDX"].symbol] = result["000010.XSHG"]
    except KeyError:
        pass

    return result


def get_underlying_listed_date(underlying_symbol, ins_type, market="cn"):
    """ 获取期货或者期权的某个品种的上市日期"""
    ins_list = _all_cached_instruments_list(ins_type, market)
    listed_dates = [i.listed_date for i in ins_list
                    if (getattr(i, "underlying_symbol", "") == underlying_symbol
                        and i.type == ins_type and i.listed_date != "0000-00-00")]

    return min(listed_dates)


@ttl_cache(3 * 3600)
def hk_all_unique_id_to_order_book_id():
    insts_list = _all_cached_instruments_list("CS", "hk")
    insts_list.extend(_all_cached_instruments_list("ETF", "hk"))
    return {i.unique_id: i.order_book_id for i in insts_list if i.unique_id.endswith(".XHKG")}


@export_as_api
@support_hk_order_book_id
def get_tick_size(order_book_ids, market="cn"):
    """获取合约价格最小变动单位

    :param order_book_ids: list/str 如: ['FU1703', 'CU2201']
    :param market: 如：'cn' (Default value = "cn")
    :returns: pandas.Series
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    insts = instruments(order_book_ids)
    data = {ins.order_book_id: ins.tick_size() for ins in insts if ins.type not in ("Future", "Option", "Spot")}
    obs = [ins.order_book_id for ins in insts if ins.type in ("Future", "Option", "Spot")]
    if obs:
        data.update(get_client().execute("get_tick_sizes", obs, market=market))
    s = pd.Series(data, name='tick_size')
    s.index.name = 'order_book_id'
    return s


HK_STOCK_PRICE_SECTIONS = [0.25, 0.5, 10, 20, 100, 200, 500, 1000, 2000, 5000]
HK_STOCK_TICK_SIZES = [0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5]


# noinspection All
class Instrument(object):
    def __init__(self, d):
        self.__dict__ = d

    def __repr__(self):
        if self.has_citics_info() and not hasattr(self, "_citics_industry_code"):
            self.citics_industry()

        return "{}({})".format(
            type(self).__name__,
            ", ".join(
                [
                    "{}={!r}".format(k.lstrip("_"), v)
                    for k, v in self.__dict__.items()
                    if v is not None
                ]
            ),
        )

    @property
    def concept_names(self):
        return get_concept_names(self.order_book_id)

    def days_from_listed(self, date=None):
        if self.listed_date == "0000-00-00":
            return -1

        date = to_date(date) if date else datetime.date.today()
        if self.de_listed_date != "0000-00-00" and date > to_date(self.de_listed_date):
            # 晚于退市日期
            return -1

        listed_date = to_date(self.listed_date)
        ipo_days = (date - listed_date).days
        return ipo_days if ipo_days >= 0 else -1

    def days_to_expire(self, date=None):
        if getattr(self, 'maturity_date', '0000-00-00') == '0000-00-00':
            return -1

        date = to_date(date) if date else datetime.date.today()

        maturity_date = to_date(self.maturity_date)
        days = (maturity_date - date).days
        return days if days >= 0 else -1

    def tick_size(self, price=None):
        if self.exchange == "XHKG":
            check_type(price, (int, float), "price")
            index = bisect.bisect_left(HK_STOCK_PRICE_SECTIONS, price)
            return HK_STOCK_TICK_SIZES[index]
        elif self.type in ["CS", "INDX"]:
            return 0.01
        elif self.type in ["ETF", "LOF", "REITs", "FUND", "FenjiB", "FenjiA", "FenjiMu", "PublicFund"]:
            return 0.001
        elif self.type == "Convertible":
            return 0.001
        elif self.type not in ["Future", "Option", "Spot"]:
            return -1
        return get_client().execute("get_tick_size", self.order_book_id, market='cn')

    def has_citics_info(self):
        return self.type == "CS" and self.exchange in {"XSHE", "XSHG"}

    def citics_industry(self, date=None):
        if self.has_citics_info():
            if date is None:
                if hasattr(self, "_citics_industry_code"):
                    return (self._citics_industry_code, self._citics_industry_name)

            if self.de_listed_date != '0000-00-00':
                date = get_previous_trading_date(self.de_listed_date)

            result = get_instrument_industry(self.order_book_id, date=date, level=1, source='citics_2019')
            if result is None:
                self._citics_industry_code, self._citics_industry_name = (None, None)
                return None

            self._citics_industry_code = result['first_industry_code'][0]
            self._citics_industry_name = result['first_industry_name'][0]

            return {"code": result.iloc[0, 0], "name": result.iloc[0, 1]}

    @property
    def citics_industry_code(self):
        if not self.has_citics_info():
            return None

        if not hasattr(self, "_citics_industry_code"):
            self.citics_industry()
        return self._citics_industry_code

    @property
    def citics_industry_name(self):
        if not self.has_citics_info():
            return None

        if not hasattr(self, "_citics_industry_name"):
            self.citics_industry()
        return self._citics_industry_name


def _get_instrument(type_, order_book_id, market="cn"):
    all_dict = _all_instruments_dict(type_, market)
    return all_dict[order_book_id]


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_serialize(converter=http_conv_instruments)
def instruments(order_book_ids, market="cn"):
    """
    获取某个国家市场内一个或多个合约最新的详细信息。

    目前仅支持中国市场。目前系统并不支持跨市场的同时调用，传入的 order_book_id list 必须属于同一国家市场，不能混合着中美两个国家市场的 order_book_id。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list。
        中国市场的 order_book_id 通常类似'000001.XSHE'。需要注意，国内股票、ETF、指数合约代码分别应当以'.XSHG'或'.XSHE'结尾，前者代表上证，后者代表深证。
        比如查询平安银行这个股票合约，则键入'000001.XSHE'，前面的数字部分为交易所内这个股票的合约代码，后半部分为对应的交易所代码。
        期货则无此要求
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    Instrument | list[Instrument]
        一个 instrument 对象，或一个 instrument list。

    Examples
    --------
    获取单一股票合约的详细信息:

    >>> instruments('000001.XSHE')
    Instrument(order_book_id='000001.XSHE', industry_code='J66', market_tplus=1, symbol='平安银行', special_type='Normal', exchange='XSHE', status='Active', type='CS', de_listed_date='0000-00-00', listed_date='1991-04-03', sector_code_name='金融', abbrev_symbol='PAYH', sector_code='Financials', round_lot=100, trading_hours='09:31-11:30,13:01-15:00', board_type='MainBoard', industry_name='货币金融服务', issue_price=40.0, citics_industry_code='40', citics_industry_name='银行')

    获取多个股票合约的详细信息:

    >>> instruments(['000001.XSHE', '000024.XSHE'])
    [Instrument(order_book_id='000001.XSHE', industry_code='J66', market_tplus=1, symbol='平安银行', special_type='Normal', exchange='XSHE', status='Active', type='CS', de_listed_date='0000-00-00', listed_date='1991-04-03', sector_code_name='金融', abbrev_symbol='PAYH', sector_code='Financials', round_lot=100, trading_hours='09:31-11:30,13:01-15:00', board_type='MainBoard', industry_name='货币金融服务',industry_name='银行'),
     Instrument(order_book_id='000024.XSHE', industry_code='K70', market_tplus=1, symbol='招商地产', special_type='Normal', exchange='XSHE', status='Delisted', type='CS', de_listed_date='2015-12-30', listed_date='1993-06-07', sector_code_name='房地产', abbrev_symbol='ZSDC', sector_code='RealEstate', round_lot=100, trading_hours='09:31-11:30,13:01-15:00', board_type='MainBoard', industry_name='房地产业')]

    """
    if market == "hk":
        return hk_instruments(order_book_ids)
    obid_to_type = _all_obid_to_type(market)
    if isinstance(order_book_ids, six.string_types):
        if order_book_ids not in obid_to_type:
            warnings.warn('unknown order_book_id: {}'.format(order_book_ids))
            return
        ob_type, ob = obid_to_type[order_book_ids]
        return _get_instrument(ob_type, ob, market)
    result = []
    for ob in order_book_ids:
        if ob not in obid_to_type:
            continue
        ob_type, ob = obid_to_type[ob]
        result.append(_get_instrument(ob_type, ob, market))
    return result


# different to `_all_instruments_dict`
# it returns `order_book_id -> list[instruments]` mapping
@ttl_cache(3 * 3600)
def _all_hk_instruments_dict(type_):
    ins = _all_cached_instruments_list(type_, market="hk")
    result = defaultdict(list)
    for i in ins:
        result[i.order_book_id].append(i)
        result[i.unique_id].append(i)
    for v in result.values():
        v.sort(key=lambda x:x.unique_id)
    return result


def hk_instruments(order_book_ids):
    obid_to_type = _all_obid_to_type(market="hk")
    if isinstance(order_book_ids, six.string_types):
        if order_book_ids not in obid_to_type:
            warnings.warn("unknown order_book_id: {}".format(order_book_ids))
            return
        ob_type, ob = obid_to_type[order_book_ids]
        all_dict = _all_hk_instruments_dict(ob_type)
        if len(all_dict[ob]) == 1:
            return all_dict[ob][0]
        else:
            return all_dict[ob]
    result = []
    for one_ob in order_book_ids:
        if one_ob not in obid_to_type:
            continue
        ob_type, ob = obid_to_type[one_ob]
        all_dict = _all_hk_instruments_dict(ob_type)
        result.extend(all_dict[ob])
    return result


VALID_TYPES = {"CS", "ETF", "LOF", "REITs", "INDX", "Future", "Spot", "Option", "Convertible", "Repo", "FUND", "FutureArbitrage"}


@export_as_api
@may_trim_bjse
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_no_index_mark
def all_instruments(type=None, date=None, market="cn", **kwargs):
    """
    获取某个国家市场的所有合约信息。

    使用者可以通过这一方法很快地对合约信息有一个快速了解，目前仅支持中国市场。
    可传入date筛选指定日期的合约，返回的 instrument 数据为合约的最新情况。

    Parameters
    ----------
    type : str | list[str], optional
        需要查询合约类型，例如：type='CS'代表股票。默认是所有类型
        - 'CS' : Common Stock, 即股票
        - 'ETF' : Exchange Traded Fund, 即交易所交易基金
        - 'LOF' : Listed Open-Ended Fund，即上市型开放式基金 （以下分级基金已并入）
        - 'INDX' : Index, 即指数
        - 'Future' : Futures，即期货，包含股指、国债和商品期货
        - 'Spot' : Spot，即现货，目前包括上海黄金交易所现货合约
        - 'Option' : 期权，包括目前国内已上市的全部期权合约
        - 'Convertible' : 沪深两市场内有交易的可转债合约
        - 'Repo' : 沪深两市交易所交易的回购合约
        - 'REITs' : 不动产投资信托基金
        - 'FUND' : 包括ETF, LOF, REITs后的其他基金
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        指定日期，筛选指定日期可交易的合约
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        所有合约的基本信息。

    Examples
    --------
    获取中国内地市场所有合约的基础信息:

    >>> all_instruments()
        abbrev_symbol order_book_id  sector_code symbol
    0 XJDQ 000400.XSHE   Industrials     许继电气
    1 HXN     002582.XSHE   ConsumerStaples 好想你
    2 NFGF 300004.XSHE   Industrials     南风股份
    3 FLYY 002357.XSHE   Industrials     富临运业
    ...

    获取中国内地市场所有 LOF 基金的基础信息:

    >>> all_instruments(type='LOF')
        abbrev_symbol order_book_id product sector_code  symbol
    0 CYGA 150303.XSHE null null 华安创业板50A
    1 JY500A 150088.XSHE null null 金鹰500A
    2 TD500A 150053.XSHE null null 泰达稳健
    3 HS500A 150110.XSHE null null 华商500A
    4 QSAJ 150235.XSHE null null 鹏华证券A
    ...

    """

    if type is not None:
        type = ensure_list_of_string(type)
        itype = set()
        for t in type:
            if t.upper() == "STOCK":
                itype.add("CS")
            elif t.upper() == "FUND":
                itype = itype.union({"ETF", "LOF", "REITs", "FUND"})
            elif t.upper() == "INDEX":
                itype.add("INDX")
            elif t not in VALID_TYPES:
                raise ValueError("invalid type: {}, chose any in {}".format(type, VALID_TYPES))
            else:
                itype.add(t)
    else:
        itype = VALID_TYPES

    if date:
        date = ensure_date_str(date)
        cond = lambda x: (  # noqa: E731
                (itype is None or x.type in itype)
                and (x.listed_date <= date or x.listed_date == "0000-00-00")
                and (
                        x.de_listed_date == "0000-00-00"
                        or (
                                x.de_listed_date >= date
                                and x.type in ("Future", "Option")
                                or (x.de_listed_date > date and x.type not in ("Future", "Option"))
                        )
                )
        )
    else:
        cond = lambda x: itype is None or x.type in itype  # noqa: E731

    cached = kwargs.pop("cached", True)
    trading_market = kwargs.pop("trading_market", 'hk')
    if kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs))

    if cached:
        get_instrument_list = _all_cached_instruments_list
    else:
        get_instrument_list = _all_instruments_list

    ins_ret = []
    for t in itype:
        ins_ret.extend(filter(cond, get_instrument_list(t, market)))

    if market == 'hk' and trading_market == 'hk':
        ins_ret = filter(lambda x: x.unique_id.endswith('.XHKG'), ins_ret)

    if itype is not None and len(itype) == 1:
        df = pd.DataFrame([v.__dict__ for v in ins_ret])
        internal_fields = [f for f in df.columns if f.startswith('_')]
        for f in internal_fields:
            del df[f]
    else:
        df = pd.DataFrame(
            [
                (
                    v.order_book_id,
                    v.symbol,
                    getattr(v, "abbrev_symbol", None),
                    v.type,
                    v.listed_date,
                    v.de_listed_date,
                )
                for v in ins_ret
            ],
            columns=[
                "order_book_id",
                "symbol",
                "abbrev_symbol",
                "type",
                "listed_date",
                "de_listed_date",
            ],
        )
    return df


def to_sector_name(s):
    for _, v in SectorCode.__dict__.items():
        if isinstance(v, SectorCodeItem):
            if v.cn == s or v.en == s or v.name == s:
                return v.name
    return s


@export_as_api
@may_trim_bjse
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def sector(code, market="cn"):
    """
    获得属于某一板块的所有股票列表。

    Parameters
    ----------
    code : str or sector_code items
        板块名称或板块代码。例如，能源板块可填写'Energy'、'能源'或 sector_code.Energy
    market : str, optional
        默认是中国市场('cn')，目前仅支持中国市场。

    Returns
    -------
    list
        属于该板块的股票 order_book_id list.

    Examples
    --------
    >>> sector('Energy')
    ['300023.XSHE', '000571.XSHE', '600997.XSHG', '601798.XSHG', '603568.XSHG', .....]

    >>> sector(sector_code.Energy)
    ['300023.XSHE', '000571.XSHE', '600997.XSHG', '601798.XSHG', '603568.XSHG', .....]

    """
    check_type(code, (SectorCodeItem, six.string_types), "code")
    if isinstance(code, SectorCodeItem):
        code = code.name
    else:
        code = to_sector_name(code)
    return [
        v.order_book_id
        for v in _all_cached_instruments_list("CS", market)
        if v.sector_code == code
    ]


def to_industry_code(s):
    for _, v in IndustryCode.__dict__.items():
        if isinstance(v, IndustryCodeItem):
            if v.name == s:
                return v.code
    return s


@export_as_api
@may_trim_bjse
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def industry(code, market="cn"):
    """
    获得属于某一行业的所有股票列表。

    Parameters
    ----------
    code : str or industry_code items
        行业名称或行业代码。例如，农业可填写 industry_code.A01 或 'A01'
    market : str, optional
        默认是中国市场('cn')，目前仅支持中国市场

    Returns
    -------
    list
        属于该行业的股票 order_book_id list.

    Examples
    --------
    >>> industry('A01')
    ['600540.XSHG', '600371.XSHG', '600359.XSHG', '600506.XSHG',...]

    >>> industry(industry_code.A01)
    ['600540.XSHG', '600371.XSHG', '600359.XSHG', '600506.XSHG',...]

    """
    if not isinstance(code, six.string_types):
        code = code.code
    else:
        code = to_industry_code(code)
    return [
        v.order_book_id
        for v in _all_cached_instruments_list("CS", market)
        if v.industry_code == code
    ]


@export_as_api
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def get_future_contracts(underlying_symbol, date=None, market="cn"):
    import rqdatac
    import warnings

    msg = "'get_future_contracts' is deprecated, please use 'futures.get_contracts' instead"
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    return rqdatac.futures.get_contracts(underlying_symbol, date, market)


@export_as_api(namespace='futures')
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def get_contracts(underlying_symbol, date=None, market="cn"):
    """获取指定期货品种在指定日期可交易的合约列表

    返回值按到期月份排序

    Parameters
    ----------
    underlying_symbol : str
        期货合约品种，例如沪深 300 股指期货为'IF'
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认为当日
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    list[str]
        可交易的 order_book_id list

    Examples
    --------
    >>> futures.get_contracts('IF', '20160801')
    ['IF1608', 'IF1609', 'IF1612', 'IF1703']

    """
    if date is None:
        date = datetime.date.today()
    date = ensure_date_str(date)

    return sorted(
        v.order_book_id
        for v in _all_cached_instruments_list("Future", market)
        if v.underlying_symbol == underlying_symbol
        and v.listed_date != "0000-00-00"
        and v.listed_date <= date <= v.de_listed_date
    )


@export_as_api
def jy_instrument_industry(order_book_ids, date=None, level=1, expect_df=True, market="cn"):
    """获取股票对应的聚源行业

    :param order_book_ids: 股票列表，如['000001.XSHE', '000002.XSHE']
    :param date: 如 '2015-01-07' (Default value = None)
    :param level: 聚源等级，0, 1, 2, 3, 'customized' (Default value = 1)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = True)
    :param market:  (Default value = "cn")
    :returns: code, name
        返回输入日期最近交易日的股票对应聚源行业以及对应的聚源等级

    """
    if expect_df is False:
        if market != "cn":
            raise ValueError("'expect_df' can not be False when market is not 'cn'")
        else:
            warnings.warn(
                "'expect_df=False' is deprecated, and will be removed in future",
                category=DeprecationWarning,
            )
    if level not in (0, 1, 2, 3, "customized"):
        raise ValueError("level should in 0, 1, 2, 3, 'customized'")
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if not date:
        date = ensure_date_int(get_previous_trading_date(datetime.date.today(), market=market))
    else:
        date = ensure_date_int(date)

    df = get_client().execute("jy_instrument_industry", order_book_ids, date, level, market=market)
    if not df:
        return
    if len(order_book_ids) == 1 and not expect_df:
        r = df[0]
        if level == 0:
            return r["first_industry_name"], r["second_industry_name"], r["third_industry_name"]
        return r["industry_name"]
    return pd.DataFrame(df).set_index("order_book_id")


@export_as_api(namespace="econ")
def get_reserve_ratio(reserve_type="all", start_date=None, end_date=None, market="cn"):
    """获取存款准备金率

    Parameters
    ----------
    reserve_type : str | list[str], optional
        存款准备金详细类别，默认为 'all'，目前支持 'all', 'major', 'other'
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始查找时间，如 '20180501'，默认为上一年的当天
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        截止查找时间，如 '20180501'，默认为当天
    market : str, optional
        (Default value = "cn")

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - reserve_type : str, 存款准备金类别
        - info_date : pandas.Timestamp, 消息公布日期
        - effective_date : pandas.Timestamp, 存款准备金率生效日期
        - ratio_floor : float, 存款准备金下限
        - ratio_ceiling : float, 存款准备金上限

    Examples
    --------
    >>> econ.get_reserve_ratio(reserve_type='major', start_date='20170101', end_date='20181017')

            reserve_type                 effective_date    ratio_ceiling  ratio_floor
    info_date
    2018-10-07    major_financial_institution     2018-10-15    15.0  15.0
    2018-04-17    major_financial_institution     2018-04-25    16.0  16.0
    """
    check_items_in_container(reserve_type, ["all", "major", "other"], "reserve_type")

    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))

    ret = get_client().execute(
        "econ.get_reserve_ratio", reserve_type, start_date, end_date, market
    )
    if not ret:
        return
    columns = ["info_date", "effective_date", "reserve_type", "ratio_floor", "ratio_ceiling"]
    df = pd.DataFrame(ret, columns=columns).set_index("info_date").sort_index(ascending=True)
    return df


@export_as_api(namespace="econ")
def get_money_supply(start_date=None, end_date=None, market="cn"):
    """获取货币供应量信息

    Parameters
    ----------
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为去年的查询当日（基准为信息公布日）。
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为查询当日。
    market : str, optional
        (Default value = "cn")

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - info_date : pandas.Timestamp, 消息公布日期
        - effective_date : pandas.Timestamp, 货币供应量生效日期
        - m2 : float
        - m1 : float
        - m0 : float
        - m2_growth_yoy : float
        - m1_growth_yoy : float
        - m0_growth_yoy : float

    Examples
    --------
    >>> econ.get_money_supply(start_date='20180801', end_date='20181017')

        effective_date    m2     m1     m0    m2_growth_yoy  m1_growth_yoy  m0_growth_yoy
    info_date
    2018-09-21  2018-08-31  178867043.0  53832464.0  6977539.0  0.082  0.039  0.033
    2018-08-16  2018-07-31  177619611.0  53662429.0  6953059.0  0.085  0.051  0.036
    """
    check_items_in_container("info_date", ["info_date", "effective_date"], "date_type")
    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))

    data = get_client().execute("econ.get_money_supply", start_date, end_date, market=market)
    if not data:
        return
    columns = [
        "info_date",
        "effective_date",
        "m2",
        "m1",
        "m0",
        "m2_growth_yoy",
        "m1_growth_yoy",
        "m0_growth_yoy",
    ]
    df = pd.DataFrame(data, columns=columns).set_index("info_date").sort_index(ascending=True)
    return df


@export_as_api
@support_hk_order_book_id
def get_main_shareholder(
        order_book_ids, start_date=None, end_date=None, is_total=False, start_rank=None, end_rank=None, market="cn"
):
    """
    获取 A 股主要股东构成及*持流通 A 股数量*比例、持股性质等信息，通常为前十位。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为去年当日。
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为查询当日。
    is_total : bool, optional
        默认为 False, 即基于持有 A 股流通股。若为 True 则基于所有发行出的 A 股。
    start_rank : int, optional
        排名开始值
    end_rank : int, optional
        排名结束值 ,start_rank ,end_rank 不传参时返回全部的十位股东名单
    market : str, optional
        市场，默认'cn'为中国内地市场。

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - info_date : pandas.Timestamp, 公告发布日
        - end_date : pandas.Timestamp, 截止日期
        - rank : int, 排名
        - shareholder_name : str, 股东名称
        - shareholder_attr : str, 股东属性
        - shareholder_kind : str, 股东性质
        - shareholder_type : str, 股东类别
        - hold_percent_total : float, 占股比例（%） 当 fields='total'时，持股数(股)/总股本*100。
        - hold_percent_float : float, 占流通 A 股比例（%）,无限售流通 A 股/已上市流通 A 股（不含高管股）*100
        - share_pledge : float, 股权质押涉及股数（股）
        - share_freeze : float, 股权冻结涉及股数（股）

    Examples
    --------
    获取平安银行在 2018 年三月上旬的主要的 A 股股东名单

    >>> get_main_shareholder('000001.XSHE', start_date='20180301', end_date='20180315', is_total=False)
                end_date  rank  shareholder_name                          shareholder_attr  shareholder_kind  shareholder_type  hold_percent_total  hold_percent_float
    info_date
    2018-03-15  2017-12-31  1      中国平安保险(集团)股份有限公司-集团本级-自有资金       企业           金融机构—保险公司       None             48.095791         48.813413
    2018-03-15  2017-12-31  2      中国平安人寿保险股份有限公司-自有资金                 企业          金融机构—保险公司       None            6.112042            6.203238

    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    check_items_in_container(is_total, [True, False], "is_total")
    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))

    ret = get_client().execute(
        "get_main_shareholder", order_book_ids, start_date, end_date, is_total,
        start_rank=start_rank, end_rank=end_rank, market=market
    )
    if not ret:
        return
    columns = ['info_date', 'end_date', 'rank', 'shareholder_name', 'shareholder_attr', 'shareholder_kind',
               'shareholder_type', 'hold_percent_total', 'hold_percent_float', 'share_pledge', 'share_freeze',
               'order_book_id']
    df = pd.DataFrame(ret, columns=columns).sort_values(by=['info_date', 'rank']).\
        set_index(['order_book_id', 'info_date'])
    return df


@export_as_api
def get_current_news(n=None, start_time=None, end_time=None, channels=None):
    """获取新闻
    :param n: 新闻条数, n 和 start_time/end_time 只能指定其一
    :param start_time: 开始日期，默认为None,格式为%Y-%m-%d %H:%M:%S，如"2018-10-20 09:10:20"
    :param end_time: 结束日期，默认为None,格式为%Y-%m-%d %H:%M:%S，如"2018-10-20 19:10:20"
    :param channels: 新闻大类, 默认为None,返回每个大类n条新闻, 如 "global"，"forex", "commodity", "a-stock"
    """

    if start_time is not None or end_time is not None:
        try:
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise ValueError('start_time should be str format like "%Y-%m-%d %H:%M:%S"')
        try:
            end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise ValueError('end_time should be str format like "%Y-%m-%d %H:%M:%S"')
        start_time = datetime_to_int14(start_time)
        end_time = datetime_to_int14(end_time)
        if n is not None:
            raise ValueError(
                "please either specify parameter n, or specify both start_time and end_time"
            )
        n = 1200
    elif n is None:
        n = 1
    else:
        n = ensure_int(n, "n")
        if n < 1 or n > 1200:
            raise ValueError("n should be in [0, 1200]")

    if channels is not None:
        channels = ensure_list_of_string(channels)
        check_items_in_container(channels, ["global", "forex", "a-stock", "commodity"], "channels")
    else:
        channels = ["global", "forex", "a-stock", "commodity"]

    data = get_client().execute("get_current_news", n, start_time, end_time, channels)
    if not data:
        return
    df = pd.DataFrame(data, columns=["channel", "datetime", "content"])
    return df.set_index("channel")


@export_as_api(namespace="econ")
def get_factors(factors, start_date, end_date, market="cn"):
    """获取宏观因子数据。

    Parameters
    ----------
    factors : str | list[str]
        宏观因子名称，<https://assets.ricequant.com/vendor/rqdata/econ_get_factors.xlsx>（见文档）
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        起始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        截止日期
    market : str, optional
        (Default value = "cn")

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - factor : str
        - info_date : pandas.Timestamp
        - start_date : pandas.Timestamp
        - end_date : pandas.Timestamp
        - value : float
        - rice_create_tm : pandas.Timestamp

    Examples
    --------
    - 获取工业品出厂价格指数 PPI*当月同比*(上年同月=100)在 2017-08-01 到 2018-08-01 数据。

    >>> econ.get_factors(factors='工业品出厂价格指数PPI_当月同比_(上年同月=100)', start_date='20170801', end_date='20180801')

                                            start_date   end_date  value      rice_create_tm
    factor                       info_date
    工业品出厂价格指数PPI_当月同比_(上年同月=100) 2017-08-09 2017-07-01 2017-07-31  105.5 2023-05-06 20:25:46
                                 2017-09-09 2017-08-01 2017-08-31  106.3 2023-05-06 20:25:46
                                 2017-10-16 2017-09-01 2017-09-30  106.9 2023-05-06 20:25:46
                                 ...
    """
    start_date, end_date = ensure_date_range(start_date, end_date)
    factors = ensure_list_of_string(factors, "factors")
    data = get_client().execute("econ.get_factors", factors, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    columns = ["factor", "info_date", "start_date", "end_date", "value"]
    if "rice_create_tm" in df.columns:
        df["rice_create_tm"] = pd.to_datetime(df["rice_create_tm"] + 3600 * 8, unit="s")
        columns.append("rice_create_tm")
    df = df.reindex(columns=columns)
    df.set_index(["factor", "info_date"], inplace=True)
    return df


@ttl_cache(12 * 3600)
def all_hk_trading_hours():
    records = get_client().execute("get_hk_trading_hours")
    return {i["date"]: i["trading_hours"] for i in records}


@export_as_api
@support_hk_order_book_id
@rqdatah_serialize(converter=http_conv_trading_hours)
def get_trading_hours(order_book_id, date=None, expected_fmt="str", frequency="1m", market="cn"):
    """
    默认获取当前点国内市场合约字符串形式的连续竞价交易时间段。

    该 API 即将退役，可使用 get_trading_periods

    Parameters
    ----------
    order_book_id : str
        合约代码
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        指定日期。使用场景，部分合约的当前和历史的连续竞价交易时间段会有不同
    expected_fmt : str, optional
        期望返回的数据类型, 可选值为 str, time, datetime
        'str' -这个函数会返回字符串
        'time' - 这个函数会返回 datetime.time 类型
        'datetime' - 这个函数会返回 datetime.datetime 类型
    frequency : str, optional
        频率,默认为 1m, 对应米筐分钟线时间段的起始, 为 tick 时返回交易所给出的交易时间
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    str
        交易时间

    Examples
    --------
    >>> get_trading_hours('000001.XSHE')
    '09:31-11:30,13:01-15:00'

    获取单个合约的交易时间, 指定返回 datetime.time 类型

    >>> get_trading_hours("000001.XSHE", expected_fmt="time")

    [[datetime.time(9, 31), datetime.time(11, 30)],
     [datetime.time(13, 1), datetime.time(15, 0)]

    获取单个合约在2025-11-13的交易时间, 指定返回 datetime.datetime类型

    >>> get_trading_hours("A2511", date=20251113, expected_fmt="datetime")

    [[datetime.datetime(2025, 11, 12, 21, 1),
      datetime.datetime(2025, 11, 12, 23, 0)],
     [datetime.datetime(2025, 11, 13, 9, 1),
      datetime.datetime(2025, 11, 13, 10, 15)],
     [datetime.datetime(2025, 11, 13, 10, 31),
      datetime.datetime(2025, 11, 13, 11, 30)],
     [datetime.datetime(2025, 11, 13, 13, 31),
      datetime.datetime(2025, 11, 13, 15, 0)]]
    """
    date = ensure_date_or_today_int(date)
    if not is_trading_date(date, market):
        warnings.warn(" %d is not a trading date" % date)
        return

    ensure_string(order_book_id, "order_book_id")
    ins = instruments(order_book_id, market=market)
    if ins is None:
        return

    ensure_string_in(expected_fmt, ("str", "time", "datetime"), "expected_fmt")
    ensure_string_in(frequency, ("1m", "tick"), "frequency")
    date_str = to_date_str(date)

    if ins.listed_date > date_str:
        return

    if ins.type in ("Future", "Option") and ins.de_listed_date < date_str and ins.de_listed_date != "0000-00-00":
        return

    if ins.type not in ("Future", "Option") and ins.de_listed_date <= date_str and ins.de_listed_date != "0000-00-00":
        return
    if market == "hk":
        # 针对港交所, 需要通过服务端获取对应信息
        # e.g: 特殊情况如台风、元旦、圣诞节前夕等港股只开半日市
        hk_trading_hours = all_hk_trading_hours()
        trading_hours = hk_trading_hours.get(date, "09:31-12:00,13:01-16:00")
    elif ins.type == "Repo":
        trading_hours = "09:31-11:30,13:01-15:30"
    elif ins.type == "Spot":
        if has_night_trading(date, market):
            trading_hours = "20:01-02:30,09:01-15:30"
        else:
            trading_hours = "09:01-15:30"
    elif ins.type not in ("Future", "Option") or (ins.type == "Option" and ins.exchange in ("XSHG", "XSHE")):
        trading_hours = "09:31-11:30,13:01-15:00"
    else:
        trading_hours = get_client().execute("get_trading_hours", ins.underlying_symbol, date, market=market)
        if trading_hours is None:
            return
        # 前一天放假或者该品种上市首日没有夜盘
        no_night_trading = (not has_night_trading(date, market) or
                            get_underlying_listed_date(ins.underlying_symbol, ins.type) == date_str)

        if no_night_trading and not trading_hours.startswith("09"):
            trading_hours = trading_hours.split(",", 1)[-1]

    if frequency == "tick":
        trading_hours = ",".join([s[:4] + str(int(s[4]) - 1) + s[5:] for s in trading_hours.split(",")])

    if expected_fmt != "str":
        trading_hours = [t.split("-", 1) for t in trading_hours.split(",")]
        for i, (start, end) in enumerate(trading_hours):
            trading_hours[i][0] = to_time(start)
            trading_hours[i][1] = to_time(end)

        if expected_fmt == "datetime":
            td = int8_to_date(date)
            prev_td = get_previous_trading_date(date)
            prev_td_next = prev_td + datetime.timedelta(days=1)

            for i, (start, end) in enumerate(trading_hours):
                if start.hour > 16:
                    start_dt = prev_td
                    end_dt = start_dt if end.hour > 16 else prev_td_next
                else:
                    start_dt = end_dt = td
                trading_hours[i][0] = datetime.datetime.combine(start_dt, start)
                trading_hours[i][1] = datetime.datetime.combine(end_dt, end)

    return trading_hours


def _build_underlying_search_map(df):
    """
    为每个underlying_symbol构建优化的搜索数据结构
    """
    underlying_data_map = {}

    for underlying_symbol, group in df.groupby('underlying'):
        dates = group['date'].dt.date.values
        trading_hours = group['trading_hours'].values
        underlying_data_map[underlying_symbol] = {
            'dates': dates,
            'trading_hours': trading_hours,
            'min_date': dates[0] if len(dates) > 0 else None,
            'max_date': dates[-1] if len(dates) > 0 else None
        }

    return underlying_data_map


def _find_trading_hours(underlying_data_map, underlying_symbol, input_date):
    """
    快速查找交易时间，使用预构建的数据结构
    """
    if underlying_symbol not in underlying_data_map:
        return None

    data = underlying_data_map[underlying_symbol]
    dates = data['dates']
    trading_hours_arr = data['trading_hours']

    if data['min_date'] and input_date < data['min_date']:
        return None
    if data['max_date'] and input_date > data['max_date']:
        return trading_hours_arr[-1] if len(trading_hours_arr) > 0 else None

    # 使用二分查找
    idx = np.searchsorted(dates, input_date, side='right') - 1
    return trading_hours_arr[idx] if idx >= 0 else None


def get_other_data(other_data, market):
    all_dates = []
    underlying_symbols = set()

    for order_book_id, date_list in other_data.items():
        for date_info in date_list:
            all_dates.append((order_book_id, date_info))
            underlying_symbols.add(date_info["underlying_symbol"])

    underlying_symbol_list = list(underlying_symbols)
    if not underlying_symbol_list:
        return []

    data = get_client().execute("get_trading_periods", underlying_symbol_list, market=market)
    df = pd.DataFrame(data)
    if df.empty:
        return []

    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    df = df.sort_values(['underlying', 'date'])

    underlying_data_map = _build_underlying_search_map(df)
    return_list = []
    for od, values in all_dates:
        underlying_symbol = values["underlying_symbol"]
        trading_hours = _find_trading_hours(
            underlying_data_map, underlying_symbol, values["date"]
        )
        if not trading_hours:
            continue

        no_night_trading = (
                not has_night_trading(values["date"], market) or
                get_underlying_listed_date(values["underlying_symbol"], values["type"]) == to_date_str(values["date"])
        )
        if no_night_trading and not trading_hours.startswith("09"):
            trading_hours = trading_hours.split(",", 1)[-1]

        return_list.append((od, values["date"], trading_hours))

    return return_list


def _get_instrument_date_range(ins, start_date, end_date, date_objs, date_index):
    start_dt = ensure_date_str(start_date)
    end_dt = ensure_date_str(end_date)

    listed_date_str = ins.listed_date
    if listed_date_str and listed_date_str > end_dt:
        return None

    de_listed_date_str = ins.de_listed_date
    if de_listed_date_str == "0000-00-00":
        de_listed_date_str = None
    if de_listed_date_str and de_listed_date_str < start_dt:
        return None

    effective_start = max(start_dt, listed_date_str) if listed_date_str else start_dt
    effective_end = min(end_dt, de_listed_date_str) if de_listed_date_str else end_dt

    start_idx = date_index.searchsorted(effective_start)
    end_idx = date_index.searchsorted(effective_end)

    # 新增：专门处理单日查询的情况
    if effective_start == effective_end:
        if 0 <= start_idx < len(date_objs) and to_date_str(date_objs[start_idx]) == effective_start:
            return start_idx, start_idx

    if start_idx > end_idx or start_idx >= len(date_objs) or end_idx < 0:
        return None

    # end_idx超出范围（结束日期不是交易日），调整为最大idx
    if end_idx >= len(date_objs):
        end_idx = len(date_objs) - 1

    return start_idx, end_idx


def _get_trading_hours_by_type(ins_type, ins, date, market):
    """根据类型获取交易时间"""
    if ins_type == "Repo":
        return "09:31-11:30,13:01-15:30"
    elif ins_type == "Spot":
        return "20:01-02:30,09:01-15:30" if has_night_trading(date, market) else "09:01-15:30"
    elif ins_type not in ("Future", "Option") or (ins_type == "Option" and ins.exchange in ("XSHG", "XSHE")):
        return "09:31-11:30,13:01-15:00"
    return None


@export_as_api
@support_hk_order_book_id
def get_trading_periods(order_book_ids, start_date=None, end_date=None, frequency="1m", market="cn"):
    """
    默认获取当前点国内市场合约字符串形式的连续竞价交易时间段。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，给出单个或多个 order_book_id
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期。不指定 start_date，end_date 默认返回最近三个月的数据
    frequency : str, optional
        频率,默认为 1m, 对应米筐分钟线时间段的起始, 为 tick 时返回交易所给出的交易时间
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        包含交易时间段的 DataFrame

    Examples
    --------
    获取单个合约一段时间的连续竞价交易时间段

    >>> get_trading_periods('000001.XSHE',20250901,20250910,'1m')
                trading_hours
    order_book_id	date
    000001.XSHE	2025-09-01	09:31-11:30,13:01-15:00
                2025-09-02	09:31-11:30,13:01-15:00
                2025-09-03	09:31-11:30,13:01-15:00
                2025-09-04	09:31-11:30,13:01-15:00
                2025-09-05	09:31-11:30,13:01-15:00
                2025-09-08	09:31-11:30,13:01-15:00
                2025-09-09	09:31-11:30,13:01-15:00

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date)
    ensure_string_in(frequency, ("1m", "tick"), "frequency")
    date_objs = get_trading_dates(start_date, end_date, market=market)
    if not date_objs:
        return

    date_index = pd.to_datetime(date_objs)
    ins_list = instruments(order_book_ids, market=market)
    result = []
    if market == "hk":
        hk_trading_hours = all_hk_trading_hours()
        for ins in ins_list:
            date_range = _get_instrument_date_range(ins, start_date, end_date, date_objs, date_index)
            if not date_range:
                continue
            start_idx, end_idx = date_range
            for i in range(start_idx, end_idx + 1):
                trading_hours = hk_trading_hours.get(to_date_int(date_objs[i]), "09:31-12:00,13:01-16:00")
                result.append((ins.order_book_id, date_objs[i], trading_hours))
    else:
        # 按类型分组
        ins_by_type = defaultdict(list)
        for ins in ins_list:
            ins_by_type[ins.type].append(ins)
        other = defaultdict(list)
        # 按类型批量处理
        for ins_type, instruments_list in ins_by_type.items():
            for ins in instruments_list:
                date_range = _get_instrument_date_range(ins, start_date, end_date, date_objs, date_index)
                if not date_range:
                    continue

                start_idx, end_idx = date_range
                # 批量处理该instrument的所有有效日期
                for i in range(start_idx, end_idx + 1):
                    trading_hours = _get_trading_hours_by_type(ins_type, ins, date_objs[i], market)
                    if trading_hours is not None:
                        result.append((ins.order_book_id, date_objs[i], trading_hours))
                    else:
                        other[ins.order_book_id].append({
                            "underlying_symbol": ins.underlying_symbol,
                            "type": ins_type,
                            "date": date_objs[i]
                        })
        if other:
            return_list = get_other_data(other, market)
            result.extend(return_list)

    df = pd.DataFrame(result, columns=['order_book_id', 'date', 'trading_hours'])
    if frequency == "tick":
        df["trading_hours"] = df["trading_hours"].apply(
            lambda x: ",".join([s[:4] + str(int(s[4]) - 1) + s[5:] for s in x.split(",")])
        )
    df.set_index(['order_book_id', 'date'], inplace=True)
    df.sort_index(inplace=True)

    return df


@export_as_api
@support_hk_order_book_id
def get_private_placement(order_book_ids, start_date=None, end_date=None, progress="complete", issue_type="private", market="cn"):
    """
    获取股票在一段时间内的定向增发信息（包含起止日期，以公告发布日为查询基准）。如未指定日期，则默认所有。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为None
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为None
    progress : str, optional
        是否已完成定增，默认为complete。可选参数["complete", "incomplete", "all"]
    issue_type : str, optional
        默认为all。可选参数["private", "public", "all"]
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date and end_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
    elif start_date:
        start_date = ensure_date_int(start_date)
    elif end_date:
        end_date = ensure_date_int(end_date)
    ensure_string_in(progress, ["complete", "incomplete", "all"], "progress")
    ensure_string_in(issue_type, ["private", "public", "all"], "issue_type")
    issue_type_change = {"private": (21, 23), "public": (22,), "all": (21, 22, 23)}
    issue_type = issue_type_change[issue_type]
    data = get_client().execute(
        "get_private_placement", order_book_ids, start_date, end_date, progress, issue_type=issue_type, market=market
    )
    if not data:
        return
    progress_map = {
        10: "董事会预案", 20: "股东大会通过", 21: "国资委通过", 22: "发审委通过", 23: "证监会通过",
        29: "实施中", 30: "实施完成", 40: "国资委否决", 41: "股东大会否决", 42: "证监会否决",
        43: "发审委否决", 50: "延期实施", 60: "停止实施", 70: "暂缓发行"}
    issue_type_map = {21: "非公开发行", 22: "公开发行", 23: "非公开发行配套融资"}
    df = pd.DataFrame(data)
    df["progress"] = df["progress"].map(progress_map)
    df["issue_type"] = df["issue_type"].map(issue_type_map)
    df.set_index(["order_book_id", "initial_info_date"], inplace=True)
    return df


@export_as_api
@rqdatah_no_index_mark
def get_share_transformation(predecessor=None, market="cn"):
    """
    查询股票因代码变更或并购等情况更换了股票代码的信息

    Parameters
    ----------
    predecessor : str, optional
        合约代码(来自交易所或其他平台), 空值返回所有变更过股票代码的股票
    market : str, optional
        目前仅支持国内市场('cn')。

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - predecessor : str, 历史股票代码
        - successor : str, 变更后股票代码
        - effective_date : pandas.Timestamp, 变更生效日期
        - share_conversion_ratio : float, 股票变更比例
        - predecessor_delisted : bool, 变更后旧代码是否退市
        - discretionary_execution : bool, 是否有变更自主选择权
        - predecessor_delisted_date : pandas.Timestamp, 历史股票代码退市日期
        - event : str, 股票代码变更原因

    Examples
    --------
    >>> get_share_transformation(predecessor="000022.XSHE")
        predecessor  successor    effective_date   share_conversion_ratio  predecessor_delisted  discretionary_execution  predecessor_delisted_date  event
    0  000022.XSHE  001872.XSHE  2018-12-26                        1.0      True                     False                 2018-12-26               code_change

    """
    if predecessor:
        predecessor = ensure_order_book_id(predecessor)
    data = get_client().execute("get_share_transformation", predecessor, market=market)
    if not data:
        return
    columns = [
        "predecessor", "successor", "effective_date", "share_conversion_ratio", "predecessor_delisted",
        "discretionary_execution", "predecessor_delisted_date", "event"
    ]
    df = pd.DataFrame(data, columns=columns)
    df = df.sort_values('predecessor').reset_index(drop=True)
    return df


@export_as_api(namespace="user")
@rqdatah_serialize(converter=http_conv_dict_to_csv)
def get_quota():
    """
    获取用户配额信息
    :return dict
        bytes_limit：每日流量使用上限（单位为字节），如为0则表示不受限
        bytes_used：当日已用流量（单位为字节）
        remaining_days：账号剩余有效天数, 如为0则表示不受限
        license_type：账户类型(FULL: 付费类型，TRIAL: 试用类型， EDU: 教育网类型, OTHER: 其他类型)
    """
    data = get_client().execute("user.get_quota")
    if data['bytes_limit'] > 0 and data["bytes_used"] >= data["bytes_limit"]:
        warnings.warn("Traffic usage has been exceeded quota,"
                      "Please call us at 0755-22676337 to upgrade"
                      "your contract.")
    return data


_CHECK_CATEGORIES = ("stock_1d", "stock_1m", "future_1d", "future_1m", "index_1d", "index_1m")


@export_as_api()
def get_update_status(categories):
    """
    获取数据最新日期
    :param categories: str or list or str, 数据类型，支持类型有:
        stock_1d: 股票日线
        stock_1m: 股票分钟线
        future_1d: 期货日线
        future_1m: 期货分钟线
        index_1d：指数日线
        index_1m：指数分钟线

    :return datetime.datetime or dict(category=datetime.datetime)
    """
    categories = ensure_list_of_string(categories, "categories")
    check_items_in_container(categories, _CHECK_CATEGORIES, "categories")
    ret = get_client().execute("get_update_status", categories)
    if len(categories) == 1:
        return ret[0]["date"]
    return {r["category"]: r["date"] for r in ret}


@export_as_api()
def info():
    """
    打印账户信息
    :return None
    """
    get_client().info()


@export_as_api()
@support_hk_order_book_id
def get_basic_info(order_book_ids=None, fields=("order_book_id", "symbol"), market='cn'):
    if order_book_ids is not None:
        order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if fields is not None:
        fields = ensure_list_of_string(fields, "fields")

    ret = get_client().execute("get_basic_info", order_book_ids, fields, market=market)
    if not ret:
        return
    columns, data = ret
    return pd.DataFrame(data, columns=columns)


@export_as_api()
def get_spot_benchmark_price(order_book_ids, start_date=None, end_date=None):
    """获取上海黄金交易所基准价数据: 有早盘价和午盘价

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可输入 order_book_id, order_book_id list。比如 'AU9999.SGEX'或者'AG9999.SGEX'
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为近 3 个月
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为近 3 个月

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_ids : str, 合约代码
        - date : pandas.Timestamp, 日期
        - morning : float, 早盘价格
        - noon : float, 午盘价格

    Examples
    --------
    获取黄金一段时间的早午盘价格。

    >>> rqdatac.get_spot_benchmark_price('AU9999.SGEX',20230501,20230508)

                          morning    noon
    order_book_id date
    AU9999.SGEX   2023-05-04   453.77  453.24
                  2023-05-05   455.84  455.57
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="Spot", market="cn")
    start_date, end_date = ensure_date_range(start_date, end_date)
    data = get_client().execute("get_spot_benchmark_price", order_book_ids, start_date=start_date, end_date=end_date)
    if not data:
        return
    df = pd.DataFrame(data)
    df.sort_values(by=["order_book_id", "date"], inplace=True)
    df.set_index(keys=["order_book_id", "date"], inplace=True)
    return df
