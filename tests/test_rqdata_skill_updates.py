import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import fetch_futures  # noqa: E402
import fetch_stocks  # noqa: E402


class FuturesContractTypeTests(unittest.TestCase):
    def test_main_accepts_contract_type_and_passes_to_update(self):
        seen_contract_types = []

        def _fake_update(*, contract_type, **kwargs):
            seen_contract_types.append(contract_type)
            return True

        with patch.object(fetch_futures, "init_rqdatac", return_value=object()), patch.object(
            fetch_futures, "get_output_dir", return_value=Path("/tmp")
        ), patch.object(
            fetch_futures, "parse_symbols", return_value=["CU"]
        ), patch.object(
            fetch_futures, "update_symbol", side_effect=_fake_update
        ):
            rc = fetch_futures.main(
                ["-f", "1d", "-s", "CU", "--contract-type", "dominant"]
            )

        self.assertEqual(rc, 0)
        self.assertEqual(seen_contract_types, ["dominant"])


class FuturesMissingOrderBookIdTests(unittest.TestCase):
    def test_update_symbol_fills_missing_order_book_id_from_default_oid(self):
        fetched = pd.DataFrame(
            {
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [123],
                "open_interest": [456],
                "total_turnover": [7890.0],
                "trading_date": ["2025-01-02"],
                "symbol": ["CU"],
            },
            index=pd.DatetimeIndex(["2025-01-02 10:00:00"], name="datetime"),
        )

        with tempfile.TemporaryDirectory() as td, patch.object(
            fetch_futures, "_fetch", return_value=fetched
        ):
            ok = fetch_futures.update_symbol(
                rqdatac=object(),
                symbol="CU",
                freq="60m",
                outdir=Path(td),
                contract_type="continuous",
                lookback=0,
                years=1,
                start_date="2025-01-01",
                end_date="2025-01-03",
            )

            self.assertTrue(ok)
            out_csv = Path(td) / "CU_1h.csv"
            self.assertTrue(out_csv.exists())
            out_df = pd.read_csv(out_csv)
            self.assertIn("order_book_id", out_df.columns)
            self.assertEqual(set(out_df["order_book_id"].astype(str)), {"CU99"})


class StocksFailFastTests(unittest.TestCase):
    def test_process_day_raises_when_daily_ohlcv_batch_unavailable(self):
        universe = pd.DataFrame(
            [
                {
                    "order_book_id": "000001.XSHE",
                    "symbol": "PingAn",
                    "listed_date": "1991-01-01",
                    "de_listed_date": "0000-00-00",
                }
            ]
        )

        with patch.object(fetch_stocks, "get_universe", return_value=universe), patch.object(
            fetch_stocks, "_batch_get_price", side_effect=[None, None]
        ):
            with self.assertRaises(RuntimeError):
                fetch_stocks._process_day(
                    object(),
                    "2024-01-02",
                    Path("/tmp/stock_data"),
                    True,
                    "month",
                    1e8,
                    True,
                    True,
                )


class StocksNoFilterModeTests(unittest.TestCase):
    def test_main_no_filter_switch_disables_filters_for_mode_a(self):
        captured = {}

        def _fake_run_daily(*args, **kwargs):
            captured.update(kwargs)
            return True

        with patch.object(fetch_stocks, "init_rqdatac", return_value=object()), patch.object(
            fetch_stocks, "get_store_path", return_value=Path("/tmp/rq_store")
        ), patch.object(fetch_stocks, "run_daily", side_effect=_fake_run_daily), patch.object(
            sys, "argv", ["fetch_stocks.py", "--no-filter"]
        ):
            rc = fetch_stocks.main()

        self.assertEqual(rc, 0)
        self.assertIn("enable_filters", captured)
        self.assertFalse(captured["enable_filters"])


class StocksIntradayTradingDateFallbackTests(unittest.TestCase):
    def test_fetch_stock_symbol_retries_without_trading_date(self):
        index = pd.DatetimeIndex(["2025-01-02 10:00:00"])
        good_df = pd.DataFrame(
            {
                "open": [10.0],
                "high": [10.2],
                "low": [9.9],
                "close": [10.1],
                "volume": [1000],
                "total_turnover": [10100.0],
            },
            index=index,
        )

        class _FakeRQ:
            def __init__(self):
                self.calls = []

            def get_price(self, *args, **kwargs):
                self.calls.append(kwargs.get("fields", []))
                if len(self.calls) == 1:
                    raise Exception("fields: got invalided value trading_date")
                return good_df

        fake = _FakeRQ()
        out = fetch_stocks._fetch_stock_symbol(
            fake, "601899.XSHG", "2025-01-01", "2025-01-05", "5m"
        )

        self.assertIsNotNone(out)
        self.assertEqual(len(out), 1)
        self.assertEqual(len(fake.calls), 2)
        self.assertNotIn("trading_date", fake.calls[-1])


class FinancialFieldDefaultsTests(unittest.TestCase):
    def test_default_financial_fields_use_valid_fixed_assets_name(self):
        self.assertIn("total_fixed_assets", fetch_stocks.DEFAULT_FINANCIAL_FIELDS)
        self.assertNotIn("fixed_assets", fetch_stocks.DEFAULT_FINANCIAL_FIELDS)


class FinancialQuarterValidationTests(unittest.TestCase):
    def test_main_rejects_invalid_quarter_format(self):
        with patch.object(fetch_stocks, "init_rqdatac", return_value=object()), patch.object(
            fetch_stocks, "get_store_path", return_value=Path("/tmp/rq_store")
        ), patch.object(fetch_stocks, "run_financials", return_value=True) as run_financials, patch.object(
            sys, "argv", ["fetch_stocks.py", "--fetch-financials", "--quarter", "2024q5"]
        ):
            rc = fetch_stocks.main()

        self.assertEqual(rc, 1)
        run_financials.assert_not_called()

    def test_main_rejects_quarter_range_when_start_after_end(self):
        with patch.object(fetch_stocks, "init_rqdatac", return_value=object()), patch.object(
            fetch_stocks, "get_store_path", return_value=Path("/tmp/rq_store")
        ), patch.object(fetch_stocks, "run_financials", return_value=True) as run_financials, patch.object(
            sys,
            "argv",
            [
                "fetch_stocks.py",
                "--fetch-financials",
                "--start-quarter",
                "2024q4",
                "--end-quarter",
                "2024q1",
            ],
        ):
            rc = fetch_stocks.main()

        self.assertEqual(rc, 1)
        run_financials.assert_not_called()


if __name__ == "__main__":
    unittest.main()
