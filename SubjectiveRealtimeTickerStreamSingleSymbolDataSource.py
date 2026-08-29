import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from subjective_abstract_data_source_package import SubjectiveDataSource

from trading_contracts.plugin_support import (
    TICKER_OUTPUT_SCHEMA,
    empty_ticker,
    icon_for,
    ticker_stream,
)


class SubjectiveRealtimeTickerStreamSingleSymbolDataSource(SubjectiveDataSource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.symbol = str(self._connection.get("symbol", "")).upper()

    @classmethod
    def connection_schema(cls):
        return {"symbol": {"type": "text", "label": "Binance Symbol", "required": True}}

    @classmethod
    def request_schema(cls):
        return {"symbol": {"type": "text", "label": "Symbol"}, "events": {"type": "array", "label": "Injected Events"}}

    @classmethod
    def output_schema(cls):
        return TICKER_OUTPUT_SCHEMA

    @classmethod
    def icon(cls):
        return icon_for(__file__)

    def supports_streaming(self):
        return True

    def stream(self, request):
        yield from ticker_stream(request or {}, self._connection, "single")

    def run(self, request):
        request = request or {}
        symbol = str(request.get("symbol") or self.symbol).upper()
        if not symbol:
            return empty_ticker("symbol is required")
        try:
            event = next(ticker_stream(request, {**self._connection, "symbol": symbol}, "single"))
            if event.get("error"):
                return empty_ticker(event["error"])
            return {**event, "error": ""}
        except StopIteration:
            return empty_ticker()
