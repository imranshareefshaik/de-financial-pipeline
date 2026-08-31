import logging
from typing import Any, Dict, Optional

import requests


class APIClient:
    """Client for fetching crypto market data from the CoinGecko API."""

    def __init__(
        self,
        base_url: str = "https://api.coingecko.com/api/v3",
        timeout: int = 15,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)

    def _request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            self.logger.exception("Request timed out while calling %s", url)
            raise RuntimeError(f"Request to {url} timed out.") from exc
        except requests.exceptions.RequestException as exc:
            self.logger.exception("Request failed while calling %s", url)
            raise RuntimeError(f"Failed to fetch data from {url}: {exc}") from exc

        try:
            return response.json()
        except ValueError as exc:
            self.logger.exception("Received invalid JSON from %s", url)
            raise RuntimeError(f"Invalid JSON response received from {url}.") from exc

    def get_market_data(
        self,
        vs_currency: str = "usd",
        ids: Optional[str] = None,
        per_page: int = 100,
        page: int = 1,
        sparkline: bool = False,
        price_change_percentage: Optional[str] = None,
    ) -> Any:
        """Fetch current market data for coins from CoinGecko."""
        params: Dict[str, Any] = {
            "vs_currency": vs_currency,
            "per_page": per_page,
            "page": page,
            "sparkline": sparkline,
        }
        if ids:
            params["ids"] = ids
        if price_change_percentage:
            params["price_change_percentage"] = price_change_percentage

        self.logger.info("Fetching market data from CoinGecko for vs_currency=%s", vs_currency)
        return self._request("/coins/markets", params=params)

    def get_coin_data(
        self,
        coin_id: str,
        localization: bool = False,
        tickers: bool = False,
        market_data: bool = True,
        community_data: bool = False,
        developer_data: bool = False,
        sparkline: bool = False,
    ) -> Any:
        """Fetch detailed metadata and metrics for a single coin."""
        params: Dict[str, Any] = {
            "localization": localization,
            "tickers": tickers,
            "market_data": market_data,
            "community_data": community_data,
            "developer_data": developer_data,
            "sparkline": sparkline,
        }
        self.logger.info("Fetching detailed data for coin_id=%s", coin_id)
        return self._request(f"/coins/{coin_id}", params=params)

    def get_global_market_data(self) -> Any:
        """Fetch global cryptocurrency market data."""
        self.logger.info("Fetching global market data from CoinGecko")
        return self._request("/global")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    client = APIClient()
    try:
        data = client.get_market_data(ids="bitcoin,ethereum", per_page=2)
        print(data)
    except RuntimeError as exc:
        logging.error("Failed to fetch CoinGecko data: %s", exc)
