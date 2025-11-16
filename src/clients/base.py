from __future__ import annotations

import concurrent.futures
import time
from typing import Any, Callable, Iterable, List, Mapping, Tuple, Union
import datetime as dt
import pandas as pd


class WeatherClient:
    """Base class for weather clients."""

    def get_historical_data(
        self,
        *,
        location: Union[str, Tuple[float, float]],
        start_date: Union[str, dt.date],
        end_date: Union[str, dt.date],
    ) -> pd.DataFrame:
        raise NotImplementedError


class BatchExecutorMixin:
    """
    A mixin for clients that support batch execution of API requests.

    This mixin provides a `_run_batch` method that uses a thread pool to execute
    requests in parallel. It also handles batching of requests and throttling

    between batches to avoid rate limiting.
    """

    request_throttle_seconds: float = 0.0

    def _run_batch(
        self,
        requests: Iterable[Mapping[str, Any]],
        worker_fn: Callable[..., Any],
        *,
        batch_size: int,
        max_workers: int | None = None,
    ) -> List[Any]:
        """
        Execute a batch of requests in parallel using a thread pool.

        Args:
            requests: An iterable of request payloads (dictionaries).
            worker_fn: The function to call for each request.
            batch_size: The number of requests to include in each batch.
            max_workers: The maximum number of worker threads to use.

        Returns:
            A list of results from the worker function, in the same order as the
            input requests.
        """
        results: List[Any] = []
        request_list = list(requests)
        if not request_list:
            return results

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            batch_futures: List[concurrent.futures.Future] = []
            for i in range(0, len(request_list), batch_size):
                batch = request_list[i : i + batch_size]
                for request_params in batch:
                    future = executor.submit(worker_fn, **request_params)
                    batch_futures.append(future)

                # Throttle between batches
                if i + batch_size < len(request_list) and self.request_throttle_seconds > 0:
                    time.sleep(self.request_throttle_seconds)

            for future in concurrent.futures.as_completed(batch_futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(exc)
        return results