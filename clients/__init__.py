from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional


class BatchExecutorMixin:
    """Utility mixin providing batched, threaded execution for API client calls."""

    request_throttle_seconds: float = 0.1

    def _run_batch(
        self,
        requests: Iterable[Mapping[str, Any]],
        worker: Callable[..., Any],
        *,
        batch_size: int = 100,
        max_workers: Optional[int] = None,
    ) -> List[Any]:
        """
        Execute the provided worker for each request mapping using batches of threads.

        Args:
            requests: Iterable of mapping objects where each mapping contains the keyword
                arguments for a single worker invocation.
            worker: Callable that accepts keyword arguments unpacked from each request.
            batch_size: Number of concurrent requests to dispatch in each batch.
            max_workers: Optional upper bound on the thread pool size. Defaults to the lesser
                of `batch_size` and the number of requests in the current batch.

        Returns:
            List of worker results in the same order as the supplied requests. If a worker
            raises an exception, the corresponding entry will contain that exception instance.
        """
        request_list = [dict(params) for params in requests]
        if not request_list:
            return []
        if batch_size <= 0:
            raise ValueError("batch_size must be positive.")

        results: List[Any] = [None] * len(request_list)
        for offset in range(0, len(request_list), batch_size):
            chunk = request_list[offset : offset + batch_size]
            worker_count = min(len(chunk), batch_size)
            if max_workers is not None:
                worker_count = min(worker_count, max(1, max_workers))

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                throttle = max(0.0, getattr(self, "request_throttle_seconds", 0.0))
                futures = {}
                for index, params in enumerate(chunk, start=offset):
                    if throttle and futures:
                        time.sleep(throttle)
                    futures[executor.submit(worker, **params)] = index
                for future in as_completed(futures):
                    index = futures[future]
                    try:
                        results[index] = future.result()
                    except Exception as exc:  # pragma: no cover - best-effort batch handling
                        results[index] = exc

        return results
