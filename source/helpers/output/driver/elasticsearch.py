"""Elasticsearch output driver — async elasticsearch-py client on a background loop."""

import asyncio
import json
import logging
import threading
from typing import Optional

from elasticsearch import AsyncElasticsearch

from helpers.output.driver import OutputDriver

logger = logging.getLogger(__name__)


class ElasticsearchOutputDriver(OutputDriver):
    """Index documents into Elasticsearch via :class:`AsyncElasticsearch`.

    The async client runs on a **dedicated background thread** with its own
    event loop, so the synchronous ``put()`` interface never deadlocks with
    the caller's event loop (same pattern as the Kafka driver).

    Errors are logged but do **not** raise — the pipeline continues
    on best-effort.
    """

    name = "elasticsearch"

    def __init__(
        self,
        index_name: str,
        hosts: list[str] | str,
        *args,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        request_timeout: int = 30,
        max_retries: int = 3,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.index_name = index_name
        self._hosts = [hosts] if isinstance(hosts, str) else list(hosts)
        self._api_key = api_key
        self._basic_auth = (username, password) if username and password else None
        self._request_timeout = request_timeout
        self._max_retries = max_retries
        self._client: Optional[AsyncElasticsearch] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._start_background()

    # ------------------------------------------------------------------
    # OutputDriver interface
    # ------------------------------------------------------------------

    def put(self, output: str, **kwargs):
        """Index *output* (JSON string or dict) into Elasticsearch."""
        index = kwargs.get("index", self.index_name)
        doc_id = kwargs.get("doc_id")

        if isinstance(output, str):
            try:
                doc = json.loads(output)
            except json.JSONDecodeError:
                logger.error("Invalid JSON for ES indexing")
                return
        elif isinstance(output, dict):
            doc = output
        else:
            logger.error("Unsupported output type for ES: %s", type(output))
            return

        if not self._ready.wait(timeout=30):
            logger.error("ES client not ready — dropping document for index=%s", index)
            return

        if self._loop is None or self._client is None:
            logger.error("ES client not available for index=%s", index)
            return

        future = asyncio.run_coroutine_threadsafe(
            self._index(index, doc, doc_id), self._loop
        )
        try:
            future.result(timeout=self._request_timeout + 5)
        except Exception as err:
            logger.error("ES indexing failed for index=%s: %s", index, err)

    def close(self):
        """Stop the background client thread."""
        if self._loop is not None and self._client is not None:
            future = asyncio.run_coroutine_threadsafe(
                self._client.close(), self._loop
            )
            try:
                future.result(timeout=10)
            except Exception:
                pass
            self._client = None
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _start_background(self):
        """Start the AsyncElasticsearch client on a dedicated background thread."""

        def _run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            try:
                self._client = AsyncElasticsearch(
                    hosts=self._hosts,
                    api_key=self._api_key,
                    basic_auth=self._basic_auth,
                    request_timeout=self._request_timeout,
                    max_retries=self._max_retries,
                    retry_on_timeout=True,
                )
                self._ready.set()
                logger.info("ElasticsearchOutputDriver connected to %s", self._hosts)
                loop.run_forever()
            except Exception as e:
                logger.error("ES background loop error: %s", e)
                self._ready.set()  # unblock waiters
            finally:
                if self._client is not None:
                    try:
                        loop.run_until_complete(self._client.close())
                    except Exception:
                        pass
                loop.close()

        self._thread = threading.Thread(target=_run_loop, daemon=True, name="es-indexer")
        self._thread.start()

    async def _index(self, index: str, doc: dict, doc_id: str | None):
        """Async index call — runs on the background loop."""
        assert self._client is not None
        resp = await self._client.index(index=index, id=doc_id, document=doc)
        logger.debug("Indexed doc_id=%s into index=%s (result=%s)",
                     doc_id, index, resp.get("result"))
