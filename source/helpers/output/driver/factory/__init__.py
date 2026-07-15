"""Output driver factory — creates the right driver for the destination."""

from exception.exception import OutputDriverNotRecognizeException
from helpers.output.driver.elasticsearch import ElasticsearchOutputDriver
from helpers.output.driver.file import FileOutputDriver
from helpers.output.driver.kafka import KafkaOutputDriver
from helpers.output.driver.std import StdOutputDriver

# Registry: destination string → driver class
_DRIVERS: dict[str, type] = {
    "kafka": KafkaOutputDriver,
    "elasticsearch": ElasticsearchOutputDriver,
    "file": FileOutputDriver,
    "std": StdOutputDriver,
}


class OutputDriverFactory:
    """Factory that instantiates the correct OutputDriver based on kwargs.

    All defaults come from :mod:`library.config` settings; CLI kwargs
    (``output``, ``bootstrap_servers``, ``elasticsearch_hosts``) override.
    """

    @staticmethod
    def create_output_driver(*args, **kwargs):
        """Create an output driver for ``kwargs['destination']``."""
        destination = kwargs.get("destination")
        if not destination:
            raise OutputDriverNotRecognizeException(
                "Destination is required (-d kafka|elasticsearch|file|std)"
            )

        driver_cls = _DRIVERS.get(destination)
        if driver_cls is None:
            raise OutputDriverNotRecognizeException(
                f"Unknown destination '{destination}'. Use: {', '.join(_DRIVERS)}"
            )

        # Copy kwargs so we don't mutate the caller's dict
        driver_kwargs = dict(kwargs)

        if destination == "kafka":
            from library.config import settings

            return KafkaOutputDriver(
                topic=driver_kwargs.pop("output", None) or settings.kafka.topic,
                bootstrap_servers=driver_kwargs.pop("bootstrap_servers", None)
                or settings.kafka.bootstrap_servers,
                client_id=settings.kafka.client_id,
                acks=settings.kafka.acks,
                compression_type=settings.kafka.compression_type,
                max_request_size=settings.kafka.max_request_size,
                linger_ms=settings.kafka.linger_ms,
                request_timeout_ms=settings.kafka.request_timeout_ms,
                *args,
                **driver_kwargs,
            )
        elif destination == "elasticsearch":
            from library.config import settings

            hosts = driver_kwargs.pop("elasticsearch_hosts", None) or settings.elasticsearch.hosts
            if isinstance(hosts, str):
                hosts = [hosts]
            return ElasticsearchOutputDriver(
                index_name=driver_kwargs.pop("output", None)
                or settings.elasticsearch.index_name,
                hosts=hosts,
                api_key=settings.elasticsearch.api_key,
                username=settings.elasticsearch.username,
                password=settings.elasticsearch.password,
                request_timeout=driver_kwargs.pop(
                    "request_timeout", settings.elasticsearch.request_timeout
                ),
                max_retries=driver_kwargs.pop(
                    "max_retries", settings.elasticsearch.max_retries
                ),
                *args,
                **driver_kwargs,
            )
        elif destination == "file":
            return FileOutputDriver(
                path=driver_kwargs.pop("output", None),
                *args,
                **driver_kwargs,
            )
        elif destination == "std":
            return StdOutputDriver(*args, **driver_kwargs)
