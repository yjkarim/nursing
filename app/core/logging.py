import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """
    Central logging setup for the whole app.
    Call this once in main.py before anything else.
    """

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ],
    )

    # تخفيف logging بتاع libraries المزعجة (اختياري لكن مهم)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("gunicorn").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    logging.info("Logging configured successfully")


# Optional: shared logger instance
logger = logging.getLogger("tg-pdf")
