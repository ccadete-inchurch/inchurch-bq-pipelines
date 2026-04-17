import os
from typing import Optional


class Config:
    """Configuration from environment variables"""

    # API Tokens
    INCHURCH_TOKEN: str = os.getenv("INCHURCH_TOKEN", "")
    A6_TOKEN: str = os.getenv("A6_TOKEN", "")

    # Superlogica API
    SUPERLOGICA_APP_TOKEN: str = os.getenv("SUPERLOGICA_APP_TOKEN", "")
    SUPERLOGICA_API_URL: str = os.getenv(
        "SUPERLOGICA_API_URL",
        "https://api.superlogica.net/v2/financeiro/"
    )

    # Database Connection
    DB_CONNECTION_STRING: str = os.getenv("DB_CONNECTION_STRING", "")

    # Notification Webhook
    NOTIFICATION_WEBHOOK_URL: str = os.getenv("NOTIFICATION_WEBHOOK_URL", "")
    NOTIFICATION_SPACE_ID: str = os.getenv("NOTIFICATION_SPACE_ID", "")

    # API Request Parameters
    API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "180"))  # 3 minutes - increased from 60s to handle large datasets
    API_CHUNK_SIZE: int = int(os.getenv("API_CHUNK_SIZE", "5000"))
    API_RATE_LIMIT_SLEEP: float = float(os.getenv("API_RATE_LIMIT_SLEEP", "0.2"))
    API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "3"))

    # Database Parameters
    DB_BATCH_SIZE: int = int(os.getenv("DB_BATCH_SIZE", "5000"))

    # Thread Pool
    THREAD_POOL_MAX_WORKERS: int = int(os.getenv("THREAD_POOL_MAX_WORKERS", "3"))

    @classmethod
    def get_token(cls, system: str) -> str:
        """Get API token for a specific system (inchurch or a6)"""
        if system.lower() == "inchurch":
            token = cls.INCHURCH_TOKEN
        elif system.lower() == "a6":
            token = cls.A6_TOKEN
        else:
            raise ValueError(f"Unknown system: {system}")

        if not token or token == "":
            raise ValueError(f"Token for system '{system}' is empty. Check environment variables.")

        return token

    @classmethod
    def validate(cls) -> bool:
        """Valida configurações críticas na startup"""
        required = [
            ("INCHURCH_TOKEN", cls.INCHURCH_TOKEN),
            ("A6_TOKEN", cls.A6_TOKEN),
            ("DB_CONNECTION_STRING", cls.DB_CONNECTION_STRING),
            ("SUPERLOGICA_APP_TOKEN", cls.SUPERLOGICA_APP_TOKEN),
        ]

        for name, value in required:
            if not value or value == "":
                raise ValueError(f"Environment variable {name} is empty or not set. Configure before starting.")

        return True

