from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv


load_dotenv()

class Settings(BaseSettings):
    RAW_PAGE_VIEWS_DIR: str
    PROCESSED_PAGE_VIEWS_DIR: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


config = Settings()