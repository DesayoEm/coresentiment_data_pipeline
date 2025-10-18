from pydantic_settings import BaseSettings
from dotenv import load_dotenv


load_dotenv()

class Settings(BaseSettings):
    PAGE_VIEWS_DIR: str


config = Settings()