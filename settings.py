from pydantic import BaseSettings


class Settings(BaseSettings):
    ib_username: str
    ib_user_pwd: str
    sql_user: str
    sql_user_pwd: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
