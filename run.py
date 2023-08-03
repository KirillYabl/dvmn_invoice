import asyncio
import dataclasses
import datetime
import json
import os
import requests
import platform
import logging
import typing

import environs
import gspread
import pandas as pd
import telethon


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Config:
    google_credentials_file_path: str
    google_authorized_user_file_path: str
    devman_token: str
    month: int | None
    year: int | None
    google_table_name: str
    telegram_api_id: int
    telegram_api_hash: str
    confirm_every_reviewer: bool
    message_template_filename: str


class DvmnClient:
    def __init__(self, token: str) -> None:
        self.token = token

        self.invoice_url = "http://127.0.0.1:8000/reviewer/invoice/"
        self.auth_headers = {"Authorization": f"Token {self.token}"}

    
    def get_invoice(self, month: int, year: int) -> dict[str, typing.Any]:
        params = {
            "month": month,
            "year": year,
        }
        response = requests.get(self.invoice_url, params=params, headers=self.auth_headers)
        response.raise_for_status()
        return response.json()
    

class GoogleLoginer:
    def __init__(self, credentials_file_path: str, authorized_user_file_path: str):
        self.credentials_file_path = credentials_file_path
        self.authorized_user_file_path = authorized_user_file_path

        self.google_client = None

    def _login(self):
        if os.path.exists(self.authorized_user_file_path):
            with open(self.authorized_user_file_path) as f:
                self.authorized_user = json.load(f)
            google_client, authorized_user = gspread.oauth_from_dict(authorized_user_info=self.authorized_user)
        else:
            with open(self.credentials_file_path) as f:
                credentials = json.load(f)
            google_client, authorized_user = gspread.oauth_from_dict(credentials=credentials)

        self.google_client = google_client

        with open(self.authorized_user_file_path, 'w') as f:
            if type(authorized_user) == str:
                f.write(authorized_user)
            else:
                json.dump(authorized_user, f)

    def get_google_client(self) -> gspread.Client:
        if self.google_client is None:
            self._login()
        return self.google_client
    

def write_df_to_worksheet(spreadsheet: gspread.Spreadsheet, df: pd.DataFrame, worksheet_index: int, worksheet_name: str):
    try:
        worksheet = spreadsheet.get_worksheet(worksheet_index)
        worksheet.update_title(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheets_count = len(spreadsheet.worksheets())
        default_rows_count = 1_000
        default_cols_count = 26
        spreadsheet.add_worksheet(worksheet_name, default_rows_count, default_cols_count)
        worksheet = spreadsheet.get_worksheet(worksheets_count)

    worksheet.insert_row(list(df.columns))
    first_row_after_header = 2
    worksheet.insert_rows(df.values.tolist(), first_row_after_header)


async def amain(config: Config):
    credentials_file_path = config.google_credentials_file_path
    authorized_user_file_path = config.google_authorized_user_file_path

    with open(config.message_template_filename, encoding='utf8') as f:
        message_template = f.read()

    dvmn_client = DvmnClient(token=config.devman_token)
    google_loginer = GoogleLoginer(credentials_file_path=credentials_file_path, authorized_user_file_path=authorized_user_file_path)
    google_client = google_loginer.get_google_client()

    invoice = dvmn_client.get_invoice(config.month, config.year)
    month_reviews_df = pd.DataFrame(data=invoice["month_reviews"])
    summary_df = pd.DataFrame(data=invoice["summary"])
    dvmn_reviewers = invoice["dvmn_reviewers"]

    for dvmn_reviewer_username, dvmn_reviewer_telegram in dvmn_reviewers.items():
        reviewer_summary_df = summary_df[summary_df['Ревьюер'] == dvmn_reviewer_username]
        reviewer_month_reviews_df = month_reviews_df[month_reviews_df['Ревьюер'] == dvmn_reviewer_username]
        if not reviewer_summary_df.shape[0]:
            logger.info(f'У ревьюера {dvmn_reviewer_username} не было работ в этом месяце')
            continue

        if config.confirm_every_reviewer:
            msg = f'Обрабатывать ревьюера {dvmn_reviewer_username}? Введите что угодно если да, иначе просто нажмите Enter'
            process_reviewer = input(msg)
            if not process_reviewer:
                logger.info(f'Пропускаю ревьюера {dvmn_reviewer_username}')
                continue

        if not dvmn_reviewer_telegram:
            msg = f"Для ревьюера {dvmn_reviewer_username} не указан телеграм в БД Dvmn, укажите самостоятельно"
            dvmn_reviewer_telegram = input(msg)
            if not dvmn_reviewer_telegram:
                logger.warning(f'Вы не указали телеграм для ревьюера {dvmn_reviewer_username}, пропускаю')
                continue
        
        year_month = f"{config.year}_{config.month:02d}"
        google_table_name = config.google_table_name.format(year_month=year_month, username=dvmn_reviewer_username)
        spreadsheet = google_client.create(google_table_name)
        spreadsheet_url = spreadsheet.url
        spreadsheet.share(email_address=None, perm_type='anyone', role='reader', with_link=True, notify=False)

        write_df_to_worksheet(spreadsheet=spreadsheet, df=reviewer_month_reviews_df, worksheet_index=0, worksheet_name="Ревью")
        write_df_to_worksheet(spreadsheet=spreadsheet, df=reviewer_summary_df, worksheet_index=1, worksheet_name="Суммарно")

        tg_client = telethon.TelegramClient(
            "session", 
            config.telegram_api_id, 
            config.telegram_api_hash,
            system_version=platform.platform(),
        )
        message = message_template.format(year=config.year, month=config.month, spreadsheet_url=spreadsheet_url)
        async with tg_client:
            await tg_client.send_message(dvmn_reviewer_telegram, message)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    env = environs.Env()
    env.read_env()

    now = datetime.datetime.now()
    max_days_in_month = 31
    for day in range(1, max_days_in_month + 1):
        prev_month_date = now - datetime.timedelta(days=day)
        if prev_month_date.month != now.month:
            default_month = prev_month_date.month
            default_year = prev_month_date.year
            break

    config = Config(
        google_credentials_file_path=env.str('GOOGLE_CREDENTIALS_FILE_PATH'),
        google_authorized_user_file_path=env.str('GOOGLE_AUTHORIZED_USER_FILE_PATH', ''),
        devman_token=env.str('DEVMAN_TOKEN'),
        month=env.int('MONTH', default_month),
        year=env.int('YEAR', default_year),
        google_table_name=env.str('GOOGLE_TABLE_NAME'),
        telegram_api_id=env.int('TELEGRAM_API_ID'),
        telegram_api_hash=env.str('TELEGRAM_API_HASH'),
        confirm_every_reviewer=env.bool('CONFIRM_EVERY_REVIEWER', False),
        message_template_filename=env.str('MESSAGE_TEMPLATE_FILENAME')
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(amain(config))