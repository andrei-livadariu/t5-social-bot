import logging
import pytz
from datetime import datetime
from typing import Optional

from reactivex import Observable
from reactivex.subject import Subject

import gspread

from integrations.google.api import GoogleApi

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, api: GoogleApi, spreadsheet_key: str, timezone: pytz.timezone):
        self._api = api
        self._spreadsheet_key = spreadsheet_key
        self.timezone = timezone

        self._spreadsheet = Subject()

    @property
    def spreadsheet(self) -> Observable:
        return self._spreadsheet

    def refresh(self) -> None:
        logger.info('Refreshing Google Sheets data')
        try:
            self._spreadsheet.on_next(self.load())
        except Exception as e:
            self._spreadsheet.on_error(e)

    async def refresh_job(self, context) -> None:
        self.refresh()

    def load(self) -> gspread.Spreadsheet:
        logger.info(f"Loading spreadsheet {self._spreadsheet_key}")
        return self._api.get_spreadsheet(self._spreadsheet_key)

    def from_datetime_string(self, datetime_string: str)-> Optional[datetime]:
        try:
            return self.timezone.localize(datetime.strptime(datetime_string.strip(), '%Y-%m-%d %H:%M:%S'))
        except ValueError:
            return None

    def to_datetime_string(self, datetime_object: Optional[datetime]) -> str:
        if not datetime_object:
            return ''

        localized_datetime = datetime_object.replace(tzinfo=self.timezone) if datetime_object.tzinfo else self.timezone.localize(datetime_object)

        return localized_datetime.strftime('%Y-%m-%d %H:%M:%S')
