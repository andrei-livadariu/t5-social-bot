import logging
from reactivex import Observable
from reactivex.subject import Subject

import gspread

from integrations.google.api import GoogleApi

logger = logging.getLogger(__name__)


class GoogleSheetDatabase:
    def __init__(self, spreadsheet_key: str, api_credentials: str = None, api: GoogleApi = None):
        self.api = api if api else GoogleApi(api_credentials)
        self.spreadsheet_key = spreadsheet_key

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
        logger.info(f"Loading spreadsheet {self.spreadsheet_key}")
        return self.api.get_spreadsheet(self.spreadsheet_key)
