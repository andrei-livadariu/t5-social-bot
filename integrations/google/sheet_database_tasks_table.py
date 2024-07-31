import logging

from gspread.utils import Dimension

from integrations.google.sheet_database_table import GoogleSheetDatabaseTable

logger = logging.getLogger(__name__)


class GoogleSheetDatabaseTasksTable(GoogleSheetDatabaseTable):
    def check_task(self, task: dict[str,str]) -> None:
        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            raw = worksheet.get_values(major_dimension=Dimension.cols)

            keys = ['time', 'name', 'is_done']
            cols = len(keys)

            weekday = int(task['weekday'])
            start = weekday * cols
            filtered_columns = raw[start:(start + cols)]
            zipped_rows = list(zip(*filtered_columns))
            keyed_rows = [dict(zip(keys, row)) for row in zipped_rows]

            last_time = ''
            for i, row in enumerate(keyed_rows):
                name = row.get('name', '').strip()
                if not name:
                    continue
                time = row.get('time', '').strip() or last_time
                last_time = time

                if task['name'] == name and task['time'] == time:
                    worksheet.update_cell(i + 1, start + 2 + 1, task['is_done'])
                    return
        except Exception as e:
            logger.exception(e)

    def insert(self, data: dict[str,str]):
        pass

    def update(self, data: dict[str, dict[str,str]]):
        pass

    def _parse(self, raw: list[list]) -> list[dict]:
        weekdays = [day for day in raw[0] if day]
        if len(weekdays) != 7:
            raise ValueError("The sheet does not contain the necessary weekdays")

        keys = ['time', 'name', 'is_done']
        cols = len(keys)

        tasks = []

        for row in raw[2:]:
            for weekday in range(0, len(row) // cols):
                start = weekday * cols
                end = start + cols
                task = dict(zip(keys, row[start:end]))
                task['weekday'] = weekday
                tasks.append(task)

        return tasks
