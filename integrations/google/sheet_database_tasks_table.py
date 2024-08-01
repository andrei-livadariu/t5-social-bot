import logging

from gspread.utils import Dimension

from integrations.google.sheet_database_table import GoogleSheetDatabaseTable

logger = logging.getLogger(__name__)

weekday_keys = {'time': 0, 'name': 1, 'is_done': 2}
weekday_cols = len(weekday_keys)


class GoogleSheetDatabaseTasksTable(GoogleSheetDatabaseTable):
    _dimension = Dimension.cols

    def check_task(self, task: dict[str,str]) -> None:
        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            raw = self._load_values(worksheet)

            weekday = int(task['weekday'])
            column_number = weekday * weekday_cols + weekday_keys['is_done'] + 1
            tasks = self._parse_weekday(raw, weekday)

            for row_number, current_task in tasks.items():
                if task['name'] == current_task['name'] and task['time'] == current_task['time']:
                    worksheet.update_cell(row_number, column_number, task['is_done'])
                    return
        except Exception as e:
            logger.exception(e)

    def insert(self, data: dict[str,str]):
        pass

    def update(self, data: dict[str, dict[str,str]]):
        pass

    def _parse(self, raw: list[list]) -> list[dict[str, str]]:
        tasks_by_weekday = [self._parse_weekday(raw, weekday).values() for weekday in range(0, 7)]
        flat = [task for weekday_tasks in tasks_by_weekday for task in weekday_tasks]
        return flat

    def _parse_weekday(self, raw: list[list], weekday: int) -> dict[int, dict[str, str]]:
        # This method preserves the row numbers from the Google Sheet as they are used for updating
        start = weekday * weekday_cols
        end = start + weekday_cols

        if len(raw) <= start:
            raise ValueError("The sheet does not contain the requested weekday")

        # Fill any missing columns at the end with blanks
        if len(raw) < end:
            for i in range(len(raw), end):
                raw.append([''] * len(raw[0]))

        raw_today = list(zip(*raw[start:end]))

        tasks = {}

        last_time = ''
        for i, row in enumerate(raw_today):
            task = dict(zip(weekday_keys.keys(), row))

            # Skip tasks with empty names; this handles the various table headers
            name = task.get('name', '').strip()
            if not name:
                continue
            task['name'] = name

            # Fill in the missing time
            time = task.get('time', '').strip()
            if time:
                last_time = time
            task['time'] = last_time

            task['weekday'] = weekday
            tasks[i + 1] = task

        return tasks
