import logging
import os
import pytz
import json

from telegram.ext import ApplicationBuilder
from dotenv import load_dotenv

from helpers.access_checker import AccessChecker
from helpers.visit_calculator import VisitCalculator
from helpers.points import Points
from helpers.raffle import Raffle
from helpers.chat_target import ChatTarget

from integrations.loyverse.api import LoyverseApi
from integrations.google.community_database import CommunityDatabase
from integrations.google.management_database import ManagementDatabase
from integrations.google.sheet_event_repository import GoogleSheetEventRepository
from integrations.google.sheet_user_repository import GoogleSheetUserRepository
from integrations.google.sheet_task_repository import GoogleSheetTaskRepository
from integrations.google.sheet_raffle_repository import GoogleSheetRaffleRepository

from modules.help import HelpModule
from modules.points import PointsModule
from modules.donate import DonateModule
from modules.raffle import RaffleModule
from modules.birthday import BirthdayModule
from modules.events import EventsModule
from modules.visits import VisitsModule
from modules.tasks import TasksModule
from modules.announcements import AnnouncementsModule
from modules.tracking import TrackingModule

load_dotenv()

logger = logging.getLogger(__name__)


class MainConfig:
    def __init__(self):
        self.log_level = logging.getLevelName(os.getenv('log_level', 'INFO'))
        self.telegram_token = os.getenv('telegram_token')
        self.loyverse_token = os.getenv('loyverse_token')
        self.loyverse_read_only = bool(int(os.getenv('loyverse_read_only', 0)))
        self.announcement_chats = ChatTarget.parse_multi(os.getenv('announcement_chats', ''))
        self.admin_chats = ChatTarget.parse_multi(os.getenv('admin_chats', ''))
        self.tasks_chats = ChatTarget.parse_multi(os.getenv('tasks_chats', ''))
        self.team_schedule_chats = ChatTarget.parse_multi(os.getenv('team_schedule_chats', ''))
        self.birthday_points = Points(os.getenv('birthday_points', 5))
        self.timezone = pytz.timezone(os.getenv('timezone', 'Europe/Bucharest'))
        self.masters = set([username for username in os.getenv('masters', '').split(',') if username])
        self.point_masters = set([username for username in os.getenv('point_masters', '').split(',') if username])
        self.google_api_credentials = os.getenv('google_api_credentials')
        self.community_google_spreadsheet_key = os.getenv('community_google_spreadsheet_key')
        self.management_google_spreadsheet_key = os.getenv('management_google_spreadsheet_key')
        self.xmas_loyverse_id = os.getenv('xmas_loyverse_id')
        self.visits_to_points = {int(visits): Points(points) for visits, points in json.loads(os.getenv('visits_to_points') or '{}').items()}


def main() -> None:
    config = MainConfig()
    logging.basicConfig(level=config.log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    community_database = CommunityDatabase(
        spreadsheet_key=config.community_google_spreadsheet_key,
        api_credentials=config.google_api_credentials,
    )

    event_repository = GoogleSheetEventRepository(community_database.events, config.timezone)
    user_repository = GoogleSheetUserRepository(community_database.users, config.timezone)
    raffle_repository = GoogleSheetRaffleRepository(community_database.raffle, config.timezone)

    management_database = ManagementDatabase(
        spreadsheet_key=config.management_google_spreadsheet_key,
        api_credentials=config.google_api_credentials,
    )

    task_repository = GoogleSheetTaskRepository(management_database.tasks, config.timezone)

    loy = LoyverseApi(config.loyverse_token, users=user_repository, read_only=config.loyverse_read_only)
    ac = AccessChecker(
        masters=config.masters,
        point_masters=config.point_masters,
    )

    vc = VisitCalculator(
        checkpoints=config.visits_to_points
    )

    raffle = Raffle(loy, entries=raffle_repository, title="Euro 2024 Sweepstakes", ticket_price=Points(5), max_tickets=3, is_active=False)

    modules = [
        PointsModule(loy=loy, users=user_repository, timezone=config.timezone),
        DonateModule(loy=loy, ac=ac, users=user_repository, announcement_chats=config.announcement_chats),
        VisitsModule(loy=loy, users=user_repository, vc=vc, timezone=config.timezone),
        RaffleModule(raffle=raffle, users=user_repository),
        BirthdayModule(
            loy=loy,
            ac=ac,
            users=user_repository,
            announcement_chats=config.announcement_chats,
            admin_chats=config.admin_chats,
            points_to_award=config.birthday_points,
            timezone=config.timezone,
        ),
        EventsModule(
            repository=event_repository,
            timezone=config.timezone,
            ac=ac,
            announcement_chats=config.announcement_chats,
            admin_chats=config.admin_chats,
        ),
        TasksModule(tasks=task_repository, tasks_chats=config.tasks_chats, timezone=config.timezone),
        AnnouncementsModule(team_schedule_chats=config.team_schedule_chats, timezone=config.timezone),
        TrackingModule(users=user_repository, timezone=config.timezone),
    ]

    # The help module must be last because it catches all chat, and it picks up menu buttons from the other modules
    help_module = HelpModule(modules.copy())  # shallow copy
    modules.append(help_module)

    application = ApplicationBuilder().token(config.telegram_token).build()
    for module in modules:
        module.install(application)

    application.job_queue.run_repeating(callback=community_database.refresh_job, interval=60 * 5)  # Refresh every 5 minutes
    application.job_queue.run_repeating(callback=management_database.refresh_job, interval=60 * 5)  # Refresh every 5 minutes

    # Start the Bot
    logger.info('start_polling')
    application.run_polling()


if __name__ == '__main__':
    main()

