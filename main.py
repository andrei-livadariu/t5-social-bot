import logging
import os
import pytz
import json

from datetime import datetime

from telegram.ext import ApplicationBuilder
from dotenv import load_dotenv

from helpers.business_logic.access_checker import AccessChecker
from helpers.business_logic.visit_calculator import VisitCalculator
from helpers.business_logic.ping_pong_calculator import PingPongCalculator
from helpers.business_logic.points import Points
from helpers.business_logic.raffle import Raffle
from helpers.telegram.chat_target import ChatTarget
from integrations.google.api import GoogleApi
from integrations.loyverse.api import LoyverseApi

from integrations.google.sheets.databases.community_database import CommunityDatabase
from integrations.google.sheets.databases.management_database import ManagementDatabase
from integrations.google.sheets.databases.ping_pong_database import PingPongDatabase
from integrations.google.sheets.databases.visits_database import VisitsDatabase

from modules.base_module import BaseModule
from modules.help import HelpModule
from modules.nominate import NominateModule
from modules.ping_pong import PingPongModule
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
        self.ping_pong_chats = ChatTarget.parse_multi(os.getenv('ping_pong_chats', ''))
        self.birthday_points = Points(os.getenv('birthday_points', 5))
        self.timezone = pytz.timezone(os.getenv('timezone', 'Europe/Bucharest'))
        self.masters = set([username for username in os.getenv('masters', '').split(',') if username])
        self.point_masters = set([username for username in os.getenv('point_masters', '').split(',') if username])
        self.google_api_credentials = os.getenv('google_api_credentials')
        self.community_google_spreadsheet_key = os.getenv('community_google_spreadsheet_key')
        self.management_google_spreadsheet_key = os.getenv('management_google_spreadsheet_key')
        self.visits_google_spreadsheet_key = os.getenv('visits_google_spreadsheet_key')
        self.ping_pong_google_spreadsheet_key = os.getenv('ping_pong_google_spreadsheet_key')
        self.xmas_loyverse_id = os.getenv('xmas_loyverse_id')
        self.visits_to_points = {int(visits): Points(points) for visits, points in json.loads(os.getenv('visits_to_points') or '{}').items()}


def main() -> None:
    config = MainConfig()
    logging.basicConfig(level=config.log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    google_api = GoogleApi(config.google_api_credentials)

    community = CommunityDatabase(
        api=google_api,
        spreadsheet_key=config.community_google_spreadsheet_key,
        timezone=config.timezone,
    )

    management = ManagementDatabase(
        api=google_api,
        spreadsheet_key=config.management_google_spreadsheet_key,
        timezone=config.timezone,
    )

    visits = VisitsDatabase(
        api=google_api,
        spreadsheet_key=config.visits_google_spreadsheet_key,
        timezone=config.timezone
    )

    ping_pong = PingPongDatabase(
        api=google_api,
        spreadsheet_key=config.ping_pong_google_spreadsheet_key,
        timezone=config.timezone
    )

    loy = LoyverseApi(config.loyverse_token, users=community.users, read_only=config.loyverse_read_only)
    ac = AccessChecker(
        masters=config.masters,
        point_masters=config.point_masters,
    )

    vc = VisitCalculator(
        checkpoints=config.visits_to_points,
        visits=visits.visits,
    )

    ppc = PingPongCalculator(
        standings=ping_pong.standings,
        matches=ping_pong.matches,
        timezone=config.timezone,
    )

    raffle = Raffle(
        loy=loy,
        users=community.users,
        entries=community.raffle_entries,
        title="Zăganu Giveaway",
        description="""Summer is finally here ☀️ and what better way to celebrate than with a nice, cold beer? 🍺 

We’re now serving ZAGANU on tap, and we’re giving away a Hefeweizen and an IPA to a couple of lucky community members!

The deadline is Sunday @ 4pm

Noroc & good luck! 🍻""",
        end_date=config.timezone.localize(datetime(2025, 6, 8, 16, 0, 0, 0)),
        ticket_price=Points(0),
        max_tickets=1
    )

    modules: list[BaseModule] = [
        PointsModule(loy=loy, users=community.users, visits=visits.visits, timezone=config.timezone),
        DonateModule(loy=loy, ac=ac, users=community.users, announcement_chats=config.announcement_chats),
        VisitsModule(
            loy=loy,
            users=community.users,
            vc=vc,
            timezone=config.timezone,
            admin_chats=config.admin_chats,
        ),
        BirthdayModule(
            loy=loy,
            ac=ac,
            users=community.users,
            admin_chats=config.admin_chats,
            points_to_award=config.birthday_points,
            timezone=config.timezone,
        ),
        EventsModule(
            repository=community.events,
            timezone=config.timezone,
            ac=ac,
            announcement_chats=config.announcement_chats,
            admin_chats=config.admin_chats,
        ),
        PingPongModule(
            users=community.users,
            calculator=ppc,
            chats=config.ping_pong_chats,
        ),
        TasksModule(tasks=management.tasks, tasks_chats=config.tasks_chats, timezone=config.timezone),
        AnnouncementsModule(
            ac=ac,
            users=community.users,
            visits=visits.visits,
            team_schedule_chats=config.team_schedule_chats,
            timezone=config.timezone
        ),
        NominateModule(
            users=community.users,
            visits=visits.visits,
            nominations=community.nominations,
            timezone=config.timezone,
        ),
        RaffleModule(raffle=raffle, users=community.users),
        TrackingModule(users=community.users, timezone=config.timezone),
    ]

    # The help module must be last because it catches all chat, and it picks up menu buttons from the other modules
    help_module = HelpModule(modules.copy())  # shallow copy
    modules.append(help_module)

    application = ApplicationBuilder().token(config.telegram_token).build()
    for module in modules:
        module.install(application)

    application.job_queue.run_repeating(callback=community.refresh_job, interval=60 * 5)  # Refresh every 5 minutes
    application.job_queue.run_repeating(callback=management.refresh_job, interval=60 * 5)  # Refresh every 5 minutes
    application.job_queue.run_repeating(callback=visits.refresh_job, interval=60 * 5)  # Refresh every 5 minutes
    application.job_queue.run_repeating(callback=ping_pong.refresh_job, interval=60 * 5)  # Refresh every 5 minutes

    # Start the Bot
    logger.info('start_polling')
    application.run_polling()


if __name__ == '__main__':
    main()

