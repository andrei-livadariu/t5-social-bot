import logging
import requests
import json
import pytz
from typing import Optional, Iterable, Iterator
from datetime import datetime

import helpers.utils.json
from helpers.business_logic.points import Points

from data.models.user import User
from data.repositories.user import UserRepository
from helpers.business_logic.visit_calculator import RawVisit

from integrations.loyverse.customer import Customer
from integrations.loyverse.receipt import Receipt
from integrations.loyverse.exceptions import InsufficientFundsError, InvalidCustomerError

logger = logging.getLogger(__name__)


class LoyverseApi:
    BASE_URL = "https://api.loyverse.com/v1.0"
    CUSTOMERS_ENDPOINT = f"{BASE_URL}/customers"
    RECEIPTS_ENDPOINT = f"{BASE_URL}/receipts"

    def __init__(self, token: str, users: UserRepository, read_only: bool = False):
        self.token = token
        self.users = users
        self.read_only = read_only

    def get_balance(self, user: User) -> Points:
        return self._get_customer(user).points

    def add_points(self, user: User, points: Points) -> None:
        if points.is_zero:
            return

        customer = self._get_customer(user)
        customer.points += points
        self._save_customer(customer)

    def remove_points(self, user: User, points: Points) -> None:
        if points.is_zero:
            return

        customer = self._get_customer(user)
        if customer.points < points:
            raise InsufficientFundsError("You don't have enough points")

        customer.points -= points
        self._save_customer(customer)

    def load_visits(self, since: datetime, end: datetime|None = None) -> list[RawVisit]:
        # Load the receipts and convert them into visits (User + creation date)
        receipts = self.get_receipts(since, end)
        matched_receipts = self._match_receipts(receipts)
        return [(user, receipt.created_at) for (receipt, user) in matched_receipts]

    def load_spending(self, start: datetime, end: datetime) -> dict[User, float]:
        receipts = self.get_receipts(start, end)

        spending = {}
        for (receipt, user) in self._match_receipts(receipts):
            # Skip this kind of receipts since they are used when the user pays using points
            if receipt.total_money < 1.0:
                continue

            if user not in spending:
                spending[user] = 0.0

            spending[user] += receipt.total_money

        return spending

    def _match_receipts(self, receipts: Iterable[Receipt]) -> Iterator[tuple[Receipt, User]]:
        for receipt in receipts:
            user = self._receipt_to_user(receipt)
            if user:
                yield receipt, user

    def _receipt_to_user(self, receipt: Receipt) -> Optional[User]:
        return self.get_user_by_customer_id(receipt.customer_id) if receipt.customer_id else None

    def get_receipts(self, start: datetime, end: datetime|None = None) -> Iterator[Receipt]:
        filters = {
            'created_at_min': start.replace(microsecond=0).astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')
        }

        if end:
            filters['created_at_max'] = end.replace(microsecond=0).astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')

        limit = 250
        cursor = None

        # Emulate do-while
        while True:
            response = requests.get(
                url=self.RECEIPTS_ENDPOINT,
                params={'limit': limit, 'cursor': cursor, **filters},
                headers={"Authorization": f"Bearer {self.token}"}
            )

            if response.status_code != 200:
                logger.error(f"Loyverse get_receipts error {response.status_code} occurred.")
                break

            response_data = response.json()
            raw_receipts = response_data.get('receipts', [])
            cursor = response_data.get('cursor')

            for raw_receipt in raw_receipts:
                yield Receipt.from_json(raw_receipt, start.tzinfo)

            if not cursor or len(raw_receipts) < limit:
                break

    def get_all_points(self) -> Iterator[tuple[User, Points]]:
        for customer in self._get_all_customers():
            user = self.get_user_by_customer_id(customer.customer_id)
            if not user:
                continue
            yield user, customer.points

    def get_user_by_customer_id(self, customer_id: str) -> Optional[User]:
        user = self.users.get_by_loyverse_id(customer_id)
        return user if user else self._initialize_user_by_customer(customer_id)

    def _get_customer(self, user: User) -> Customer:
        customer = self._get_single_customer(user.loyverse_id) if user.loyverse_id else None

        if not customer:
            customer = self._initialize_customer_by_user(user)

        if not customer:
            raise InvalidCustomerError(f"The user @{user.telegram_username} is not a recognized Loyverse customer.")

        return customer

    def _get_single_customer(self, customer_id: str) -> Optional[Customer]:
        response = requests.get(f"{self.CUSTOMERS_ENDPOINT}/{customer_id}", headers={
            "Authorization": f"Bearer {self.token}"
        })

        if response.status_code != 200:
            logger.error(f"Loyverse get_single_customer error {response.status_code} occurred.")
            return None

        return Customer.from_json(response.json())

    def _get_single_customer_by_username(self, username: str) -> Optional[Customer]:
        for customer in self._get_all_customers():
            if customer.username == username:
                return customer
        return None

    def _initialize_customer_by_user(self, user: User) -> Optional[Customer]:
        if not user.telegram_username:
            return None

        customer = self._get_single_customer_by_username(user.telegram_username)
        if not customer:
            return None

        self._link_user_to_customer(user, customer)

        return customer

    def _initialize_user_by_customer(self, customer_id: str) -> Optional[User]:
        customer = self._get_single_customer(customer_id)
        if not customer:
            return None

        if not customer.username:
            return None

        user = self.users.get_by_telegram_name(customer.username)
        if not user:
            return None

        return self._link_user_to_customer(user, customer)

    def _link_user_to_customer(self, user: User, customer: Customer) -> User:
        # Save the customer id to the user data for future reference
        user = user.copy(loyverse_id=customer.customer_id)
        self.users.save(user)
        return user

    def _get_all_customers(self) -> Iterator[Customer]:
        limit = 250
        cursor = None

        # Emulate do-while
        while True:
            response = requests.get(
                url=self.CUSTOMERS_ENDPOINT,
                params={'limit': limit, 'cursor': cursor},
                headers={"Authorization": f"Bearer {self.token}"},
            )

            if response.status_code != 200:
                logger.error(f"Loyverse get_all_customers error {response.status_code} occurred.")
                break

            response_data = response.json()
            raw_customers = response_data.get('customers', [])
            cursor = response_data.get('cursor')

            for raw_customer in raw_customers:
                customer = Customer.from_json(raw_customer)
                if customer:
                    yield customer

            if not cursor or len(raw_customers) < limit:
                break

    def _save_customer(self, customer: Customer) -> None:
        data = json.dumps(customer, default=helpers.utils.json.default)
        if self.read_only:
            logger.info(data)
            return

        response = requests.post(self.CUSTOMERS_ENDPOINT, data=data, headers={
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        })

        if response.status_code != 200:
            logger.error(f"Loyverse save_customer error {response.status_code} occurred.")

        logger.info(response.json())

