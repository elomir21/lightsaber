import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from config.definitions import ROOT_DIR


class Balances:
    """Class responsible for balances methods"""

    def __init__(self):
        ...

    def send_one_balance(
        self,
        producer: KafkaProducer,
        account_id: str,
        client_id: str,
        request_id: int,
        request_date: str,
        available_amount: float,
        available_amount_currency: str,
        blocked_amount: float,
        blocked_amount_currency: str,
        automatically_invested_amount: float,
        automatically_invested_amount_currency: str,
    ) -> None:
        """Send one balance to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param account_id: The balance account ID
        :type account_id: str
        :param client_id: The balance client ID
        :type account_id: str
        :param request_id: The ID of request
        :type request_id: int
        :param request_date: The request date
        :type request_date: str
        :param available_amount: The balance available amount
        :type available_amount: float
        :param available_amount_currency: The available amount currency of balance
        :type available_amount_currency: str
        :param blocked_amount: The balance blocked amount
        :type blocked_amount: float
        :param blocked_amount_currency: The blocked amount currency of the balance
        :type blocked_amount_currency: str
        :param automatically_invested_amount: The automatically invested amount of the balance
        :type automatically_invested_amount: float
        :param automatically_invested_amount_currency: The automatically invested amount currency of the balance
        :type automatically_invested_amount_currency: str
        :return: None
        """
        data = {
            "accountId": account_id,
            "clientId": client_id,
            "requestId": int(request_id),
            "requestDate": request_date,
            "availableAmount": float(available_amount),
            "availableAmountCurrency": available_amount_currency,
            "blockedAmount": float(blocked_amount),
            "blockedAmountCurrency": blocked_amount_currency,
            "automaticallyInvestedAmount": float(automatically_invested_amount),
            "automaticallyInvestedAmountCurrency": automatically_invested_amount_currency,
        }

        future = producer.send("balancesObserver", json.dumps(data).encode("utf-8"))
        result = future.get(timeout=60)

        print(f"sent balance of account {account_id} to kafka")

    def send_balances(self, producer: KafkaProducer) -> None:
        """Send balances to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :return None
        """
        print("Method not implemented yet")
