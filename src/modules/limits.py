import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from config.definitions import ROOT_DIR


class Limits:
    """Class responsible for limits"""

    def __init__(self):
        pass

    def send_one_credit_card_limit(
        self,
        producer: KafkaProducer,
        credit_card_account_id: str,
        available_amount: float,
        consolidation_type: str,
        credit_line_limit_type: str,
        is_limit_flexible: bool,
        limit_amount: float,
        limit_amount_currency: str,
        line_name: str,
        line_name_additional_info: str,
        used_amount: float,
        identification_number: str,
        used_amount_currency: str,
        available_amount_currency: str,
    ) -> None:
        """Send one credit card limit to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param credit_card_account_id: Account id of credit card
        :type credit_card_account_id: str
        :param available_amount: Available limit values
        :type available_amount: float
        :param consolidation_type: Value limit indication
        :type consolidation_type: str
        :param credit_line_limit_type: Limit indication type
        :type credit_line_limit_type: str
        :param is_limit_flexible: Indicate if limit is flexible
        :type is_limit_flexible: bool
        :param limit_amount: The amount of limit
        :type float
        :param limit_amount_currency: The currecy of amount limit
        :type limit_amount_currency: str
        :param line_name: The line name
        :type line_name: str
        :param line_name_additional_info: The addicional info of the line name
        :type line_name_additional_info: str
        :param used_amount: Limit amount used
        :type used_amount: float
        :param identification_number: The identification number
        :type identification_number: str
        :param used_amount_currency: The used amount currency
        :type used_amount_currency: str
        :param available_amount_currency: The available amount currency
        :type available_amount_currency: str
        :return: None
        """
        data = [
            {
                "creditCardAccountId": credit_card_account_id,
                "availableAmount": float(available_amount),
                "consolidationType": consolidation_type,
                "creditLineLimitType": credit_line_limit_type,
                "isLimitFlexible": bool(is_limit_flexible),
                "limitAmount": limit_amount,
                "limitAmountCurrency": limit_amount_currency,
                "lineName": line_name,
                "lineNameAdditionalInfo": line_name_additional_info,
                "usedAmount": float(used_amount),
                "identificationNumber": identification_number,
                "usedAmountCurrency": used_amount_currency,
                "availableAmountCurrency": available_amount_currency,
            }
        ]

        future = producer.send(
            "creditCardLimitsObserver", json.dumps(data).encode("utf-8")
        )
        result = future.get(timeout=60)

        print(f"sent limit of credit card {identification_number} to kafka")

    def send_credit_card_limits(self, producer: KafkaProducer) -> None:
        """Send credit_card_limits to kafka

        :return: None
        """
        print("Method not implemented yet")
