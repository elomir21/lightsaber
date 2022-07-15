import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from config.definitions import ROOT_DIR


class Accounts:
    """Class responsible for accounts methods"""

    def __init__(self, base_url, account_url):
        self.base_url = base_url
        self.account_url = account_url

    def send_one_account(
        self,
        producer: KafkaProducer,
        compe_code: str,
        client_id: str,
        branch_code: str,
        number: str,
        account_type: str,
        account_subtype: str,
        description: str,
        check_digit: str,
        currency: str,
        account_id: str,
    ) -> None:
        """Send one account post request to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param compe_code: The id of the financial institution
        :type compe_code: str
        :param client_id: The id of the account owner
        :type client_id: str
        :param banch_code: The agency code
        :type branch_code: str
        :param number: The account number
        :type number: str
        :param account_type: The type of account
        :type account_type: list
        :param account_subtype: The subtype of the account
        :type account_subtype: list
        :param description: The account description
        :type description: str
        :param check_digit: The account digit
        :type check_digit: str
        :param currency: The coin type
        :type currency: str
        :param account_id: The account identifier of deposit, savings or payment
        :type account_id: str
        :return: None
        """
        data = {
            "compeCode": compe_code,
            "clientId": client_id,
            "branchCode": branch_code,
            "number": number,
            "type": account_type,
            "subtype": account_subtype,
            "description": description,
            "checkDigit": check_digit,
            "currency": currency,
            "accountId": account_id,
        }

        future = producer.send("accountsObserver", json.dumps(data).encode("utf-8"))
        result = future.get(timeout=60)

        print(f"sent account {account_id} to kafka")

    def send_accounts(self, producer: KafkaProducer) -> None:
        """Read and send accounts to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :return: None
        """
        with open(str(os.path.join(ROOT_DIR, "files", "account.csv")), "r") as files:
            next(files)
            for file in files:
                compe_code = file.split(",")[0]
                client_id = file.split(",")[1].strip("\n")
                account_type = file.split(",")[2].strip("\n")
                branch_code = file.split(",")[3].strip("\n")
                check_digit = file.split(",")[4].strip("\n")
                currency = file.split(",")[5].strip("\n")
                account_id = file.split(",")[6].strip("\n")
                number = file.split(",")[7].strip("\n")

                data = {
                    "compeCode": compe_code,
                    "clientId": client_id,
                    "branchCode": branch_code,
                    "number": number,
                    "type": account_type,
                    "checkDigit": check_digit,
                    "currency": currency,
                    "accountId": account_id,
                }

                future = producer.send(
                    "accountsObserver", json.dumps(data).encode("utf-8")
                )
                result = future.get(timeout=60)

                print(f"sent account {account_id} to kafka")

    def delete_one_account(self, account_id: str) -> None:
        """Delete one account from pluggy api

        :param client_id: The ID of client
        :type client_id: str
        :return: None
        """
        if "," in account_id:
            account_id = account_id.split(",")[6].strip("\n")

        response = requests.delete(self.base_url + self.account_url + account_id)

        print(
            f"The account {account_id} was deleted with status code: {response.status_code}"
        )

    def delete_accounts(self) -> None:
        """Delete accounts from pluggy api

        :return: None
        """
        with open(str(os.path.join(ROOT_DIR, "files", "account.csv")), "r") as files:
            next(files)
            with ThreadPoolExecutor(max_workers=24) as executor:
                executor.map(self.delete_one_account, files)
