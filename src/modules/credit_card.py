import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from config.definitions import ROOT_DIR


class CreditCards:
    """Class responsible for credit cards module"""

    def __init__(self, base_url, credit_card_url):
        self.base_url = base_url
        self.credit_card_url = credit_card_url

    def send_one_credit_card(
        self,
        producer: KafkaProducer,
        credit_card_account_id: str,
        company_cnpj: str,
        client_id: str,
        name: str,
        product_type: str,
        product_additional_info: str,
        credit_card_network: str,
        network_additional_info: str,
        payment_method: str,
    ) -> None:
        """Send one credt card to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param credit_card_account_id: Account id of credit card
        :type credit_card_account_id: str
        :param company_cnpj: Institution CNPJ
        :type company_cnpj: str
        :param client_id: The id of client
        :type client_id: str
        :param name: The credit card name
        :type name: str
        :param product_type: Credit card type
        :type product_type: str
        :param product_additional_info: Credit card additional information
        :type product_additional_info: str
        :param credit_card_network: Flag name of credit card
        :type credit_card_network: str
        :param network_additional_info: Flag description of credit card
        :type network_additional_info: str
        :param payment_method: Payment methods of a credit card
        :type payment_method: str
        :return: None
        """
        data = {
            "creditCardAccountId": credit_card_account_id,
            "companyCnpj": company_cnpj,
            "clientId": client_id,
            "name": name,
            "productType": product_type,
            "productAdditionalInfo": product_additional_info,
            "creditCardNetwork": credit_card_network,
            "networkAdditionalInfo": network_additional_info,
            "paymentMethod": [json.loads(payment_method)],
        }

        future = producer.send("creditCardsObserver", json.dumps(data).encode("utf-8"))
        result = future.get(timeout=60)

        print(f"sent credit card {credit_card_account_id} to kafka")

    def send_credit_card(self, producer: KafkaProducer) -> None:
        """Read and send credit cards to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :return: None
        """
        with open(str(os.path.join(ROOT_DIR, "files", "creditcard.csv")), "r") as files:
            next(files)
            for file in files:
                credit_card_account_id = file.split(",")[0].strip("\n")
                company_cnpj = file.split(",")[1].strip("\n")
                client_id = file.split(",")[2].strip("\n")
                name = file.split(",")[3].strip("\n")
                product_type = file.split(",")[4].strip("\n")
                credit_card_network = file.split(",")[5].strip("\n")
                identification_number = file.split(",")[6].strip("\n")
                is_multiple_credit_card = file.split(",")[7].strip("\n")

                data = {
                    "creditCardAccountId": credit_card_account_id,
                    "companyCnpj": company_cnpj,
                    "clientId": client_id,
                    "name": name,
                    "productType": product_type,
                    "creditCardNetwork": credit_card_network,
                    "paymentMethod": [
                        {
                            "identificationNumber": identification_number,
                            "isMultipleCreditCard": is_multiple_credit_card,
                        }
                    ],
                }

                future = producer.send(
                    "creditCardsObserver", json.dumps(data).encode("utf-8")
                )
                result = future.get(timeout=60)

                print(f"sent credit card {credit_card_account_id} to kafka")

    def delete_one_credit_card(self, credit_card_account_id: str) -> None:
        """Delete one credit card from pluggy api

        :param credit_card_account_id: The ID of credit card account
        :type credit_card_account_id: str
        :return: None
        """
        if "," in credit_card_account_id:
            credit_card_account_id = credit_card_account_id.split(",")[0].strip("\n")

        response = requests.delete(
            self.base_url + self.credit_card_url + credit_card_account_id
        )

        print(
            f"The credit card {credit_card_account_id} was deleted with status code: {response.status_code}"
        )

    def delete_credit_card(self) -> None:
        """Delete credit cards from pluggy api

        :return: None
        """

        with open(str(os.path.join(ROOT_DIR, "files", "creditcard.csv")), "r") as files:
            next(files)
            with ThreadPoolExecutor(max_workers=24) as executor:
                executor.map(self.delete_one_credit_card, files)
