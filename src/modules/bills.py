mport os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
from kafka import KafkaProducer
from config.definitions import ROOT_DIR


requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class Bills:
    """Class responsible for bills methods"""

    def __init__(self, base_url, bills_url):
        self.base_url = base_url
        self.bill_url = bills_url

    def send_one_bill(
        self,
        producer: KafkaProducer,
        credit_card_account_id: str,
        bill_total_amount: float,
        bill_id: str,
        due_date: str,
        is_instalment: bool,
        bill_minimum_amount: float,
        bill_minimum_amount_currency: str,
        bill_total_amount_currency: str,
        finance_charges_type: str,
        finance_charges_additional_info: str,
        finance_charges_amount: float,
        finance_charges_currency: str,
        payments_value_type: str,
        payments_date: str,
        payments_mode: str,
        payments_amount: float,
        payments_currency: str,
    ) -> None:
        """Send one bill to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param credit_card_account_id: account id of the credit card
        :type credit_card_account_id: str
        :param bill_total_amount: Total value of the bill
        :type bill_total_amount: str
        :param bill_id: The bill ID
        :type bill_id: str
        :param due_date: Due date of the bill
        :type due_date: str
        :param is_instalment: If the bill is installment
        :type is_instalment: bool
        :param bill_minimum_amount: Minimum payment value of the bill
        :type: float
        :param bill_minimum_amount_currency: Currency type of the minimum payment bill value
        :type bill_minimum_amount_currency: str
        :param bill_total_amount_currency: Currency type of the total payment bill value
        :type bill_total_amount_currency: str
        :param finance_charges_type: Finance charges type of the bill
        :type finance_charges_type: str
        :param finance_charges_additional_info: Finance charges info of the bill
        :type finance_charges_additional_info: str
        :param finance_charges_amount: Finance charges amount
        :type finance_charges_amount: float
        :param finance_charges_currency: Finance charges currency
        :type finance_charges_currency: str
        :param payments_value_type: Payments value type
        :type payments_value_type: str
        :param payments_date: Date of payment
        :type payments_date: str
        :param payments_mode: The payment mode
        :type payments_mode: str
        :param payments_amount: The amount of payment
        :type payments_amount: float
        :param payments_currency: The currency of payment
        :type payments_currency: str
        """
        data = [
            {
                "creditCardAccountId": credit_card_account_id,
                "billTotalAmount": float(bill_total_amount),
                "billId": bill_id,
                "dueDate": due_date,
                "isInstalment": bool(is_instalment),
                "billMinimumAmount": float(bill_minimum_amount),
                "billMinimumAmountCurrency": bill_total_amount_currency,
                "billTotalAmountCurrency": bill_total_amount_currency,
                "financeCharges": [
                    {
                        "type": finance_charges_type,
                        "additionalInfo": finance_charges_additional_info,
                        "amount": float(finance_charges_amount),
                        "currency": finance_charges_currency,
                    }
                ],
                "payments": [
                    {
                        "valueType": payments_value_type,
                        "paymentDate": payments_date,
                        "paymentMode": payments_mode,
                        "amount": float(payments_amount),
                        "currency": payments_currency,
                    }
                ],
            }
        ]

        future = producer.send("billsObserver", json.dumps(data).encode("utf-8"))
        result = future.get(timeout=60)

        print(f"sent bill {bill_id} to kafka")

    def send_bills(self, producer: KafkaProducer) -> None:
        """Send bills to kafka

        :return None
        """
        print("Method not implemented yet")

    def delete_one_bill(bill_id: str) -> None:
        """Delete the bill from pluggy api

        :param bill_id: The ID of bill
        :type bill_id: str
        """
        if "," in bill_id:
            bill_id = bill_id.split(",")[0]

        payload = {"billid": bill_id, "isDeleteTransaction": True}
        response = requests.delete(self.base_url + self.bills_url, params=payload, verify=False)

        print(
            f"The bill {bill_id} was deleted with status code: {response.status_code}"
        )

    def delete_bills(self) -> None:
        """Delete bills on pluggy api

        :return: None
        """
        print("Method not implemented yet")
