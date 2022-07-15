import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from config.definitions import ROOT_DIR


class Transactions:
    """Class responsible for all transactions implementations"""

    def __init__(self, base_url, transaction_url):
        self.base_url = base_url
        self.transaction_url = transaction_url

    def delete_one_transaction(self, transaction_id: str) -> None:
        """Delete one transaction from pluggy api

        :param transaction_id: The ID of transaction
        :type transaction_id: str
        :return: None
        """
        if "," in transaction_id:
            transaction_id = transaction_id.split(",")[10]

        response = requests.delete(
            self.base_url + self.transaction_url + transaction_id
        )

        print(
            f"The transaction {transaction_id} was deleted with status code: {response.status_code}"
        )

    def delete_transactions(self) -> None:
        """Delete transactions on pluggy api

        :return: None
        """
        with open(
            str(os.path.join(ROOT_DIR, "files", "transaction_100.csv")), "r"
        ) as files:
            next(files)
            with ThreadPoolExecutor(max_workers=24) as executor:
                executor.map(self.delete_one_transaction, files)


class PrePaidTransactions(Transactions):
    """Class responsible for pre praid transactions methods"""

    def send_one_pre_paid_transaction(
        self,
        producer: KafkaProducer,
        transaction_id: str,
        transaction_date: str,
        amount: float,
        transaction_currency: str,
        credit_debit_type: str,
        transaction_name: str,
        category: str,
        account_id: str,
        request_id: int,
        partie_person_type: str,
        partie_cnpj_cpf: str,
        transaction_model_type: str,
        partie_compe_code: str,
        partie_branch_code: str,
        partie_number: str,
        partie_check_digit: str,
        completed_authorised_payment_type: str,
    ) -> None:
        """Send one pre paid transaction to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param transaction_id: The external id of the transaction
        :type transaction_id: str
        :param transaction_date: The registered date of the transaction
        :type transaction_date: str
        :param amount: The value of the transaction
        :type amount: float
        :param transaction_currency: The currency type of the transaction
        :type transaction_currency: str
        :param credit_debit_type: Credit or debit type
        :type credit_debit_type: str
        :param transaction_name: The name of transaction
        :type transaction_name: str
        :param category: The category type of the transaction
        :type category: str
        :param account_id: The account id
        :type account_id: str
        :param request_id: The request id of the received balance
        :type request_id: int
        :param partie_person_type: The person type involved in the transaction
        :type partie_person_type: str
        :param partie_cnpj_cpf: The CPF or CNPJ
        :type partie_cnpj_cpf: str
        :param transaction_model_type: The model type of the transaction
        :type transaction_model_type: str
        :param partie_compe_code: The bank number
        :type partie_compe_code: str
        :param partie_branch_code: The branch code
        :type partie_branch_code: str
        :param partie_number: The account number
        :type partie_number: str
        :param partie_check_digit: The check digit of the account
        :type partie_check_digit: str
        :param completed_authorised_payment_type: Indicator type of transaction
        :type completed_authorised_payment_type: str
        :return: None
        """
        data = {
            "transactionId": transaction_id,
            "transactionDate": transaction_date,
            "amount": amount,
            "transactionCurrency": transaction_currency,
            "creditDebitType": credit_debit_type,
            "transactionName": transaction_name,
            "category": category,
            "accountId": account_id,
            "requestId": request_id,
            "partiePersonType": partie_person_type,
            "partieCnpjCpf": partie_cnpj_cpf,
            "type": transaction_model_type,
            "partieCompeCode": partie_compe_code,
            "partieBranchCode": partie_branch_code,
            "partieNumber": partie_number,
            "partieCheckDigit": partie_check_digit,
            "completedAuthorisedPaymentType": completed_authorised_payment_type,
        }

        future = producer.send(
            "prePaidTransactionsObserver", json.dumps(data).encode("utf-8")
        )
        result = future.get(timeout=60)

        print(f"sent account {transaction_id} to kafka")

    def send_pre_paid_transactions(self, producer: KafkaProducer) -> None:
        """Read and send transactions to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :return: None
        """
        with open(
            str(os.path.join(ROOT_DIR, "files", "transaction_1000.csv")), "r"
        ) as files:
            next(files)
            for file in files:
                transaction_date = file.split(",")[0].strip("\n")
                transaction_name = file.split(",")[1].strip("\n")
                amount = file.split(",")[2].strip("\n")
                transaction_currency = file.split(",")[3].strip("\n")
                credit_debit_type = file.split(",")[4].strip("\n")
                category = file.split(",")[5].strip("\n")
                account_id = file.split(",")[6].strip("\n")
                request_id = file.split(",")[7].strip("\n")
                partie_person_type = file.split(",")[8].strip("\n")
                completed_authorised_payment_type = file.split(",")[9].strip("\n")
                transaction_id = file.split(",")[10]
                transaction_model_type = file.split(",")[11].strip("\n")

                data = {
                    "transactionId": transaction_id,
                    "transactionDate": transaction_date,
                    "amount": amount,
                    "transactionCurrency": transaction_currency,
                    "creditDebitType": credit_debit_type,
                    "transactionName": transaction_name,
                    "accountId": account_id,
                    "requestId": request_id,
                    "partiePersonType": partie_person_type,
                    "type": transaction_model_type,
                    "completedAuthorisedPaymentType": completed_authorised_payment_type,
                }

                future = producer.send(
                    "prePaidTransactionsObserver", json.dumps(data).encode("utf-8")
                )
                result = future.get(timeout=60)

                print(f"sent transaction {transaction_id} to kafka")


class PostPaidTransactions(Transactions):
    """Class responsible for post praid transactions methods"""

    def send_one_post_paid_transaction(
        self,
        producer: KafkaProducer,
        transaction_id: str,
        transaction_date: str,
        amount: float,
        transaction_currency: str,
        credit_debit_type: str,
        transaction_name: str,
        bill_id: str,
        charge_number: int,
        charge_identificador: str,
        payment_type: str,
        transaction_type: str,
        bill_post_date: str,
        category: str,
        account_id: str,
        payee_mcc: str,
        fee_type: str,
        fee_type_addicional_info: str,
        other_credit_type: str,
        transactional_additional_info: str,
        line_name: str,
        identification_number: str,
        other_credits_additional_info: str,
        brazilian_amount: float,
    ) -> None:
        """Send one post paid transaction to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param transaction_id: The external id of the transaction
        :type transaction_id: str
        :param transaction_date: The registered date of the transaction
        :type transaction_date: str
        :param amount: The value of the transaction
        :type amount: float
        :param transaction_currency: The currency type of the transaction
        :type transaction_currency: str
        :param credit_debit_type: Credit or debit type
        :type credit_debit_type: str
        :param transaction_name: The name of transaction
        :type transaction_name: str
        :param bill_id: The bill ID of the realized transaction
        :type bill_id: str
        :param charge_number: Installments number in the transaction
        :type charge_number: int
        :param charge_identificador: Free field for installment description
        :type charge_identificador: str
        :param payment_type: Payment type of credit card
        :type payment_type: str
        :param transaction_type: A credit card transaction type
        :type transaction_type: str
        :param bill_post_date: The insertion date of the transaction
        :type str
        :param category: The category type of the transaction
        :type category: str
        :param account_id: The account id
        :type account_id: str
        :param payee_mcc: The MCC code to classify the business
        :type: payee_mcc: str
        :param fee_type: The type of a credit card transaction fee
        :type fee_type: str
        :param fee_type_addicional_info: Free field for type additional info
        :type fee_type_addicional_info: str
        :param other_credit_type: The other credit types in the credit card
        :type other_credit_type: str
        :param transactional_additional_info: Free field for transactional additional info
        :type transactional_additional_info: str
        :param line_name: The credit line type of a transaction credit card
        :type line_name: str
        :param identification_number: The credit card number identifier
        :type identification_number: str
        :param other_credits_additional_info: Free field for others credit types additional info
        :type other_credits_additional_info: str
        :param brazilian_amount: The transaction value in brazilian currency
        :type brazilian_amount: float
        :return: None
        """
        data = {
            "transactionId": transaction_id,
            "transactionDate": transaction_date,
            "amount": float(amount),
            "transactionCurrency": transaction_currency,
            "creditDebitType": credit_debit_type,
            "transactionName": transaction_name,
            "billId": bill_id,
            "chargeNumber": int(charge_number),
            "chargeIdentificator": charge_identificador,
            "paymentType": payment_type,
            "transactionType": transaction_type,
            "billPostDate": bill_post_date,
            "category": category,
            "accountId": account_id,
            "payeeMCC": payee_mcc,
            "feeType": fee_type,
            "feeTypeAdditionalInfo": fee_type_addicional_info,
            "otherCreditType": other_credit_type,
            "transactionalAdditionalInfo": transactional_additional_info,
            "lineName": line_name,
            "identificationNumber": identification_number,
            "otherCreditsAdditionalInfo": other_credits_additional_info,
            "brazilianAmount": float(brazilian_amount),
        }

        future = producer.send(
            "postPaidTransactionsObserver", json.dumps(data).encode("utf-8")
        )
        result = future.get(timeout=60)

        print(f"sent account {transaction_id} to kafka")

    def send_post_paid_transactions(self, producer: KafkaProducer):
        """Send post paid transactions to kafka"""
        print("Method not implemented yet")
