import argparse
import timeit
from kafka import BrokerConnection, KafkaProducer
from modules.accounts import Accounts
from modules.balances import Balances
from modules.bills import Bills
from modules.clients import Clients
from modules.credit_card import CreditCards
from modules.limits import Limits
from modules.transactions import Transactions, PrePaidTransactions, PostPaidTransactions
from routes import URL


KAFKA_PRODUCER = KafkaProducer(bootstrap_servers=URL["url_kafka"])


def main() -> None:
    """Responsible for run the methods send_client and delete_clients

    :return: None
    """

    parser = argparse.ArgumentParser(
        description="application to interact with kafka and pluggy api"
    )

    clients = parser.add_argument_group("clients", "clients module")

    clients.add_argument(
        "-soc",
        "--send_one_client",
        action="store_true",
        help="send one client to kafka",
    )

    clients.add_argument(
        "-doc",
        "--delete_one_client",
        action="store_true",
        help="delete one client from pluggy api",
    )

    clients.add_argument(
        "-sc",
        "--send_clients",
        action="store_true",
        help="read data clients and send to kafka",
    )

    clients.add_argument(
        "-dc",
        "--delete_clients",
        action="store_true",
        help="delete clients from pluggy api",
    )

    clients_parameters = parser.add_argument_group(
        "clients_parameters", "clients parameters"
    )

    clients_parameters.add_argument(
        "-ci",
        "--client_id",
        metavar="client_id",
        type=str,
        help="The client id",
    )

    clients_parameters.add_argument(
        "-cn",
        "--cpf_number",
        metavar="cpf_number",
        type=str,
        help="The client cpf number",
    )

    accounts = parser.add_argument_group("accounts", "accounts module")

    accounts.add_argument(
        "-soa",
        "--send_one_account",
        action="store_true",
        help="send one account to kafka",
    )

    accounts.add_argument(
        "-doa",
        "--delete_one_account",
        action="store_true",
        help="delete one account from pluggy api",
    )

    accounts.add_argument(
        "-sa",
        "--send_accounts",
        action="store_true",
        help="read data accounts and send to kafka",
    )

    accounts.add_argument(
        "-da",
        "--delete_accounts",
        action="store_true",
        help="delete accounts from pluggy api",
    )

    accounts_parameters = parser.add_argument_group(
        "accounts_parameters", "accounts parameters"
    )

    accounts_parameters.add_argument(
        "-cc",
        "--compe_code",
        metavar="compe_code",
        type=str,
        help="The id of the account owner",
    )

    accounts_parameters.add_argument(
        "-bc",
        "--branch_code",
        metavar="branch_code",
        type=str,
        help="The agency code",
    )

    accounts_parameters.add_argument(
        "-n",
        "--number",
        metavar="number",
        type=str,
        help="The account number",
    )

    accounts_parameters.add_argument(
        "-at",
        "--account_type",
        metavar="account_type",
        type=str,
        help="The type of account",
    )

    accounts_parameters.add_argument(
        "-ast",
        "--account_subtype",
        metavar="account_subtype",
        type=str,
        help="The subtype of account",
    )

    accounts_parameters.add_argument(
        "-d",
        "--description",
        metavar="description",
        type=str,
        help="The account description",
    )

    accounts_parameters.add_argument(
        "-cd",
        "--check_digit",
        metavar="check_digit",
        type=str,
        help="The account digit",
    )

    accounts_parameters.add_argument(
        "-c",
        "--currency",
        metavar="currency",
        type=str,
        help="The coin type",
    )

    accounts_parameters.add_argument(
        "-ai",
        "--account_id",
        metavar="account_id",
        type=str,
        help="The account identifier of deposit, savings or payment",
    )

    pre_paid_transactions = parser.add_argument_group(
        "pre_paid_transactions", "pre paid transactions module"
    )

    pre_paid_transactions.add_argument(
        "-soppt_1",
        "--send_one_pre_paid_transaction",
        action="store_true",
        help="send one pre paid transaction to kafka",
    )

    pre_paid_transactions.add_argument(
        "-dot",
        "--delete_one_transaction",
        action="store_true",
        help="delete one transaction from pluggy api",
    )

    pre_paid_transactions.add_argument(
        "-sppt_1",
        "--send_pre_paid_transactions",
        action="store_true",
        help="send pre paid transactions to kafka",
    )

    pre_paid_transactions.add_argument(
        "-dt",
        "--delete_transactions",
        action="store_true",
        help="delete one transaction from pluggy api",
    )

    pre_paid_transactions_parameters = parser.add_argument_group(
        "pre_paid_transactions_parameters", "pre paid transactions parameters"
    )

    pre_paid_transactions_parameters.add_argument(
        "-ti",
        "--transaction_id",
        metavar="transaction_id",
        type=str,
        help="The external id of the transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-td",
        "--transaction_date",
        metavar="transaction_date",
        type=str,
        help="The registered date of the transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-a",
        "--amount",
        metavar="amount",
        type=float,
        help="The value of the transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-tc",
        "--transaction_currency",
        metavar="transaction_currency",
        type=str,
        help="The currency type of the transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-cdt",
        "--credit_debit_type",
        metavar="credit_debit_type",
        type=str,
        help="Credit or debit type",
    )

    pre_paid_transactions_parameters.add_argument(
        "-tn",
        "--transaction_name",
        metavar="transaction_name",
        type=str,
        help="The name of transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-ct",
        "--category",
        metavar="category",
        type=str,
        help="The category type of the transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-ri",
        "--request_id",
        metavar="request_id",
        type=int,
        help="The request id of the received balance",
    )

    pre_paid_transactions_parameters.add_argument(
        "-ppt",
        "--partie_person_type",
        metavar="partie_person_type",
        type=str,
        help="The person type involved in the transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-pcc",
        "--partie_cnpj_cpf",
        metavar="partie_cnpj_cpf",
        type=str,
        help="The CPF or CNPJ",
    )

    pre_paid_transactions_parameters.add_argument(
        "-tmt",
        "--transaction_model_type",
        metavar="transaction_model_type",
        type=str,
        help="The model type of the transaction",
    )

    pre_paid_transactions_parameters.add_argument(
        "-pccd",
        "--partie_compe_code",
        metavar="partie_compe_code",
        type=str,
        help="The bank number",
    )

    pre_paid_transactions_parameters.add_argument(
        "-pbc",
        "--partie_branch_code",
        metavar="partie_branch_code",
        type=str,
        help="The branch code",
    )

    pre_paid_transactions_parameters.add_argument(
        "-pn",
        "--partie_number",
        metavar="partie_number",
        type=str,
        help="The account number",
    )

    pre_paid_transactions_parameters.add_argument(
        "-pcd",
        "--partie_check_digit",
        metavar="partie_check_digit",
        type=str,
        help="The check digit of the account",
    )

    pre_paid_transactions_parameters.add_argument(
        "-capt",
        "--completed_authorised_payment_type",
        metavar="completed_authorised_payment_type",
        type=str,
        help="Indicator type of transaction",
    )

    post_paid_transactions = parser.add_argument_group(
        "post_paid_transactions_parameters", "post paid transactions"
    )

    post_paid_transactions.add_argument(
        "-soppt_2",
        "--send_one_post_paid_transaction",
        action="store_true",
        help="Send one post paid transaction",
    )

    post_paid_transactions.add_argument(
        "-sppt_2",
        "--send_post_paid_transactions",
        action="store_true",
        help="Send post paid transactions",
    )

    post_paid_transactions_parameters = parser.add_argument_group(
        "post_paid_transactions_parameters", "post paid transactions parameters"
    )

    post_paid_transactions_parameters.add_argument(
        "-cn_1",
        "--charge_number",
        metavar="charge_number",
        help="Installments number in the transaction",
    )

    post_paid_transactions_parameters.add_argument(
        "-ci_1",
        "--charge_identificador",
        metavar="charge_identificador",
        help="Free field for installment description",
    )

    post_paid_transactions_parameters.add_argument(
        "-pt_1",
        "--payment_type",
        metavar="payment_type",
        help="Payment type of credit card",
    )

    post_paid_transactions_parameters.add_argument(
        "-tt",
        "--transaction_type",
        metavar="transaction_type",
        help="A credit card transaction type",
    )

    post_paid_transactions_parameters.add_argument(
        "-bpd",
        "--bill_post_date",
        metavar="bill_post_date",
        help="The insertion date of the transaction",
    )

    post_paid_transactions_parameters.add_argument(
        "-pmmc",
        "--payee_mcc",
        metavar="payee_mcc",
        help="The MCC code to classify the business",
    )

    post_paid_transactions_parameters.add_argument(
        "-ft",
        "--fee_type",
        metavar="fee_type",
        help="The type of a credit card transaction fee",
    )

    post_paid_transactions_parameters.add_argument(
        "-ftai",
        "--fee_type_additional_info",
        metavar="fee_type_additional_info",
        help="Free field for type additional info",
    )

    post_paid_transactions_parameters.add_argument(
        "-oct",
        "--other_credit_type",
        metavar="other_credit_type",
        help="The other credit types in the credit card",
    )

    post_paid_transactions_parameters.add_argument(
        "-tai",
        "--transactional_additional_info",
        metavar="transactional_additional_info",
        help="Free field for transactional additional info",
    )

    post_paid_transactions_parameters.add_argument(
        "-ocai",
        "--other_credits_additional_info",
        metavar="other_credits_additional_info",
        help="Free field for others credit types additional info",
    )

    post_paid_transactions_parameters.add_argument(
        "-ba",
        "--brazilian_amount",
        metavar="brazilian_amount",
        help="The transaction value in brazilian currency",
    )

    credit_cards = parser.add_argument_group("credit_cards", "credit cards")

    credit_cards.add_argument(
        "-socc",
        "--send_one_credit_card",
        action="store_true",
        help="send one credit card to kafka",
    )

    credit_cards.add_argument(
        "-docc",
        "--delete_one_credit_card",
        action="store_true",
        help="delete one credit card from pluggy api",
    )

    credit_cards.add_argument(
        "-scc",
        "--send_credit_card",
        action="store_true",
        help="send credit card to kafka",
    )

    credit_cards.add_argument(
        "-dcc",
        "--delete_credit_card",
        action="store_true",
        help="delete credit card from pluggy api",
    )

    credit_cards_parameters = parser.add_argument_group(
        "credit_cards_parameters", "credit cards parameters"
    )

    credit_cards_parameters.add_argument(
        "-ccai",
        "--credit_card_account_id",
        metavar="credit_card_account_id",
        type=str,
        help="Account id of credit card",
    )

    credit_cards_parameters.add_argument(
        "-cc_1",
        "--company_cnpj",
        metavar="company_cnpj",
        type=str,
        help="Institution CNPJ",
    )

    credit_cards_parameters.add_argument(
        "-nm",
        "--name",
        metavar="name",
        type=str,
        help="Credit card name",
    )

    credit_cards_parameters.add_argument(
        "-pt",
        "--product_type",
        metavar="product_type",
        type=str,
        help="Credit card type",
    )

    credit_cards_parameters.add_argument(
        "-pai",
        "--product_additional_info",
        metavar="product_additional_info",
        type=str,
        help="Credit card additional information",
    )

    credit_cards_parameters.add_argument(
        "-ccn",
        "--credit_card_network",
        metavar="credit_card_network",
        type=str,
        help="Flag name of credit card",
    )

    credit_cards_parameters.add_argument(
        "-nai",
        "--network_additional_info",
        metavar="network_additional_info",
        type=str,
        help="Flag description of credit card",
    )

    credit_cards_parameters.add_argument(
        "-pm",
        "--payment_method",
        metavar="payment_method",
        type=str,
        help="Payment methods of a credit card",
    )

    bills = parser.add_argument_group("bills", "The bills")

    bills.add_argument(
        "-sob",
        "--send_one_bill",
        action="store_true",
        help="Send one bill to kafka",
    )

    bills.add_argument(
        "-sb",
        "--send_bills",
        action="store_true",
        help="Send bills to kafka",
    )

    bills.add_argument(
        "-dob",
        "--delete_one_bill",
        action="store_true",
        help="Delete one bill from pluggy api",
    )

    bills.add_argument(
        "-db",
        "--delete_bills",
        action="store_true",
        help="Delete bills from pluggy api",
    )

    bills_parameters = parser.add_argument_group(
        "bills_parameters", "The bills parameters"
    )

    bills_parameters.add_argument(
        "-bta",
        "--bill_total_amount",
        metavar="bill_total_amount",
        help="Total value of the bill",
    )

    bills_parameters.add_argument(
        "-bi",
        "--bill_id",
        metavar="bill_id",
        help="The bill ID",
    )

    bills_parameters.add_argument(
        "-dd",
        "--due_date",
        metavar="due_date",
        help="Due date of the bill",
    )

    bills_parameters.add_argument(
        "-ii",
        "--is_instalment",
        metavar="If the bill is installment",
    )

    bills_parameters.add_argument(
        "-bma",
        "--bill_minimum_amount",
        metavar="bill_minimum_amount",
        help="Minimum payment value of the bill",
    )

    bills_parameters.add_argument(
        "-bmac",
        "--bill_minimum_amount_currency",
        metavar="bill_minimum_amount_currency",
        help="Currency type of the minimum payment bill value",
    )

    bills_parameters.add_argument(
        "-btac",
        "--bill_total_amount_currency",
        metavar="bill_total_amount_currency",
        help="Currency type of the total payment bill value",
    )

    bills_parameters.add_argument(
        "-fct",
        "--finance_charges_type",
        metavar="finance_charges_type",
        help="Finance charges type of the bill",
    )

    bills_parameters.add_argument(
        "-fcai",
        "--finance_charges_additional_info",
        metavar="finance_charges_additional_info",
        help="Finance charges info of the bill",
    )

    bills_parameters.add_argument(
        "-fca",
        "--finance_charges_amount",
        metavar="finance_charges_amount",
        help="Finance charges amount",
    )

    bills_parameters.add_argument(
        "-fcc",
        "--finance_charges_currency",
        metavar="finance_charges_currency",
        help="Finance charges currency",
    )

    bills_parameters.add_argument(
        "-pvt",
        "--payments_value_type",
        metavar="payments_value_type",
        help="Payments value type",
    )

    bills_parameters.add_argument(
        "-pd",
        "--payments_date",
        metavar="payments_date",
        help="Date of payment",
    )

    bills_parameters.add_argument(
        "-pm_1",
        "--payments_mode",
        metavar="payments_mode",
        help="The payment mode",
    )

    bills_parameters.add_argument(
        "-pa",
        "--payments_amount",
        metavar="payments_amount",
        help="The amount of payment",
    )

    bills_parameters.add_argument(
        "-pc",
        "--payments_currency",
        metavar="payments_currency",
        help="The currency of payment",
    )

    credit_card_limits = parser.add_argument_group(
        "credit card limits", "The credit card limits"
    )

    credit_card_limits.add_argument(
        "-soccl",
        "--send_one_credit_card_limit",
        action="store_true",
        help="send one credit card limit to kafka",
    )

    credit_card_limits.add_argument(
        "-sccl",
        "--send_credit_card_limits",
        action="store_true",
        help="Send credit card limits to kafka",
    )

    credit_card_limits_parameters = parser.add_argument_group(
        "credit card limits parameters",
        "The parameters of credit card limits",
    )

    credit_cards_parameters.add_argument(
        "-aa",
        "--available_amount",
        metavar="available_amount",
        help="Available limits values",
    )

    credit_cards_parameters.add_argument(
        "-ct_1",
        "--consolidation_type",
        metavar="consolidation_type",
        help="Value limit indication",
    )

    credit_cards_parameters.add_argument(
        "-cllt",
        "--credit_line_limit_type",
        metavar="credit_line_limit_type",
        help="Limit indication type",
    )

    credit_cards_parameters.add_argument(
        "-ilf",
        "--is_limit_flexible",
        metavar="is_limit_flexible",
        help="Indicate if limit is flexible",
    )

    credit_cards_parameters.add_argument(
        "-la",
        "--limit_amount",
        metavar="limit_amount",
        help="The amount of limit",
    )

    credit_cards_parameters.add_argument(
        "-lac",
        "--limit_amount_currency",
        metavar="limit_amount_currency",
        help="The currency of amount limit",
    )

    credit_cards_parameters.add_argument(
        "-ln",
        "--line_name",
        metavar="line_name",
        help="The addicional info of the line name",
    )

    credit_cards_parameters.add_argument(
        "-lnai",
        "--line_name_additional_info",
        metavar="line_name_additional_info",
        help="The addicional info of the line name",
    )

    credit_cards_parameters.add_argument(
        "-ua",
        "--used_amount",
        metavar="used_amount",
        help="Limit amount used",
    )

    credit_cards_parameters.add_argument(
        "-in",
        "--identification_number",
        metavar="identification_number",
        help="The identification number",
    )

    credit_cards_parameters.add_argument(
        "-uac",
        "--used_amount_currency",
        metavar="used_amount_currency",
        help="The available amount currency",
    )

    credit_cards_parameters.add_argument(
        "-aac",
        "--available_amount_currency",
        metavar="available_amount_currency",
        help="The available amount currency",
    )

    balances = parser.add_argument_group("balances", "The balances")

    balances.add_argument(
        "-sob_1",
        "--send_one_balance",
        action="store_true",
        help="Send one balance to kafka",
    )

    balances.add_argument(
        "-sb_1",
        "--send_balances",
        action="store_true",
        help="Send balances to kafka",
    )

    balances_parameters = parser.add_argument_group(
        "balances_parameters", "The balances parameters"
    )

    balances_parameters.add_argument(
        "-rd",
        "--request_date",
        metavar="request_date",
        help="The request date",
    )

    balances_parameters.add_argument(
        "-ba_1",
        "--blocked_amount",
        metavar="blocked_amount",
        help="The blocked amount",
    )

    balances_parameters.add_argument(
        "-bac",
        "--blocked_amount_currency",
        metavar="blocked_amount_currency",
        help="The blocked amount currency",
    )

    balances_parameters.add_argument(
        "-ai_1",
        "--automatically_invested_amount",
        metavar="automatically_invested_amount",
        help="The automatically invested amount balance",
    )

    balances_parameters.add_argument(
        "-aic",
        "--automatically_invested_amount_currency",
        metavar="automatically_invested_amount_currency",
        help="The automatically invested amount currency of balance",
    )

    args = parser.parse_args()

    if args.send_one_client:
        if args.client_id and args.cpf_number:
            client = Clients(URL["url_base"], URL["url_clients_delete"])
            client.send_one_client(KAFKA_PRODUCER, args.client_id, args.cpf_number)
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--client_id or -ci \n"
                f"--cpf_number or -cn"
            )
    elif args.delete_one_client:
        if args.client_id:
            client = Clients(URL["url_base"], URL["url_clients_delete"])
            client.delete_one_client(args.client_id)
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--client_id or -ci"
            )
    elif args.send_clients:
        client = Clients(URL["url_base"], URL["url_clients_delete"])
        start = timeit.default_timer()
        client.send_clients(KAFKA_PRODUCER)
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")

    elif args.delete_clients:
        client = Clients(URL["url_base"], URL["url_clients_delete"])
        start = timeit.default_timer()
        client.delete_clients()
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.send_one_account:
        if (
            args.compe_code
            and args.client_id
            and args.branch_code
            and args.number
            and args.account_type
            and args.account_subtype
            and args.description
            and args.check_digit
            and args.currency
            and args.account_id
        ):
            account = Accounts(URL["url_base"], URL["url_accounts_delete"])
            account.send_one_account(
                KAFKA_PRODUCER,
                args.compe_code,
                args.client_id,
                args.branch_code,
                args.number,
                args.account_type,
                args.account_subtype,
                args.description,
                args.check_digit,
                args.currency,
                args.account_id,
            )
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--compe_code or -cc \n"
                f"--client_id or -ci \n"
                f"--branch_code or -bc \n"
                f"--number or -n \n"
                f"--account_type or -at \n"
                f"--account_subtype or -ast \n"
                f"--description or -d \n"
                f"--check_digit or -cd \n"
                f"--currency or -c \n"
                f"--account_id or -ai"
            )
    elif args.delete_one_account:
        if args.account_id:
            account = Accounts(URL["url_base"], URL["url_accounts_delete"])
            account.delete_one_account(args.account_id)
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--account_id or -ai"
            )
    elif args.send_accounts:
        account = Accounts(URL["url_base"], URL["url_accounts_delete"])
        start = timeit.default_timer()
        account.send_accounts(KAFKA_PRODUCER)
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.delete_accounts:
        account = Accounts(URL["url_base"], URL["url_accounts_delete"])
        start = timeit.default_timer()
        account.delete_accounts()
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.send_one_pre_paid_transaction:
        if (
            args.transaction_id
            and args.transaction_date
            and args.amount
            and args.transaction_currency
            and args.credit_debit_type
            and args.transaction_name
            and args.category
            and args.account_id
            and args.request_id
            and args.partie_person_type
            and args.partie_cnpj_cpf
            and args.transaction_model_type
            and args.partie_compe_code
            and args.partie_branch_code
            and args.partie_number
            and args.partie_check_digit
            and args.completed_authorised_payment_type
        ):
            pre_paid_transactions = PrePaidTransactions(
                URL["url_base"], URL["url_transactions_delete"]
            )
            pre_paid_transactions.send_one_pre_paid_transaction(
                KAFKA_PRODUCER,
                args.transaction_id,
                args.transaction_date,
                args.amount,
                args.transaction_currency,
                args.credit_debit_type,
                args.transaction_name,
                args.category,
                args.account_id,
                args.request_id,
                args.partie_person_type,
                args.partie_cnpj_cpf,
                args.transaction_model_type,
                args.partie_compe_code,
                args.partie_branch_code,
                args.partie_number,
                args.partie_check_digit,
                args.completed_authorised_payment_type,
            )
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--transaction_id or -ti \n"
                f"--transaction_date or -td \n"
                f"--amount or -a \n"
                f"--transaction_currency or -tc"
                f"--credit_debit_type or -cdt \n"
                f"--transaction_name or -tn \n"
                f"--category or -ct \n"
                f"--account_id or -ai"
                f"--request_id or -ri \n"
                f"--partie_cnpj_cpf or -pcc \n"
                f"--transaction_model_type or -tmt \n"
                f"--partie_compe_code or -pccd \n"
                f"--partie_branch_code or -pbc \n"
                f"--partie_number or -pn"
                f"--partie_check_digit or -pcd \n"
                f"--completed_authorised_payment_type or -capt"
            )
    elif args.send_one_post_paid_transaction:
        if (
            args.transaction_id
            and args.transaction_date
            and args.amount
            and args.transaction_currency
            and args.credit_debit_type
            and args.transaction_name
            and args.bill_id
            and args.charge_number
            and args.charge_identificador
            and args.payment_type
            and args.transaction_type
            and args.bill_post_date
            and args.category
            and args.account_id
            and args.payee_mcc
            and args.fee_type
            and args.fee_type_additional_info
            and args.other_credit_type
            and args.transactional_additional_info
            and args.line_name
            and args.identification_number
            and args.other_credits_additional_info
            and args.brazilian_amount
        ):
            post_paid_transaction = PostPaidTransactions(
                URL["url_base"], URL["url_transactions_delete"]
            )
            post_paid_transaction.send_one_post_paid_transaction(
                KAFKA_PRODUCER,
                args.transaction_id,
                args.transaction_date,
                args.amount,
                args.transaction_currency,
                args.credit_debit_type,
                args.transaction_name,
                args.bill_id,
                args.charge_number,
                args.charge_identificador,
                args.payment_type,
                args.transaction_type,
                args.bill_post_date,
                args.category,
                args.account_id,
                args.payee_mcc,
                args.fee_type,
                args.fee_type_additional_info,
                args.other_credit_type,
                args.transactional_additional_info,
                args.line_name,
                args.identification_number,
                args.other_credits_additional_info,
                args.brazilian_amount,
            )
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--transaction_id or -ti \n"
                f"--transaction_date or -td \n"
                f"--amount or -a \n"
                f"--transaction_currency or -tc"
                f"--credit_debit_type or -cdt \n"
                f"--transaction_name or -tn \n"
                f"--bill_id or -bi \n"
                f"--charge_number or -cn_1 \n"
                f"--charge_identificador or -ci_1 \n"
                f"--payment_type or -pt_1 \n"
                f"--transaction_type or -tt \n"
                f"--bill_post_date or -bpd \n"
                f"--category or -c \n"
                f"--account_id or -ai \n"
                f"--payee_mcc or -pmmc \n"
                f"--fee_type or -ft \n"
                f"--fee_type_additional_info or -ftai \n"
                f"--other_credit_type or -oct \n"
                f"--transactional_additional_info or -tai \n"
                f"--line_name or -ln \n"
                f"--identification_number or -in \n"
                f"--other_credits_additional_info or -ocai \n"
                f"--brazilian_amount or -ba \n"
            )
    elif args.delete_one_transaction:
        if args.transaction_id:
            transactions = Transactions(URL["url_base"], URL["url_transactions_delete"])
            transactions.delete_one_transaction(args.transaction_id)
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--transaction_id or -ti"
            )
    elif args.send_pre_paid_transactions:
        pre_paid_transactions = PrePaidTransactions(
            URL["url_base"], URL["url_transactions_delete"]
        )
        start = timeit.default_timer()
        pre_paid_transactions.send_pre_paid_transactions(KAFKA_PRODUCER)
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.delete_transactions:
        transactions = Transactions(URL["url_base"], URL["url_transactions_delete"])
        start = timeit.default_timer()
        transactions.delete_transactions()
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.send_one_credit_card:
        if (
            args.credit_card_account_id
            and args.company_cnpj
            and args.client_id
            and args.name
            and args.product_type
            and args.product_additional_info
            and args.credit_card_network
            and args.network_additional_info
            and args.payment_method
        ):
            credit_card = CreditCards(URL["url_base"], URL["url_credit_cards_delete"])
            credit_card.send_one_credit_card(
                KAFKA_PRODUCER,
                args.credit_card_account_id,
                args.company_cnpj,
                args.client_id,
                args.name,
                args.product_type,
                args.product_additional_info,
                args.credit_card_network,
                args.network_additional_info,
                args.payment_method,
            )
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--credit_card_account_id or -ccai \n"
                f"--company_cnpj or -cc_1 \n"
                f"--client_id or -ci \n"
                f"--name or -nm \n"
                f"--product_type or -pt \n"
                f"--product_additional_info or -pai \n"
                f"--credit_card_network or -ccn \n"
                f"--network_additional_info or -nai \n"
                f"--payment_method or -pm \n"
            )
    elif args.delete_one_credit_card:
        if args.account_id:
            credit_card = CreditCards(URL["url_base"], URL["url_credit_cards_delete"])
            credit_card.delete_one_credit_card(args.account_id)
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--account_id or -ai"
            )
    elif args.send_credit_card:
        credit_card = CreditCards(URL["url_base"], URL["url_credit_cards_delete"])
        start = timeit.default_timer()
        credit_card.send_credit_card(KAFKA_PRODUCER)
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.delete_credit_card:
        credit_card = CreditCards(URL["url_base"], URL["url_credit_cards_delete"])
        start = timeit.default_timer()
        credit_card.delete_credit_card()
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")

    elif args.send_one_bill:
        if (
            args.credit_card_account_id
            and args.bill_total_amount
            and args.bill_id
            and args.due_date
            and args.is_instalment
            and args.bill_minimum_amount
            and args.bill_minimum_amount_currency
            and args.bill_total_amount_currency
            and args.finance_charges_type
            and args.finance_charges_additional_info
            and args.finance_charges_amount
            and args.finance_charges_currency
            and args.payments_value_type
            and args.payments_date
            and args.payments_mode
            and args.payments_amount
            and args.payments_currency
        ):
            bill = Bills(URL["url_base"], URL["url_bills_delete"])
            bill.send_one_bill(
                KAFKA_PRODUCER,
                args.credit_card_account_id,
                args.bill_total_amount,
                args.bill_id,
                args.due_date,
                args.is_instalment,
                args.bill_minimum_amount,
                args.bill_minimum_amount_currency,
                args.bill_total_amount_currency,
                args.finance_charges_type,
                args.finance_charges_additional_info,
                args.finance_charges_amount,
                args.finance_charges_currency,
                args.payments_value_type,
                args.payments_date,
                args.payments_mode,
                args.payments_amount,
                args.payments_currency,
            )
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--credit_card_account_id or -ccai \n"
                f"--bill_total_amount or -bta \n"
                f"--bill_id or -bi \n"
                f"--due_date or -dd \n"
                f"--is_instalment or -ii \n"
                f"--bill_minimum_amount or -bma \n"
                f"--bill_minimum_amount_currency or -bmac \n"
                f"--bill_total_amount_currency or -btac \n"
                f"--finance_charges or -fc \n"
                f"--payments or -p"
            )
    elif args.delete_one_bill:
        if args.bill_id:
            bill = Bills(URL["url_base"], URL["url_bills_delete"])
            bill.delete_one_bill(args.bill_id)
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--bill_id or -bi \n"
            )
    elif args.send_bills:
        bill = Bills(URL["url_base"], URL["url_bills_delete"])
        start = timeit.default_timer()
        bill.send_bills(KAFKA_PRODUCER)
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.delete_bills:
        bill = Bills(URL["url_base"], URL["url_bills_delete"])
        start = timeit.default_timer()
        bill.delete_bills()
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")
    elif args.send_one_credit_card_limit:
        if (
            args.credit_card_account_id
            and args.available_amount
            and args.consolidation_type
            and args.credit_line_limit_type
            and args.is_limit_flexible
            and args.limit_amount
            and args.limit_amount_currency
            and args.line_name
            and args.line_name_additional_info
            and args.used_amount
            and args.identification_number
            and args.used_amount_currency
            and args.available_amount_currency
        ):
            limit = Limits()
            limit.send_one_credit_card_limit(
                KAFKA_PRODUCER,
                args.credit_card_account_id,
                args.available_amount,
                args.consolidation_type,
                args.credit_line_limit_type,
                args.is_limit_flexible,
                args.limit_amount,
                args.limit_amount_currency,
                args.line_name,
                args.line_name_additional_info,
                args.used_amount,
                args.identification_number,
                args.used_amount_currency,
                args.available_amount_currency,
            )
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--credit_card_account_id or -ccai \n"
                f"--available_amount or -aa \n"
                f"--consolidation_type or -ct_1 \n"
                f"--credit_line_limit_type or -cllt \n"
                f"--is_limit_flexible or -ilf \n"
                f"--limit_amount or -la \n"
                f"--limit_amount_currency or -lac \n"
                f"--line_name or -ln \n"
                f"--line_name_additional_info or -lnai \n"
                f"--used_amount or -ua \n"
                f"--identification_number or -in \n"
                f"--used_amount_currency or -uac \n"
                f"--available_amount_currency or -aac \n"
            )
    elif args.send_credit_card_limits:
        limit = Limits()
        start = timeit.default_timer()
        limit.send_credit_card_limits(KAFKA_PRODUCER)
        stop = timeit.default_timer()
        print(f"time: {round(stop - start, 3)}s")

    elif args.send_one_balance:
        if (
            args.account_id
            and args.client_id
            and args.request_id
            and args.request_date
            and args.available_amount
            and args.available_amount_currency
            and args.blocked_amount
            and args.blocked_amount_currency
            and args.automatically_invested_amount
            and args.automatically_invested_amount_currency
        ):
            balance = Balances()
            balance.send_one_balance(
                KAFKA_PRODUCER,
                args.account_id,
                args.client_id,
                args.request_id,
                args.request_date,
                args.available_amount,
                args.available_amount_currency,
                args.blocked_amount,
                args.blocked_amount_currency,
                args.automatically_invested_amount,
                args.automatically_invested_amount_currency,
            )
        else:
            print(
                f"verify if you have passed all parameters needed: \n"
                f"--account_id or -ai \n"
                f"--client_id or -ci \n"
                f"--request_id or -ri \n"
                f"--request_date or -rd \n"
                f"--available_amount or -aa \n"
                f"--available_amount_currency or -aac \n"
                f"--blocked_amount or -ba_1 \n"
                f"--blocked_amount_currency or -bac \n"
                f"--automatically_invested_amount or -aia \n"
                f"--automatically_invested_amount_currency or -aiac \n"
            )
    elif args.send_balances:
        balance = Balances()
        balance.send_balances(KAFKA_PRODUCER)

    else:
        print(
            f"choose a valid option: \n"
            f"--send_one_client or -soc \n"
            f"--delete_one_client or -doc \n"
            f"--send_clients or -sc \n"
            f"--delete_clients or -dc \n"
            f"--send_one_account or -soa \n"
            f"--delete_one_account or -doa \n"
            f"--send_accounts or -sa \n"
            f"--delete_accounts or -da \n"
            f"--send_one_pre_paid_transaction or -soppt_1 \n"
            f"--delete_one_transaction or -dot \n"
            f"--send_pre_paid_transactions or -sppt_1 \n"
            f"--delete_transactions or -dt \n"
            f"--send_one_credit_card or -socc \n"
            f"--delete_one_credit_card or -docc \n"
            f"--send_one_bill or -sob"
            f"--delete_one_bill or -dob \n"
            f"--send_bills or -sb \n"
            f"--delete_bills or -db \n"
            f"--send_one_credit_card_limit \n"
            f"--send_credit_card_limits \n"
            f"--send_one_balance or -sob_1 \n"
            f"--send_balances or -sb_1 \n\n"
            f"examples: \n"
            f"python3 src/filename.py --send_one_client --client_id test123 --cpf_number 12345678900 \n"
            f"python3 src/filename.py --delete_one_client --client_id test123 \n"
            f"python3 src/filename.py --send_clients \n"
            f"python3 src/filename.py --delete_clients \n"
            f"python3 src/filename.py --send_one_account --compe_code 001 --client_id account_test_1"
            f"--branch_code 0001 --number 00000 --check_digit 0 --account_type CONTA_DEPOSITO_A_VISTA "
            f'--account_subtype INDIVIDUAL --description "the test request" --currency BRL --account_id 0 \n'
            f"python3 src/filename.py --delete_one_account --account_id test123 \n"
            f"python3 src/filename.py --send_accounts \n"
            f"python3 src/filename.py --delete_accounts \n"
            f"python3 src/filename.py --send_one_pre_paid_transaction --transaction_id 1 "
            f'--transaction_date "2022-01-01 00:00:00.000" --amount 1.0 --transaction_currency BRL '
            f"--credit_debit_type CREDITO --transaction_name transaction --category uber "
            f"--account_id 1 --request_id 1 --partie_person_type PESSOA_NATURAL "
            f"--partie_cnpj_cpf 00000000000 --transaction_model_type PIX --partie_compe_code 001 "
            f"--partie_branch_code 11245 --partie_number 158964 --partie_check_digit 1 "
            f"--completed_authorised_payment_type TRANSACAO_EFETIVADA \n"
            f"python3 src/filename.py --delete_one_transaction --transaction_id 1 \n"
            f"python3 src/filename.py --send_one_credit_card --credit_card_account_id 321 --client_id client123 --name test "
            f'--product_type virtual --product_additional_info test --credit_card_network "master card" --company_cnpj 001 '
            f"--payment_method "
            + '\'{"identificationNumber": "0084", "isMultipleCreditCard": true, "active": true}\''
            + "\n"
            f"python3 src/filename.py --delete_one_credit_card --account_id 1 \n"
            f"python3 src/filename.py --send_credit_card \n"
            f"python3 src/filename.py --delete_credit_card \n"
            f"python3 src/filename.py --send_one_bill --credit_card_account_id 1 --bill_total_amount 100 --bill_id 1 "
            f"--due_date 2022-07-01 --is_instalment true --bill_minimum_amount 1 --bill_minimum_amount_currency BRL "
            f"--bill_total_amount_currency BRL --finance_charges "
            + '\'{"type": "mora", "additionalInfo": "test", "amount": 25, "currency": "BRL"}\''
            f"--payments "
            + '\'{"valueType": "test", "paymentDate": "2022-07-22", "paymentMode": "dinheiro", "amount": 100, "currency": "BRL"}\''
            + "\n"
            f"python3 src/filename.py --delete_one_bill --bill_id 1 \n"
            f"python3 src/filename.py --send_bills \n"
            f"python3 src/filename.py --delete_bills \n"
            f"python3 src/filename.py --send_one_credit_card_limit --credit_card_account_id 123321 --available_amount 1 "
            f"--consolidation_type CONSOLIDADO --credit_line_limit_type LIMITE_CREDITO_TOTAL --is_limit_flexible true "
            f"--limit_amount 10 --limit_amount_currency BRL --line_name CREDITO_A_VISTA --line_name_additional_info test "
            f"--used_amount 1 --identification_number 0084 --used_amount_currency BRL --available_amount_currency BRL \n"
            f"python3 src/filename.py --send_credit_card_limits \n"
            f"python3 src/kafka_producer.py --send_one_balance --account_id 300 --client_id elomirjunior --request_id 2 "
            f'--request_date "2022-08-11 00:00:00.000" --available_amount 100 --available_amount_currency BRL --blocked_amount 0 '
            f"--blocked_amount_currency BRL --automatically_invested_amount 0 --automatically_invested_amount_currency BRL \n"
        )


if __name__ == "__main__":
    main()
