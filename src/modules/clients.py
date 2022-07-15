import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from config.definitions import ROOT_DIR


class Clients:
    """Class responsible for clients methods"""

    def __init__(self, base_url, client_url):
        self.base_url = base_url
        self.client_url = client_url

    def send_one_client(
        self, producer: KafkaProducer, client_id: str, cpf_number: str
    ) -> None:
        """Send one client to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :param client_id: The ID of client
        :type client_id: str
        :param cpf_number: The cpf number of the client
        :type cpf_number: str
        :return: None
        """
        data = {"clientId": client_id, "cpfNumber": cpf_number}

        future = producer.send("clientsObserver", json.dumps(data).encode("utf-8"))
        result = future.get(timeout=60)

        print(f"sent client {client_id} to kafka")

    def send_clients(self, producer: KafkaProducer) -> None:
        """Read the data test and send to kafka

        :param producer: Producer of kafka to receive the data
        :type producer: KafkaProducer
        :return: None
        """
        with open(str(os.path.join(ROOT_DIR, "files", "client.csv")), "r") as files:
            next(files)
            for file in files:
                client = file.split(",")[0]
                cpf = file.split(",")[1].strip("\n")

                data = {"clientId": client, "cpfNumber": cpf}

                future = producer.send(
                    "clientsObserver", json.dumps(data).encode("utf-8")
                )
                result = future.get(timeout=60)

                print(f"sent client {client} to kafka")

    def delete_one_client(self, client_id: str) -> None:
        """Delete one client from pluggy api

        :param client_id: The ID of client
        :type client_id: str
        :return: None
        """
        if "," in client_id:
            client_id = client_id.split(",")[0]

        response = requests.delete(self.base_url + self.client_url + client_id)

        print(
            f"The client {client_id} was deleted with status code: {response.status_code}"
        )

    def delete_clients(self) -> None:
        """Delete clients on pluggy api

        :return: None
        """
        with open(str(os.path.join(ROOT_DIR, "files", "client.csv")), "r") as files:
            next(files)
            with ThreadPoolExecutor(max_workers=24) as executor:
                executor.map(self.delete_one_client, files)
