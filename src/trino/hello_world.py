from utilities.connection_clients import TrinoClient
from utilities.setup import TRINO_CONNECTION_CREDENTIALS

from common.logger import logger

#####


def main(trino_client: TrinoClient):
    with trino_client.cursor() as cursor:
        cursor.execute("SELECT 'Hello, World!'")
        rows = cursor.fetchall()
        for row in rows:
            logger.info(row[0])


#####

if __name__ == "__main__":
    trino_client = TrinoClient(**TRINO_CONNECTION_CREDENTIALS)
    main(trino_client=trino_client)
