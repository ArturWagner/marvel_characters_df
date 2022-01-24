import os
import time
import hashlib
import pandas as pd
from pandas import DataFrame
from typing import Dict, List
import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


logging.basicConfig(
    format='%(asctime)s %(levelname)-4s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)


def env_config_keys() -> dict:
    '''Build a config from environment.

    Returns:
      Dict with credentials keys.

    Raises:
      KeyError: If no keys is found on environment.
    '''
    try:
        return {
            'public_key': os.environ['PUBLIC_KEY'],
            'private_key': os.environ['PRIVATE_KEY']
        }
    except KeyError as e:
        logging.error('Make sure to set PUBLIC_KEY and PRIVATE_KEY as env var')
        raise e


class APIClient:
    '''Base class containing the requests logic.
    Note:
        This class was created using this docs
        https://developer.marvel.com/docs#!

    Attributes:
        base_url (str): Api url
    '''

    base_url = 'https://gateway.marvel.com/v1/public/'

    def __init__(self, config: Dict) -> None:
        '''
        Args:
            config (obj):  Python dict containing private_key and public_key.
        '''
        self.config = config
        self.pub_key = config.get('public_key')
        self.private_key = config.get('private_key')

    def gen_md5_hash(self, timestamp: str) -> str:
        '''Method to generate md5 hash used to authenticate the request.

        Args:
            timestamp (str): Stringing containing a timestamp.

        Returns:
            md5 digested key as string
        '''
        key_string = timestamp+self.private_key+self.pub_key
        key = hashlib.md5(key_string.encode())
        return key.hexdigest()

    def requests_retry_session(
        self,
        retries: int = 5,
        backoff_factor: int = 2,
        status_forcelist: List = (500, 502, 504),
        session: requests.Session = None,
    ) -> requests.Session:
        '''Method to start a request and retry session.

        Args:
            retries (int): How many times to retry on bad status codes.
                These are retries made on responses, where status code
                matches status_forcelist.
            backoff_factor (int):
                A backoff factor to apply between attempts
                after the second try.
            status_forcelist (List):
                A set of integer HTTP status codes that we should
                force a retry on.
            session (requests.Session): A request session used to
                persist certain parameters across requests.

        Returns:
            A request session
        '''

        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def do_request(
        self,
        endpoint: str,
        params: Dict,
        session: requests.Session
    ) -> requests.Response:
        '''Method to execute one request.

        Args:
            endpoint (str): String with a valid Marvel endpoint.
                Starts without /
            params (Dict):
                A backoff factor to apply between attempts
                after the second try.
            session (requests.Session): A request session.

        Returns:
            A raw request response.
        '''
        url = f'{self.base_url}{endpoint}'

        raw_response = self.requests_retry_session(session=session).get(
            url, params=params
        )

        if raw_response.status_code != 200:
            logging.error('Something went wrong.')
            raise Exception(raw_response.json())

        return raw_response

    def sync(self, offset: int) -> Dict:
        '''Method which builds and execute the request.

        Args:
            offset (int): Number of records to skip

        Returns:
            A Dict containing the response content
        '''
        timestamp = str(int(time.time()))
        params = {
            'apikey': self.pub_key,
            'hash': self.gen_md5_hash(timestamp),
            'limit': self.limit,
            'ts': timestamp,
            'offset': offset
        }

        if self.config.get('modifiedSince'):
            params['modifiedSince'] = self.config.get('modifiedSince')

        session = requests.Session()
        response = self.do_request(
            self.endpoint,
            params,
            session
        ).json()

        return response


class Characters(APIClient):
    '''Retrieves characters from the marvel API.

    Attributes:
        endpoint (str): Api endpoint
        limit (int): Api parameter to limit the result
            set to the specified number of resources
        fields_to_aggregate (list): Columns fields to be aggregated
        df (DataFrame): Empty Pandas DF, will be populated later.
    '''
    endpoint = 'characters'
    limit = 100
    fields_to_aggregate = ['comics',  'series',  'stories', 'events']
    df = DataFrame()

    def append_data_to_df(self, results: List[Dict]) -> None:
        '''Method which add a data to characters df

        Args:
            results (List[Dict]): A list with each item as a character dict

        Returns:
            No directy result, but this affects the class df
        '''
        results_df = pd.DataFrame.from_dict(results)
        self.df = pd.concat([self.df, results_df], ignore_index=True)

    def aggregate_df_columns(self) -> None:
        '''Method which filter get the aggregated results of each col

        Returns:
            No directy result, but this affects the class df
        '''
        for col in self.fields_to_aggregate:
            self.df[col] = self.df[col].map(lambda x: x['available'])

    def clean_df_extra_column(self) -> None:
        '''Method which select the desired columns

        Returns:
            No directy result, but this affects the class df
        '''
        self.df = self.df[[
            'id',
            'name',
            'description',
            'comics',
            'series',
            'stories',
            'events'
        ]]


def extract_dataframe() -> DataFrame:
    config = env_config_keys()
    characters_endpoint = Characters(config)

    logging.info('Starting extraction')
    characters_data = characters_endpoint.sync(0)
    offset = characters_data['data']['offset']
    total = characters_data['data']['total']

    while offset < total:
        results = characters_data['data']['results']
        characters_endpoint.append_data_to_df(results)

        offset += 100
        characters_data = characters_endpoint.sync(offset)
        logging.info(
            f'Extrated {len(characters_endpoint.df)} of {total} total.'
        )

    logging.info('End extraction')
    characters_endpoint.aggregate_df_columns()
    characters_endpoint.clean_df_extra_column()

    # Uncomment the line bellow to export a csv
    # characters_endpoint.df.to_csv('characters.csv')

    return characters_endpoint.df


def main() -> DataFrame:
    return extract_dataframe()


if __name__ == '__main__':
    main()
