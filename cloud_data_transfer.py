# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# python3
"""Module for managing BigQuery data transfers."""

import copy
import datetime
import logging
import time
from typing import Any, Dict

import auth
from google.cloud import bigquery_datatransfer
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
import pytz


_MERCHANT_CENTER_ID = 'merchant_center'  # Data source id for Merchant Center.
_GOOGLE_ADS_ID = 'google_ads'  # Data source id for Google Ads.
_SLEEP_SECONDS = 60  # Seconds to sleep before checking resource status.
_MAX_POLL_COUNTER = 100
_PENDING_STATE = 2
_RUNNING_STATE = 3
_SUCCESS_STATE = 4
_FAILED_STATE = 5
_CANCELLED_STATE = 6

_ADS_TABLES = [
    'ShoppingProductStats',
]


class Error(Exception):
  """Base error for this module."""


class DataTransferError(Error):
  """An exception to be raised when data transfer was not successful."""


class CloudDataTransferUtils(object):
  """This class provides methods to manage BigQuery data transfers.

  Typical usage example:
    >>> data_transfer = CloudDataTransferUtils('project_id')
    >>> data_transfer.create_merchant_center_transfer(12345, 'dataset_id')
  """

  def __init__(self, project_id: str):
    """Initialise new instance of CloudDataTransferUtils.

    Args:
      project_id: GCP project id.
    """
    self.project_id = project_id
    self.client = bigquery_datatransfer.DataTransferServiceClient()

  def wait_for_transfer_completion(
      self, transfer_config: Dict[str, Any], dataset_location: str
  ) -> None:
    """Waits for the completion of data transfer operation.

    This method retrieves data transfer operation and checks for its status. If
    the operation is not completed, then the operation is re-checked after
    `_SLEEP_SECONDS` seconds.

    Args:
      transfer_config: Resource representing data transfer.
      dataset_location: Location of the BigQuery dataset.

    Raises:
      DataTransferError: If the data transfer is not successfully completed.
    """
    transfer_config_name = transfer_config.name
    transfer_config_id = transfer_config_name.split('/')[-1]
    poll_counter = 0  # Counter to keep polling count.
    while True:
      transfer_config_path = f'projects/{self.project_id}/locations/{dataset_location}/transferConfigs/{transfer_config_id}'
      response = self.client.list_transfer_runs(
          {'parent': transfer_config_path}
      )
      latest_transfer = None
      for transfer in response:
        latest_transfer = transfer
        break
      if not latest_transfer:
        return
      if latest_transfer.state == _SUCCESS_STATE:
        logging.info('Transfer %s was successful.', transfer_config_name)
        return
      if (
          latest_transfer.state == _FAILED_STATE
          or latest_transfer.state == _CANCELLED_STATE
      ):
        error_message = (
            f'Transfer {transfer_config_name} was not successful. '
            f'Error - {latest_transfer.error_status}'
        )
        logging.error(error_message)
        raise DataTransferError(error_message)
      logging.info(
          'Transfer %s still in progress. Sleeping for %s seconds before '
          'checking again.',
          transfer_config_name,
          _SLEEP_SECONDS,
      )
      time.sleep(_SLEEP_SECONDS)
      poll_counter += 1
      if poll_counter >= _MAX_POLL_COUNTER:
        error_message = (
            f'Transfer {transfer_config_name} is taking too long'
            ' to finish. Hence failing the request.'
        )
        logging.error(error_message)
        raise DataTransferError(error_message)

  def _get_existing_transfer(
      self,
      data_source_id: str,
      dataset_location: str,
      destination_dataset_id: str = None,
      params: Dict[str, str] = None,
      name: str = None,
  ) -> bool:
    """Gets data transfer if it already exists.

    Args:
      data_source_id: Data source ID.
      dataset_location: Location of the BigQuery dataset.
      destination_dataset_id: BigQuery dataset id.
      params: Data transfer specific parameters.
      name: The display name of the transfer.

    Returns:
      Data Transfer if the transfer already exists.
      None otherwise.
    """
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    for transfer_config in self.client.list_transfer_configs(
        {'parent': parent}
    ):
      if transfer_config.data_source_id != data_source_id:
        continue
      if (
          destination_dataset_id
          and transfer_config.destination_dataset_id != destination_dataset_id
      ):
        continue
      # If the transfer config is in Failed state, we should ignore.
      is_valid_state = transfer_config.state in (
          _PENDING_STATE,
          _RUNNING_STATE,
          _SUCCESS_STATE,
      )
      params_match = self._check_params_match(transfer_config, params)
      name_matches = name is None or name == transfer_config.display_name
      if params_match and is_valid_state and name_matches:
        return transfer_config
    return None

  def _check_params_match(
      self,
      transfer_config: bigquery_datatransfer.TransferConfig,
      params: Dict[str, str],
  ) -> bool:
    """Checks if given parameters are present in transfer config.

    Args:
      transfer_config: Data transfer configuration.
      params: Data transfer specific parameters.

    Returns:
      True if given parameters are present in transfer config, False otherwise.
    """
    if not params:
      return True
    for key, value in params.items():
      config_params = transfer_config.params
      if key not in config_params or config_params[key] != value:
        return False
    return True

  def _update_existing_transfer(
      self,
      transfer_config: bigquery_datatransfer.TransferConfig,
      params: Dict[str, str],
  ) -> bigquery_datatransfer.TransferConfig:
    """Updates existing data transfer.

    If the parameters are already present in the config, then the transfer
    config update is skipped.

    Args:
      transfer_config: Data transfer configuration to update.
      params: Data transfer specific parameters.

    Returns:
      Updated data transfer config.
    """
    if self._check_params_match(transfer_config, params):
      logging.info(
          'The data transfer config "%s" parameters match. Hence '
          'skipping update.',
          transfer_config.display_name,
      )
      return transfer_config
    new_transfer_config = copy.deepcopy(transfer_config)
    # Clear existing parameter values.
    new_transfer_config.params = {}
    for key, value in params.items():
      new_transfer_config.params[key] = value
    # Only params field is updated.
    update_mask = {'paths': ['params']}
    request = bigquery_datatransfer.UpdateTransferConfigRequest(
        transfer_config=new_transfer_config, update_mask=update_mask
    )
    new_transfer_config = self.client.update_transfer_config(request)
    logging.info(
        'The data transfer config "%s" parameters updated.',
        new_transfer_config.display_name,
    )
    return new_transfer_config

  def create_merchant_center_transfer(
      self, merchant_id: str, destination_dataset: str, dataset_location: str
  ) -> bigquery_datatransfer.TransferConfig:
    """Creates a new merchant center transfer.

    Merchant center allows retailers to store product info into Google. This
    method creates a data transfer config to copy the product data to BigQuery.

    Args:
      merchant_id: Google Merchant Center(GMC) account id.
      destination_dataset: BigQuery dataset id.
      dataset_location: Location of the BigQuery dataset.

    Returns:
      Transfer config.
    """
    logging.info('Creating Merchant Center Transfer.')
    parameters = struct_pb2.Struct()
    parameters['merchant_id'] = merchant_id
    parameters['export_products'] = True
    parameters['export_performance'] = True
    parameters['export_best_sellers_v2'] = True
    parameters['export_price_competitiveness'] = True
    parameters['export_price_insights'] = True
    parameters['export_offer_targeting'] = True
    data_transfer_config = self._get_existing_transfer(
        _MERCHANT_CENTER_ID, dataset_location, destination_dataset, parameters
    )
    if data_transfer_config:
      logging.info(
          'Data transfer for merchant id %s to destination dataset %s '
          'already exists.',
          merchant_id,
          destination_dataset,
      )
      return self._update_existing_transfer(data_transfer_config, parameters)
    logging.info(
        'Creating data transfer for merchant id %s to destination dataset %s',
        merchant_id,
        destination_dataset,
    )
    has_valid_credentials = self._check_valid_credentials(
        _MERCHANT_CENTER_ID, dataset_location
    )
    authorization_code = None
    if not has_valid_credentials:
      authorization_code = self._get_authorization_code(
          _MERCHANT_CENTER_ID, dataset_location
      )
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    input_config = {
        'display_name': f'Merchant Center Transfer - {merchant_id}',
        'data_source_id': _MERCHANT_CENTER_ID,
        'destination_dataset_id': destination_dataset,
        'params': parameters,
        'data_refresh_window_days': 0,
    }
    request = bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=input_config,
        authorization_code=authorization_code,
    )
    transfer_config = self.client.create_transfer_config(request)
    logging.info(
        'Data transfer created for merchant id %s to destination dataset %s',
        merchant_id,
        destination_dataset,
    )
    return transfer_config

  def create_google_ads_transfer(
      self,
      customer_id: str,
      destination_dataset: str,
      dataset_location: str,
      backfill_days: int = 30,
  ) -> bigquery_datatransfer.TransferConfig:
    """Creates a new Google Ads transfer.

    This method creates a data transfer config to copy Google Ads data to
    BigQuery dataset.

    Args:
      customer_id: Google Ads customer id.
      destination_dataset: BigQuery dataset id.
      dataset_location: Location of the BigQuery dataset.
      backfill_days: Number of days to backfill.

    Returns:
      Transfer config.
    """
    logging.info('Creating Google Ads Transfer.')

    parameters = struct_pb2.Struct()
    parameters['customer_id'] = customer_id
    parameters['include_pmax'] = True
    parameters['table_filter'] = ','.join(_ADS_TABLES)
    data_transfer_config = self._get_existing_transfer(
        _GOOGLE_ADS_ID, dataset_location, destination_dataset, parameters
    )
    if data_transfer_config:
      logging.info(
          'Data transfer for Google Ads customer id %s to destination dataset '
          '%s already exists.',
          customer_id,
          destination_dataset,
      )
      return data_transfer_config
    logging.info(
        'Creating data transfer for Google Ads customer id %s to destination '
        'dataset %s',
        customer_id,
        destination_dataset,
    )
    has_valid_credentials = self._check_valid_credentials(
        _GOOGLE_ADS_ID, dataset_location
    )
    authorization_code = None
    if not has_valid_credentials:
      authorization_code = self._get_authorization_code(
          _GOOGLE_ADS_ID, dataset_location
      )
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    input_config = {
        'display_name': f'Google Ads Transfer - {customer_id}',
        'data_source_id': _GOOGLE_ADS_ID,
        'destination_dataset_id': destination_dataset,
        'params': parameters,
        'data_refresh_window_days': 1,
    }
    request = bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=input_config,
        authorization_code=authorization_code,
    )
    transfer_config = self.client.create_transfer_config(request=request)
    logging.info(
        'Data transfer created for Google Ads customer id %s to destination '
        'dataset %s',
        customer_id,
        destination_dataset,
    )
    if backfill_days:
      transfer_config_name = transfer_config.name
      transfer_config_id = transfer_config_name.split('/')[-1]
      start_time = datetime.datetime.now(tz=pytz.utc) - datetime.timedelta(
          days=backfill_days
      )
      end_time = datetime.datetime.now(tz=pytz.utc)
      start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
      end_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
      transfer_config_path = f'{parent}/transferConfigs/{transfer_config_id}'
      start_time_pb = timestamp_pb2.Timestamp()
      end_time_pb = timestamp_pb2.Timestamp()
      start_time_pb.FromDatetime(start_time)
      end_time_pb.FromDatetime(end_time)
      self.client.schedule_transfer_runs(
          parent=transfer_config_path,
          start_time=start_time_pb,
          end_time=end_time_pb,
      )
    return transfer_config

  def schedule_query(
      self, name: str, dataset_location: str, query_string: str
  ) -> bigquery_datatransfer.TransferConfig:
    """Schedules query to run every day.

    Args:
      name: Name of the scheduled query.
      dataset_location: Location of the BigQuery dataset.
      query_string: The query to be run.

    Returns:
      Transfer config.
    """
    data_transfer_config = self._get_existing_transfer(
        'scheduled_query', dataset_location, name=name
    )
    parameters = struct_pb2.Struct()
    parameters['query'] = query_string
    if data_transfer_config:
      logging.info(
          'Data transfer for scheduling query "%s" already exists.', name
      )
      updated_transfer_config = self._update_existing_transfer(
          data_transfer_config, parameters
      )
      logging.info('Data transfer for scheduling query "%s" updated.', name)
      start_time_pb = timestamp_pb2.Timestamp()
      start_time = datetime.datetime.now(tz=pytz.utc)
      start_time_pb.FromDatetime(start_time)
      request = bigquery_datatransfer.StartManualTransferRunsRequest(
          parent=updated_transfer_config.name, requested_run_time=start_time_pb
      )
      self.client.start_manual_transfer_runs(request=request)
      logging.info(
          'One time manual run started. It might take up to 1 hour for'
          ' performance data to reflect on the dash.'
      )
      return updated_transfer_config
    has_valid_credentials = self._check_valid_credentials(
        'scheduled_query', dataset_location
    )
    authorization_code = ''
    if not has_valid_credentials:
      authorization_code = self._get_authorization_code(
          'scheduled_query', dataset_location
      )
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    input_config = bigquery_datatransfer.TransferConfig(
        display_name=name,
        data_source_id='scheduled_query',
        params={'query': query_string},
        schedule='every 24 hours',
    )
    request = bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=input_config,
        authorization_code=authorization_code,
    )
    transfer_config = self.client.create_transfer_config(request=request)
    return transfer_config

  def _get_data_source(
      self, data_source_id: str, dataset_location: str
  ) -> bigquery_datatransfer.DataSource:
    """Returns data source.

    Args:
      data_source_id: Data source ID.
      dataset_location: Location of the BigQuery dataset.
    """
    name = f'projects/{self.project_id}/locations/{dataset_location}/dataSources/{data_source_id}'
    return self.client.get_data_source({'name': name})

  def _check_valid_credentials(
      self, data_source_id: str, dataset_location: str
  ) -> bool:
    """Returns true if valid credentials exist for the given data source.

    Args:
      data_source_id: Data source ID.
      dataset_location: Location of the BigQuery dataset.
    """
    name = f'projects/{self.project_id}/locations/{dataset_location}/dataSources/{data_source_id}'
    response = self.client.check_valid_creds({'name': name})
    return response.has_valid_creds

  def _get_authorization_code(
      self, data_source_id: str, dataset_location: str
  ) -> str:
    """Returns authorization code for a given data source.

    Args:
      data_source_id: Data source ID.
      dataset_location: Location of the BigQuery dataset.
    """
    data_source = self._get_data_source(data_source_id, dataset_location)
    client_id = data_source.client_id
    scopes = data_source.scopes

    if not data_source:
      raise AssertionError('Invalid data source')
    return auth.retrieve_authorization_code(client_id, scopes, data_source_id)
