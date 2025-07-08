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
from typing import Dict, Optional

import auth
import google.auth
from google.auth import impersonated_credentials
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery_datatransfer
from google.cloud.bigquery_datatransfer_v1.types import TransferConfig
from google.cloud.bigquery_datatransfer_v1.types import TransferState
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
import pytz

# The unique identifier for the Merchant Center data source.
_MERCHANT_CENTER_ID = 'merchant_center'
# The unique identifier for the Google Ads data source.
_GOOGLE_ADS_ID = 'google_ads'
# The unique identifier for the Scheduled Query data source.
_SCHEDULED_QUERY_ID = 'scheduled_query'

# Polling interval in seconds between transfer status checks.
_SLEEP_SECONDS = 60
# Maximum number of polling attempts to prevent infinite loops.
_MAX_POLL_COUNTER = 100

_ADS_TABLES = [
    'ShoppingProductStats',
]


class Error(Exception):
  """Base error for this module."""


class DataTransferError(Error):
  """An exception to be raised when data transfer was not successful."""


class CloudDataTransferUtils:
  """This class provides methods to manage BigQuery data transfers.

  Typical usage example:
    >>> data_transfer = CloudDataTransferUtils('project_id')
    >>> data_transfer.create_merchant_center_transfer(12345, 'dataset_id', 'US')
  """
  # TODO(b/432669656): Add unit tests for this class

  def __init__(
      self,
      project_id: str,
      impersonated_service_account: Optional[str] = None,
  ):
    """Initialises new instance of CloudDataTransferUtils.

    Args:
      project_id: GCP project id.
      impersonated_service_account: Optional. The email of the service account
        to impersonate. If None, uses Application Default Credentials.
    """
    self.project_id = project_id

    try:
      if impersonated_service_account:
        logging.info(
            'Impersonation mode ENABLED. Using service account: %s',
            impersonated_service_account,
        )

        source_credentials, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )

        target_credentials = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=impersonated_service_account,
            target_scopes=['https://www.googleapis.com/auth/cloud-platform'],
        )

        self.client = bigquery_datatransfer.DataTransferServiceClient(
            credentials=target_credentials
        )
      else:
        logging.info(
            'Impersonation mode DISABLED. Using Application Default '
            'Credentials.'
        )
        self.client = bigquery_datatransfer.DataTransferServiceClient()

    except DefaultCredentialsError as e:
      logging.error(
          'Could not determine credentials. Please configure your environment '
          'with "gcloud auth application-default login" or ensure you are '
          'running in a configured GCP environment. Error: %s',
          e,
      )
      raise

  def wait_for_transfer_completion(
      self, transfer_config: TransferConfig, dataset_location: str
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
    poll_counter = 0

    while True:
      transfer_config_path = (
          f'projects/{self.project_id}/locations/{dataset_location}'
          f'/transferConfigs/{transfer_config_id}'
      )
      response = self.client.list_transfer_runs(parent=transfer_config_path)
      latest_transfer = next(iter(response), None)

      if not latest_transfer:
        logging.info(
            'No transfer runs found for %s. Assuming completion.',
            transfer_config_name,
        )
        return

      if latest_transfer.state == TransferState.SUCCEEDED:
        logging.info('Transfer %s was successful.', transfer_config_name)
        return

      if latest_transfer.state in (
          TransferState.FAILED,
          TransferState.CANCELLED,
      ):
        error_message = (
            f'Transfer {transfer_config_name} was not successful. '
            f'Final state: {latest_transfer.state.name}. '
            f'Error: {latest_transfer.error_status}'
        )
        logging.error(error_message)
        raise DataTransferError(error_message)

      logging.info(
          'Transfer %s still in progress (State: %s). Sleeping for %s '
          'seconds before checking again.',
          transfer_config_name,
          latest_transfer.state.name,
          _SLEEP_SECONDS,
      )
      time.sleep(_SLEEP_SECONDS)

      poll_counter += 1
      if poll_counter >= _MAX_POLL_COUNTER:
        error_message = (
            f'Transfer {transfer_config_name} is taking too long to finish. '
            'Exiting due to max poll attempts.'
        )
        logging.error(error_message)
        raise DataTransferError(error_message)

  def _get_existing_transfer(
      self,
      data_source_id: str,
      dataset_location: str,
      destination_dataset_id: Optional[str] = None,
      params: Optional[Dict[str, str]] = None,
      name: Optional[str] = None,
  ) -> Optional[TransferConfig]:
    """Gets data transfer if it already exists."""
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    for transfer_config in self.client.list_transfer_configs(parent=parent):
      if transfer_config.data_source_id != data_source_id:
        continue
      if (
          destination_dataset_id
          and transfer_config.destination_dataset_id != destination_dataset_id
      ):
        continue

      # Ignore transfers that are already in a failed or cancelled state.
      is_valid_state = transfer_config.state in (
          TransferState.PENDING,
          TransferState.RUNNING,
          TransferState.SUCCEEDED,
      )
      params_match = self._check_params_match(transfer_config, params)
      name_matches = name is None or name == transfer_config.display_name

      if params_match and is_valid_state and name_matches:
        return transfer_config
    return None

  def _check_params_match(
      self,
      transfer_config: TransferConfig,
      params: Dict[str, str],
  ) -> bool:
    """Checks if given parameters are present in transfer config."""
    if not params:
      return True
    for key, value in params.items():
      config_params = transfer_config.params
      if key not in config_params or config_params[key] != value:
        return False
    return True

  def _update_existing_transfer(
      self,
      transfer_config: TransferConfig,
      params: struct_pb2.Struct,
  ) -> TransferConfig:
    """Updates existing data transfer if parameters have changed."""
    if self._check_params_match(transfer_config, params):
      logging.info(
          'The data transfer config "%s" parameters already match. '
          'Skipping update.',
          transfer_config.display_name,
      )
      return transfer_config

    new_transfer_config = copy.deepcopy(transfer_config)
    new_transfer_config.params.clear()
    new_transfer_config.params.update(params)

    # The update_mask ensures that only the 'params' field is modified.
    update_mask = {'paths': ['params']}
    request = bigquery_datatransfer.UpdateTransferConfigRequest(
        transfer_config=new_transfer_config, update_mask=update_mask
    )
    new_transfer_config = self.client.update_transfer_config(request)
    logging.info(
        'The data transfer config "%s" parameters were updated.',
        new_transfer_config.display_name,
    )
    return new_transfer_config

  def create_merchant_center_transfer(
      self, merchant_id: str, destination_dataset: str, dataset_location: str
  ) -> TransferConfig:
    """Creates a new merchant center transfer."""
    logging.info('Creating Merchant Center Transfer.')
    parameters = struct_pb2.Struct()
    parameters.update({
        'merchant_id': merchant_id,
        'export_products': True,
        'export_performance': True,
        'export_best_sellers_v2': True,
        'export_price_competitiveness': True,
        'export_price_insights': True,
        'export_offer_targeting': True,
    })

    existing_transfer = self._get_existing_transfer(
        _MERCHANT_CENTER_ID,
        dataset_location,
        destination_dataset_id=destination_dataset,
        params=parameters,
    )
    if existing_transfer:
      logging.info(
          'Data transfer for merchant id %s to destination dataset %s '
          'already exists.',
          merchant_id,
          destination_dataset,
      )
      return self._update_existing_transfer(existing_transfer, parameters)

    logging.info(
        'Creating data transfer for merchant id %s to destination dataset %s',
        merchant_id,
        destination_dataset,
    )
    has_valid_credentials = self._check_valid_credentials(
        _MERCHANT_CENTER_ID, dataset_location
    )
    version_info = None
    if not has_valid_credentials:
      version_info = self._get_version_info(
          _MERCHANT_CENTER_ID, dataset_location
      )
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    input_config = TransferConfig(
        display_name=f'Merchant Center Transfer - {merchant_id}',
        data_source_id=_MERCHANT_CENTER_ID,
        destination_dataset_id=destination_dataset,
        params=parameters,
        data_refresh_window_days=0,
    )
    request = bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=input_config,
        version_info=version_info,
    )
    transfer_config = self.client.create_transfer_config(request)
    logging.info(
        'Data transfer created for merchant id %s to destination dataset %s.',
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
  ) -> TransferConfig:
    """Creates a new Google Ads transfer and schedules a backfill."""
    logging.info('Creating Google Ads Transfer.')

    parameters = struct_pb2.Struct()
    parameters.update({
        'customer_id': customer_id,
        'include_pmax': True,
        'table_filter': ','.join(_ADS_TABLES),
    })
    existing_transfer = self._get_existing_transfer(
        _GOOGLE_ADS_ID,
        dataset_location,
        destination_dataset_id=destination_dataset,
        params=parameters,
    )
    if existing_transfer:
      logging.info(
          'Data transfer for Google Ads customer id %s to destination dataset '
          '%s already exists.',
          customer_id,
          destination_dataset,
      )
      return existing_transfer

    logging.info(
        'Creating data transfer for Google Ads customer id %s to destination '
        'dataset %s.',
        customer_id,
        destination_dataset,
    )
    has_valid_credentials = self._check_valid_credentials(
        _GOOGLE_ADS_ID, dataset_location
    )
    version_info = None
    if not has_valid_credentials:
      version_info = self._get_version_info(
          _GOOGLE_ADS_ID, dataset_location
      )
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    input_config = TransferConfig(
        display_name=f'Google Ads Transfer - {customer_id}',
        data_source_id=_GOOGLE_ADS_ID,
        destination_dataset_id=destination_dataset,
        params=parameters,
        data_refresh_window_days=1,
    )
    request = bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=input_config,
        version_info=version_info,
    )
    transfer_config = self.client.create_transfer_config(request=request)
    logging.info(
        'Data transfer created for Google Ads customer id %s.',
        customer_id,
    )

    if backfill_days > 0:
      logging.info('Scheduling backfill for the last %d days.', backfill_days)
      now_utc = datetime.datetime.now(pytz.utc)
      start_time = now_utc - datetime.timedelta(days=backfill_days)
      end_time = now_utc
      start_time_pb = timestamp_pb2.Timestamp()
      end_time_pb = timestamp_pb2.Timestamp()
      start_time_pb.FromDatetime(
          start_time.replace(hour=0, minute=0, second=0, microsecond=0)
      )
      end_time_pb.FromDatetime(
          end_time.replace(hour=0, minute=0, second=0, microsecond=0)
      )

      self.client.schedule_transfer_runs(
          parent=transfer_config.name,
          start_time=start_time_pb,
          end_time=end_time_pb,
      )
    return transfer_config

  def schedule_query(
      self, name: str, dataset_location: str, query_string: str
  ) -> TransferConfig:
    """Schedules a query to run daily."""
    parameters = struct_pb2.Struct()
    parameters['query'] = query_string

    existing_transfer = self._get_existing_transfer(
        _SCHEDULED_QUERY_ID, dataset_location, name=name
    )

    if existing_transfer:
      logging.info('Scheduled query "%s" already exists. Updating...', name)
      updated_transfer_config = self._update_existing_transfer(
          existing_transfer, parameters
      )
      logging.info('Triggering a manual run for the updated query.')
      start_time_pb = timestamp_pb2.Timestamp()
      start_time_pb.FromDatetime(datetime.datetime.now(pytz.utc))
      request = bigquery_datatransfer.StartManualTransferRunsRequest(
          parent=updated_transfer_config.name,
          requested_run_time=start_time_pb,
      )
      self.client.start_manual_transfer_runs(request=request)
      return updated_transfer_config
    has_valid_credentials = self._check_valid_credentials(
        'scheduled_query', dataset_location
    )
    version_info = ''
    if not has_valid_credentials:
      version_info = self._get_version_info(
          'scheduled_query', dataset_location
      )
    parent = f'projects/{self.project_id}/locations/{dataset_location}'
    input_config = TransferConfig(
        display_name=name,
        data_source_id=_SCHEDULED_QUERY_ID,
        params=parameters,
        schedule='every 24 hours',
    )
    request = bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=input_config,
        version_info=version_info,
    )
    transfer_config = self.client.create_transfer_config(request=request)
    logging.info('Scheduled query "%s" created successfully.', name)
    return transfer_config

  def _get_data_source(
      self, data_source_id: str, dataset_location: str
  ) -> bigquery_datatransfer.DataSource:
    """Returns data source details."""
    name = (
        f'projects/{self.project_id}/locations/{dataset_location}/'
        f'dataSources/{data_source_id}'
    )
    return self.client.get_data_source(name=name)

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

  def _get_version_info(
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
    return auth.retrieve_version_info(client_id, scopes, data_source_id)
