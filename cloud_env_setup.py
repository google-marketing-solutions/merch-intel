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
"""Cloud Environment setup module for Merch Intel.

This module automates the following 4 steps:
  1. Enable required Cloud APIs
  2. Create Google Merchant Center and Google Ads data transfers
  3. Create tables in BigQuery.
  4. Create a scheduled query to run the main workflow.
"""

import argparse
import logging

import cloud_bigquery
import cloud_data_transfer
from plugins.cloud_utils import cloud_api

# Set logging level.
logging.getLogger().setLevel(logging.INFO)
logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)

# Required Cloud APIs to be enabled.
_APIS_TO_BE_ENABLED = [
    'bigquery.googleapis.com',
    'bigquerydatatransfer.googleapis.com',
]
_DEFAULT_DATASET_ID = 'merch_intel'
_DEFAULT_DATASET_LOCATION = 'us'


def enable_apis(project_id: str) -> None:
  """Enables list of cloud APIs for given cloud project.

  Args:
    project_id: A cloud project id.
  """
  cloud_api_utils = cloud_api.CloudApiUtils(project_id=project_id)
  cloud_api_utils.enable_apis(_APIS_TO_BE_ENABLED)


def parse_boolean(arg: str):
  """Returns boolean representation of argument."""
  arg = str(arg).lower()
  if 'true'.startswith(arg):
    return True
  return False


def parse_arguments() -> argparse.Namespace:
  """Initialize command line parser using argparse.

  Returns:
    An argparse.ArgumentParser.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('--project_id', help='GCP project ID', required=True)
  parser.add_argument(
      '--dataset_id',
      help='BigQuery dataset ID',
      default=_DEFAULT_DATASET_ID,
      required=False,
  )
  parser.add_argument(
      '--dataset_location',
      help='BigQuery dataset location',
      default=_DEFAULT_DATASET_LOCATION,
      required=False,
  )
  parser.add_argument(
      '--merchant_id', help='Google Merchant Center account ID', required=True
  )
  parser.add_argument(
      '--ads_customer_id', help='Google Ads external customer ID', required=True
  )
  return parser.parse_args()


def main():
  args = parse_arguments()
  ads_customer_id = args.ads_customer_id.replace('-', '')
  data_transfer = cloud_data_transfer.CloudDataTransferUtils(args.project_id)
  logging.info('Enabling APIs.')
  enable_apis(args.project_id)
  logging.info('Enabled APIs.')
  logging.info('Creating %s dataset.', args.dataset_id)
  cloud_bigquery.create_dataset_if_not_exists(
      args.project_id, args.dataset_id, args.dataset_location
  )
  merchant_center_config = data_transfer.create_merchant_center_transfer(
      args.merchant_id, args.dataset_id, args.dataset_location
  )
  ads_config = data_transfer.create_google_ads_transfer(
      ads_customer_id, args.dataset_id, args.dataset_location
  )
  try:
    logging.info('Checking the GMC data transfer status.')
    data_transfer.wait_for_transfer_completion(
        merchant_center_config, args.dataset_location
    )
    logging.info('The GMC data have been successfully transferred.')
  except cloud_data_transfer.DataTransferError:
    logging.error(
        'If you have just created GMC transfer - you may need to'
        'wait for up to 90 minutes before the data of your Merchant'
        'account are prepared and available for the transfer.'
    )
    raise
  logging.info('Checking the Google Ads data transfer status.')
  data_transfer.wait_for_transfer_completion(ads_config, args.dataset_location)
  logging.info('The Google Ads data have been successfully transferred.')
  cloud_bigquery.load_language_codes(args.project_id, args.dataset_id)
  cloud_bigquery.load_geo_targets(args.project_id, args.dataset_id)
  logging.info('Creating Merch Intel tables.')
  cloud_bigquery.execute_queries(
      args.project_id,
      args.dataset_id,
      args.dataset_location,
      args.merchant_id,
      ads_customer_id,
  )
  logging.info('Created Merch Intel tables.')
  logging.info('Updating targeted products')
  query = cloud_bigquery.get_main_workflow_sql(
      args.project_id, args.dataset_id, args.merchant_id, ads_customer_id
  )
  data_transfer.schedule_query(
      f'Main workflow - {args.dataset_id} - {ads_customer_id}',
      args.dataset_location,
      query,
  )
  logging.info('Job created to run Merch Intel main workflow.')
  logging.info('Merch Intel installation is complete!')


if __name__ == '__main__':
  main()
