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
"""Cloud BigQuery module."""

import logging
import pathlib
import typing

from google.cloud import bigquery
from google.cloud import exceptions


# Main workflow sql.
_MAIN_WORKFLOW_SQL = 'sql/main_workflow.sql'

# Set logging level.
logging.getLogger().setLevel(logging.INFO)
logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)


def create_dataset_if_not_exists(
    project_id: str, dataset_id: str, dataset_location: str
) -> None:
  """Creates BigQuery dataset if it doesn't exist.

  Args:
    project_id: Cloud project ID.
    dataset_id: BigQuery dataset ID.
    dataset_location: location of BigQuery dataset.
  """
  # Construct a BigQuery client object.
  client = bigquery.Client(project=project_id)
  fully_qualified_dataset_id = f'{project_id}.{dataset_id}'
  try:
    client.get_dataset(fully_qualified_dataset_id)
    logging.info('Dataset %s already exists.', fully_qualified_dataset_id)
  except exceptions.NotFound:
    logging.info('Dataset %s is not found.', fully_qualified_dataset_id)
    dataset = bigquery.Dataset(fully_qualified_dataset_id)
    dataset.location = dataset_location
    client.create_dataset(dataset)
    logging.info('Dataset %s created.', fully_qualified_dataset_id)


def load_language_codes(project_id: str, dataset_id: str) -> None:
  """Loads language codes.

  Args:
    project_id: Cloud project ID.
    dataset_id: BigQuery dataset ID.
  """
  client = bigquery.Client(project=project_id)
  fully_qualified_table_id = f'{project_id}.{dataset_id}.language_codes'
  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.CSV,
      skip_leading_rows=1,
      autodetect=True,
  )
  file_name = 'data/language_codes.csv'
  with open(file_name, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file, fully_qualified_table_id, job_config=job_config
    )

  job.result()


def load_geo_targets(project_id: str, dataset_id: str) -> None:
  """Loads geo targets.

  Args:
    project_id: Cloud project ID.
    dataset_id: BigQuery dataset ID.
  """
  client = bigquery.Client(project=project_id)
  fully_qualified_table_id = f'{project_id}.{dataset_id}.geo_targets'
  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.CSV,
      skip_leading_rows=1,
      autodetect=True,
  )
  file_name = 'data/geo_targets.csv'
  with open(file_name, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file, fully_qualified_table_id, job_config=job_config
    )

  job.result()


def configure_sql(
    sql_path: str, query_params: typing.Dict[str, typing.Any]
) -> str:
  """Configures parameters of SQL script with variables supplied.

  Args:
    sql_path: Path to SQL script.
    query_params: Configuration containing query parameter values.

  Returns:
    sql_script: String representation of SQL script with parameters assigned.
  """
  sql_script = pathlib.Path(sql_path).read_text()

  params = {}
  for param_key, param_value in query_params.items():
    # If given value is list of strings (ex. 'a,b,c'), create tuple of
    # strings (ex. ('a', 'b', 'c')) to pass to SQL IN operator.
    if isinstance(param_value, str) and ',' in param_value:
      params[param_key] = tuple(param_value.split(','))
    else:
      params[param_key] = param_value

  return sql_script.format(**params)


def execute_queries(
    project_id: str,
    dataset_id: str,
    dataset_location: str,
    merchant_id: str,
    customer_id: str,
) -> None:
  """Executes list of queries.

  Args:
    project_id: Cloud project ID.
    dataset_id: BigQuery dataset ID.
    dataset_location: location of BigQuery dataset.
    merchant_id: Merchant center ID.
    customer_id: Google Ads customer ID.
  """
  # Sql files to be executed in a specific order.
  sql_files = [
      'sql/inventory.sql',
      'sql/best_sellers.sql',
      _MAIN_WORKFLOW_SQL,
  ]
  query_params = {
      'project_id': project_id,
      'dataset': dataset_id,
      'merchant_id': merchant_id,
      'external_customer_id': customer_id,
  }
  client = bigquery.Client(project=project_id)
  for sql_file in sql_files:
    try:
      query = configure_sql(sql_file, query_params)
      query_job = client.query(query, location=dataset_location)
      query_job.result()
    except:
      logging.exception('Error in %s', sql_file)
      raise


def get_main_workflow_sql(
    project_id: str, dataset_id: str, merchant_id: str, customer_id: str
) -> str:
  """Returns main workflow sql.

  Args:
    project_id: Cloud project ID.
    dataset_id: BigQuery dataset ID.
    merchant_id: Merchant center ID.
    customer_id: Google Ads customer ID.
  """
  query_params = {
      'project_id': project_id,
      'dataset': dataset_id,
      'merchant_id': merchant_id,
      'external_customer_id': customer_id,
  }
  return configure_sql(_MAIN_WORKFLOW_SQL, query_params)
