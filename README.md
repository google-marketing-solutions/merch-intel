# Merch Intel

Disclaimer: This is not an officially supported Google product.

Merch Intel is a dashboard for visualizing Market Insights data from Google
Merchant Center. This includes best-selling products and brands, competitive
price points, sale price suggestions, and information about the competitive
landscape for your industry.

## 1. Overview

## 2. Installation

### 2.1. Prerequisites

#### 2.1.1. A Google Cloud project with billing enabled

You may skip this step if you already have a Google Cloud account with billing enabled.

* [Create a Google Cloud account](https://cloud.google.com/docs/get-started)

* [Create and manage projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

* [Manage your Cloud Billing account](https://cloud.google.com/billing/docs/how-to/manage-billing-account)

#### 2.1.2. Google Merchant Center, Google Ads and Google Cloud permissions

The user running the installation script will need the following permissions:

* [Standard access for Google Merchant Center](https://support.google.com/merchants/answer/1637190?hl=en)

* [Standard access for Google Ads](https://support.google.com/google-ads/answer/7476552?hl=en)

* [Editor (or owner) role in the Google Cloud project](https://cloud.google.com/iam/docs/understanding-roles)

### 2.2. Setup local environment

### 2.2.1. Option 1: Cloud Shell

Merch Intel can be installed directly in the Google Cloud console using
[Cloud Shell](https://ssh.cloud.google.com/cloudshell?shellonly=true), which
comes with `gcloud` already installed. It does however disconnect after 1 hour
without any user interaction and may require, for Google Merchant Center
accounts with a large number of products, the user to press Enter while the
script is running to reset the timer.

### 2.2.2. Option 2: Local

Merch Intel can also be installed on a local environment, which will not time
out but requires `gcloud` to be installed and set up manually:

* [Install Google Cloud CLI](https://cloud.google.com/sdk/?e=48754805&hl=en#Quick_Start)

##### 2.3. Download source code

Open [Cloud Shell](https://ssh.cloud.google.com/cloudshell?shellonly=true) or
your terminal (if running locally) and download the repository from GitHub.

```
git clone https://github.com/google-marketing-solutions/merch-intel
```

##### 2.4. Run install script

The following values will be needed to run the installation script:

* [Google Cloud project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

* [Google Merchant Center ID](https://support.google.com/merchants/answer/12159157?hl=en)

* [Google Ads customer ID](https://support.google.com/google-ads/answer/1704344?hl=en)

```
cd merch-intel;
sh setup.sh --project_id=<project_id> --merchant_id=<merchant_id> --ads_customer_id=<ads_customer_id>
```

During installation, the script may ask you to open authorization URLs in the
browser. Follow the instructions to proceed.

The script will perform the following:

* Enable Google Cloud components and Google APIs:

 * [BigQuery](https://console.cloud.google.com/bigquery)

 * [BigQuery Data Transfer](https://console.cloud.google.com/bigquery/transfers)

* Create the Google Merchant Center and Google Ads BigQuery data transfers.

* Set up a daily job that will create the `InventoryView` and
`BestSellerWeeklyProductView` Merch Intel tables

##### 2.5. Create Looker Studio data sources

Copy the following Looker Studio data sources and reconnect them to your Merch
Intel tables:
* [InventoryView (TEMPLATE)](https://lookerstudio.google.com/c/datasources/deb2d31b-fd28-4a5b-9dd4-3bee3aa975e3)
 and reconnect it to `merch_intel.InventoryView`
* [BestSellerWeeklyProductView (TEMPLATE)](https://lookerstudio.google.com/c/datasources/e427ee64-eee2-4cf6-bc67-efa8eeccc208)
 and reconnect it to `merch_intel.BestSellerWeeklyProductView`

To copy and reconnect a data source:

* Click on the data source template link above.

* Click on the <img src="images/copy_icon.png"> icon in the top right corner
 next to "Create report".

* Click "Copy Data Source" on the "Copy Data Source" pop-up.

* Select your project, dataset, and table to be connected, then press
 "Reconnect" in the top right corner.

* Click "Apply" on the "Apply Connection Changes" pop-up.

* Repeat this process for all data source templates above.

##### 2.6. Create Looker Studio dashboard

* Open the
    [Merch Intel template](https://lookerstudio.google.com/c/reporting/cd1b8fa2-e7d5-4ac4-874f-e022ebd20467/preview)

* Click "Use my own data"

* Replace the data sources by choosing the new `InventoryView` and
 `BestSellerWeeklyProductView` Merch Intel data sources created in the previous
 step

##### Note - Performance metrics in the dashboard may take 12-24 hours to appear.