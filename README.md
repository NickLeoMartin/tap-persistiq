# tap-persistiq

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md). 

This tap:

* Pulls raw data from the [PersistIQ v1. 0 API](http://apidocs.persistiq.com/#introduction)
* Extracts the following resources:
  + [Users](http://apidocs.persistiq.com/#users)
  + [Leads](http://apidocs.persistiq.com/#leads)
  + [Campaigns](http://apidocs.persistiq.com/#campaigns)
* Outputs the schema for each resource
* Incrementally pulls data based on the input state

## Streams

[users](http://apidocs.persistiq.com/#list-all-users)

* Endpoint: https://api.persistiq.com/v1/users
* Primary key fields: id
* Foreign key fields: none
* Replication strategy: FULL_TABLE
* Transformations: none

[leads](http://apidocs.persistiq.com/#list-all-leads)

* Endpoint: https://api.persistiq.com/v1/leads
* Primary key fields: id
* Foreign key fields: none
* Replication strategy: FULL_TABLE
* Transformations: none

[campaigns](http://apidocs.persistiq.com/#list-campaigns)

* Endpoint: https://api.persistiq.com/v1/campaigns
* Primary key fields: id
* Foreign key fields: lead_id > id, owner_id > id
* Replication strategy: FULL_TABLE
* Transformations: none

## Authentication

## Quick Start

### 1. Install

Clone this repository, and then install using setup. py. We recommend using a virtualenv:

``` bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-persistiq
    > pip install .
```

or alternatively:

``` bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install git+https://github.com/NickLeoMartin/tap-persistiq#egg=tap-persistiq
```

### 2. Dependent libraries

The following dependent libraries were installed. 

``` bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
```

* [singer-tools](https://github.com/singer-io/singer-tools)
* [target-stitch](https://github.com/singer-io/target-stitch)

### 3. Create your tap's `config.json` file. 

<!-- Intercom [Authentication Types](https://developers.intercom.com/building-apps/docs/authentication-types) explains how to get an `access_token` . Make sure your [OAuth Scope](https://developers.intercom.com/building-apps/docs/oauth-scopes) allows Read access to the endpoints above. Additionally, your App should use [API Version ](https://developers.intercom.com/building-apps/docs/update-your-api-version) **[v1. 4](https://developers.intercom.com/intercom-api-reference/v1.4/reference)**. -->

``` json
    {
        "api_key": "YOUR_PERSISTIQ_API_KEY",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-persistiq <api_user_email@your_company.com>"
    }
```

Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off. 

``` json
    {
        "currently_syncing": "leads",
        "bookmarks": {
            "users": "2019-09-27T22:34:39.000000Z",
            "companies": "2019-09-28T18:23:53Z"
        }
    }
```

### 4. Run the Tap in Discovery Mode

This creates a catalog. json for selecting objects/fields to integrate:

``` bash
    tap-persistiq --config config.json --discover > catalog.json
```

See the Singer docs on discovery mode
[here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode). 

### 5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

For Sync mode:

``` bash
    > tap-persistiq --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To load to json files to verify outputs:

``` bash
    > tap-persistiq --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:

``` bash
    > tap-persistiq --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```
