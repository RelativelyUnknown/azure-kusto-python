# Microsoft Azure Kusto (Azure Data Explorer) SDK for Python

This repository holds two packages:

- [**azure-kusto-data**](https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data) lets you query Kusto clusters and run management commands.<br>
  [![PyPI version](https://badge.fury.io/py/azure-kusto-data.svg)](https://badge.fury.io/py/azure-kusto-data)
  [![Downloads](https://pepy.tech/badge/azure-kusto-data)](https://pepy.tech/project/azure-kusto-data)
- [**azure-kusto-ingest**](https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-ingest) sends data to Kusto (queued, streaming or managed streaming ingestion).<br>
  [![PyPI version](https://badge.fury.io/py/azure-kusto-ingest.svg)](https://badge.fury.io/py/azure-kusto-ingest)
  [![Downloads](https://pepy.tech/badge/azure-kusto-ingest)](https://pepy.tech/project/azure-kusto-ingest)

## Install

From the Python Package Index:

```bash
pip install azure-kusto-data
pip install azure-kusto-ingest
```

Or from source:

```bash
git clone https://github.com/Azure/azure-kusto-python
```

### Optional extras

Both packages have the same optional extras. Install only what you use:

| Extra     | Adds                                                                 | Example                                 |
| --------- | -------------------------------------------------------------------- | --------------------------------------- |
| `pandas`  | Results as a `DataFrame`, and ingesting a `DataFrame`                | `pip install azure-kusto-data[pandas]`  |
| `pyarrow` | Results as Arrow tables and record batches, and ingesting Arrow data | `pip install azure-kusto-data[pyarrow]` |
| `aio`     | The asynchronous clients (`azure.kusto.data.aio`)                    | `pip install azure-kusto-data[aio]`     |

## Minimum requirements

- Python 3.9 or later
- The dependencies are listed in each package's `pyproject.toml`

## Authentication methods

The clients authenticate with Microsoft Entra ID (formerly Azure Active Directory), or with a token that you provide. Choose the method with the `KustoConnectionStringBuilder`:

* Azure CLI - use the account that is signed in to the [Azure CLI](https://learn.microsoft.com/cli/azure/).
* Interactive login or device code - sign in through a browser window, or with a code.
* Microsoft Entra application - authenticate with an application ID and a secret, or with a certificate.
* Managed identity - authenticate with the managed identity of an Azure resource.
* Azure credential - use any `azure-identity` credential, such as `DefaultAzureCredential`.
* User name and password - this doesn't work for accounts that require multi-factor authentication.
* Token - provide your own token or token provider.

The [azure-kusto-data README](https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data#authenticate-the-client) lists every method.

## Samples

- [Quick start app](https://github.com/Azure/azure-kusto-python/tree/master/quick-start): a complete, runnable script that connects, ingests and queries.
- [Query snippets](https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py)
- [Ingestion snippets](https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py)

## Best practices

See the SDK [best practices guide](https://learn.microsoft.com/kusto/api/netfx/kusto-ingest-best-practices). It was written for the .NET SDK, but most of it applies here too.

## Need help?

- **Feature request?** Post it on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer).
- **Technical question?** Ask on [Microsoft Q&A](https://learn.microsoft.com/answers/topics/azure-data-explorer.html) or on [Stack Overflow with the tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer).
- **Need support?** Every customer with an active Azure subscription can [open a support ticket](https://learn.microsoft.com/azure/azure-portal/supportability/how-to-create-azure-support-request).
- **Found a bug?** Please [file an issue](https://github.com/Azure/azure-kusto-python/issues/new) with as much detail as you can.

## SDKs for other languages

- [Node](https://github.com/azure/azure-kusto-node)
- [Java](https://github.com/azure/azure-kusto-java)
- [.NET](https://learn.microsoft.com/kusto/api/netfx/about-the-sdk)
- [Go](https://github.com/Azure/azure-kusto-go)

## Contribute

We gladly accept community contributions. Report bugs in the Issues section of GitHub, and see [CONTRIBUTING.md](CONTRIBUTING.md) if you'd like to send code.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the
[Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any questions or comments.
