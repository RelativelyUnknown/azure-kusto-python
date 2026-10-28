# Microsoft Azure Kusto  (Azure Data Explorer) SDK  for Python

[*azure-kusto-data*](https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data) Package provides the capability to query Kusto clusters with Python.<br>
[![PyPI version](https://badge.fury.io/py/azure-kusto-data.svg)](https://badge.fury.io/py/azure-kusto-data)
[![Downloads](https://pepy.tech/badge/azure-kusto-data)](https://pepy.tech/project/azure-kusto-data)<br>
[*azure-kusto-ingest*](https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-ingest) Package allows sending data to Kusto service - i.e. ingest data.<br>
[![PyPI version](https://badge.fury.io/py/azure-kusto-ingest.svg)](https://badge.fury.io/py/azure-kusto-ingest)
[![Downloads](https://pepy.tech/badge/azure-kusto-ingest)](https://pepy.tech/project/azure-kusto-ingest)<br>


## Install
### Option 1: Via PyPi
To install via the Python Package Index (PyPI), type:

* `pip install azure-kusto-data`
* `pip install azure-kusto-ingest`

### Option 2: Source Via Git
To get the source code of the SDK via git just type:

```bash
git clone https://github.com/Azure/azure-kusto-python
```

### Option 3: Source Zip
Download a zip of the code via GitHub or PyPi. Then follow the same instructions in option 2.

### Optionals:
* [_Pandas_](https://pandas.pydata.org/) - Package provides extra functionality for use with pandas. Since these are optional dependencies, install with pandas:
    * `pip install azure-kusto-data[pandas]`
    * `pip install azure-kusto-ingest[pandas]`
* [_PyArrow_](https://arrow.apache.org/docs/python/) - Package provides the results of queries as Arrow tables and record batches, and ingestion of Arrow data. Since these are optional dependencies, install with pyarrow:
    * `pip install azure-kusto-data[pyarrow]`
    * `pip install azure-kusto-ingest[pyarrow]`
* [_aio_](https://docs.aiohttp.org/) - Package provides async interfaces.
    * `pip install azure-kusto-data[aio]`
    * `pip install azure-kusto-ingest[aio]`

## Minimum Requirements
* Python 3.9 and above
* See `pyproject.toml` files for dependencies

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

## Samples:

* [Kusto Quick Start Sample App](https://github.com/Azure/azure-kusto-python/tree/master/quick-start)

* [Kusto query sample snippets](https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py)

* [Data ingest sample snippets](https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py)

## Best Practices
See the SDK [best practices guide](https://learn.microsoft.com/kusto/api/netfx/kusto-ingest-best-practices), which though written for the .NET SDK, applies similarly here.

## Need Support?
- **Have a feature request for SDKs?** Please post it on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer) to help us prioritize
- **Have a technical question?** Ask on [Microsoft Q&A](https://learn.microsoft.com/answers/topics/azure-data-explorer.html) or on [Stack Overflow with tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer)
- **Need Support?** Every customer with an active Azure subscription has access to [support](https://learn.microsoft.com/azure/azure-portal/supportability/how-to-create-azure-support-request) with guaranteed response time.  Consider submitting a ticket and get assistance from Microsoft support team
- **Found a bug?** Please help us fix it by thoroughly documenting it and [filing an issue](https://github.com/Azure/azure-kusto-python/issues/new).

## Looking for SDKs for other languages/platforms?
- [Node](https://github.com/azure/azure-kusto-node)
- [Java](https://github.com/azure/azure-kusto-java)
- [.NET](https://learn.microsoft.com/kusto/api/netfx/about-the-sdk)
- [Go](https://github.com/Azure/azure-kusto-go)

# Contribute

We gladly accept community contributions.

- Issues: Please report bugs using the Issues section of GitHub
- Forums: Interact with the development teams on StackOverflow or the Microsoft Azure Forums
- Source Code Contributions: If you would like to become an active contributor to this project please follow the instructions provided in [Contributing.md](CONTRIBUTING.md).

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

For general suggestions about Microsoft Azure please use our [UserVoice forum](https://feedback.azure.com/forums/34192--general-feedback).
