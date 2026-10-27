# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License

import asyncio
import csv
import dataclasses
import json
import os
import platform
import random
from datetime import datetime
from typing import Optional, ClassVar, Callable
from urllib.parse import urljoin

import pytest
from azure.identity import DefaultAzureCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._cloud_settings import CloudSettings, DEFAULT_DEV_KUSTO_SERVICE_RESOURCE_ID
from azure.kusto.data._models import WellKnownDataSet
from azure.kusto.data.aio import KustoClient as AsyncKustoClient
from azure.kusto.data.env_utils import get_env, set_env, prepare_app_key_auth
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.kusto_trusted_endpoints import MatchRule, well_known_kusto_endpoints
from azure.kusto.data.streaming_response import FrameType


@pytest.fixture(scope="module")
def event_loop_policy(request):
    if platform.system() == "Windows":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture(params=["ManagedStreaming", "NormalClient"])
def is_managed_streaming(request):
    return request.param == "ManagedStreaming"


class TestE2E:
    """A class to define mappings to deft table."""

    input_folder_path: ClassVar[str]
    streaming_test_table: ClassVar[str]
    streaming_test_table_query: ClassVar[str]
    ai_test_table_cmd: ClassVar[str]
    test_streaming_data_csv_raw: ClassVar[str]
    engine_cs: ClassVar[Optional[str]]
    ai_engine_cs: ClassVar[Optional[str]]
    test_db: ClassVar[Optional[str]]
    ai_test_db: ClassVar[Optional[str]]
    cred: ClassVar[Callable[[], DefaultAzureCredential]]
    async_cred: ClassVar[Callable[[], DefaultAzureCredential]]

    CHUNK_SIZE = 1024

    @staticmethod
    def table_json_mapping_reference():
        """A method to get json mappings reference to test table."""
        return """'['
                    '    { "column" : "rownumber", "datatype" : "int", "Properties":{"Path":"$.rownumber"}},'
                    '    { "column" : "rowguid", "datatype" : "string", "Properties":{"Path":"$.rowguid"}},'
                    '    { "column" : "xdouble", "datatype" : "real", "Properties":{"Path":"$.xdouble"}},'
                    '    { "column" : "xfloat", "datatype" : "real", "Properties":{"Path":"$.xfloat"}},'
                    '    { "column" : "xbool", "datatype" : "bool", "Properties":{"Path":"$.xbool"}},'
                    '    { "column" : "xint16", "datatype" : "int", "Properties":{"Path":"$.xint16"}},'
                    '    { "column" : "xint32", "datatype" : "int", "Properties":{"Path":"$.xint32"}},'
                    '    { "column" : "xint64", "datatype" : "long", "Properties":{"Path":"$.xint64"}},'
                    '    { "column" : "xuint8", "datatype" : "long", "Properties":{"Path":"$.xuint8"}},'
                    '    { "column" : "xuint16", "datatype" : "long", "Properties":{"Path":"$.xuint16"}},'
                    '    { "column" : "xuint32", "datatype" : "long", "Properties":{"Path":"$.xuint32"}},'
                    '    { "column" : "xuint64", "datatype" : "long", "Properties":{"Path":"$.xuint64"}},'
                    '    { "column" : "xdate", "datatype" : "datetime", "Properties":{"Path":"$.xdate"}},'
                    '    { "column" : "xsmalltext", "datatype" : "string", "Properties":{"Path":"$.xsmalltext"}},'
                    '    { "column" : "xtext", "datatype" : "string", "Properties":{"Path":"$.xtext"}},'
                    '    { "column" : "xnumberAsText", "datatype" : "string", "Properties":{"Path":"$.rowguid"}},'
                    '    { "column" : "xtime", "datatype" : "timespan", "Properties":{"Path":"$.xtime"}},'
                    '    { "column" : "xtextWithNulls", "datatype" : "string", "Properties":{"Path":"$.xtextWithNulls"}},'
                    '    { "column" : "xdynamicWithNulls", "datatype" : "dynamic", "Properties":{"Path":"$.xdynamicWithNulls"}},'
                    ']'"""

    @staticmethod
    def application_insights_tables():
        """A method to get the tables of an application insights instance"""
        return [
            "availabilityResults",
            "browserTimings",
            "customEvents",
            "customMetrics",
            "dependencies",
            "exceptions",
            "pageViews",
            "performanceCounters",
            "requests",
            "resources",
            "traces",
        ]

    @staticmethod
    def get_file_path() -> str:
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-data", "tests", "input"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)
        return os.path.join(current_dir, *missing_path_parts)

    @classmethod
    def engine_kcsb_from_env(cls, app_insights=False, is_async=False) -> KustoConnectionStringBuilder:
        engine = cls.engine_cs if not app_insights else cls.ai_engine_cs
        return KustoConnectionStringBuilder.with_azure_token_credential(
            engine,
            credential=cls.cred() if not is_async else cls.async_cred(),
        )

    @classmethod
    def setup_class(cls):
        cls.engine_cs = get_env("ENGINE_CONNECTION_STRING")
        cls.ai_engine_cs = get_env("APPLICATION_INSIGHTS_ENGINE_CONNECTION_STRING", optional=True)

        # Called to set the env variables for the default azure credentials
        prepare_app_key_auth(optional=True)

        set_env("AZURE_AUTHORITY_HOST", "login.microsoftonline.com")

        cls.test_db = get_env("TEST_DATABASE")
        cls.ai_test_db = get_env("APPLICATION_INSIGHTS_TEST_DATABASE", optional=True)  # name of e2e database could be changed

        cls.cred = lambda: DefaultAzureCredential(exclude_interactive_browser_credential=False)
        # Async credentials don't support interactive browser authentication for now, so until they do, we'll use the sync default credential for async tests
        cls.async_cred = lambda: DefaultAzureCredential(exclude_interactive_browser_credential=False)

        cls.input_folder_path = cls.get_file_path()

        with open(os.path.join(cls.input_folder_path, "big_source.csv")) as f:
            cls.test_streaming_data_csv_raw = f.read()
            reader = csv.reader(cls.test_streaming_data_csv_raw.splitlines())
            cls.test_streaming_data = sorted(reader, key=lambda x: (float(x[0]), x[3]))  # sort by AvgTicketPrice, Dest

        # Init clients
        cls.streaming_test_table = cls.create_streaming_table()
        cls.streaming_test_table_query = cls.streaming_test_table + "| order by AvgTicketPrice asc, Dest asc"

        cls.ai_test_table_cmd = ".show tables"

    @classmethod
    def create_streaming_table(cls):
        with cls.get_client() as client:
            table_name = f"StreamingTestTable{datetime.now():%Y%m%d%H%M%S}{random.randint(0, 10000)}"
            streaming_table_format = (
                "AvgTicketPrice:real,Cancelled:bool,Carrier:string,Dest:string,DestAirportID:string,DestCityName:string,"
                "DestCountry:string,DestLocation:dynamic,DestRegion:string,DestWeather:string,DistanceKilometers:real,"
                "DistanceMiles:real,FlightDelay:bool,FlightDelayMin:long,FlightDelayType:string,FlightNum:string,FlightTimeHour:real,"
                "FlightTimeMin:real,Origin:string,OriginAirportID:string,OriginCityName:string,OriginCountry:string,"
                "OriginLocation:dynamic,OriginRegion:string,OriginWeather:string,dayOfWeek:int,timestamp:datetime"
            )
            client.execute_mgmt(cls.test_db, f".create table {table_name} ({streaming_table_format})")
            client.execute_mgmt(cls.test_db, f".ingest inline into table {table_name} <| {cls.test_streaming_data_csv_raw}")

            return table_name

    @classmethod
    def teardown_class(cls):
        with cls.get_client() as client:
            client.execute_mgmt(cls.test_db, ".drop table {}".format(cls.streaming_test_table))

    @classmethod
    async def get_async_client(cls, app_insights=False) -> AsyncKustoClient:
        return AsyncKustoClient(cls.engine_kcsb_from_env(app_insights, is_async=True))

    @classmethod
    def get_client(cls, app_insights=False) -> KustoClient:
        kcsb = cls.engine_kcsb_from_env(app_insights, is_async=False)
        kcsb.user_name_for_tracing = "E2E_Test_ø"
        return KustoClient(kcsb)

    @staticmethod
    def normalize_row(row):
        result = []
        for r in row:
            if isinstance(r, bool):
                result.append(str(r).lower())
            elif isinstance(r, datetime):
                result.append(r.strftime("%Y-%m-%dT%H:%M:%SZ"))
            elif isinstance(r, float):
                result.append(r)
            elif isinstance(r, dict):
                result.append(json.dumps(r).replace(" ", ""))
            else:
                result.append(str(r))
        return result

    # assertions

    def test_streaming_query(self):
        with self.get_client() as client:
            result = client.execute_streaming_query(self.test_db, self.streaming_test_table_query + ";" + self.streaming_test_table_query)
            counter = 0

            result.set_skip_incomplete_tables(True)
            for primary in result.iter_primary_results():
                counter += 1
                for expected_row in self.test_streaming_data:
                    actual_row = self.normalize_row(next(primary).to_list())
                    for i in range(len(expected_row)):
                        if isinstance(actual_row[i], float):
                            assert pytest.approx(float(expected_row[i]), 0.1) == actual_row[i]
                        else:
                            assert expected_row[i] == actual_row[i]

            assert counter == 2

            assert result.finished
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    @pytest.mark.asyncio
    async def test_streaming_query_async(self):
        async with await self.get_async_client() as client:
            result = await client.execute_streaming_query(self.test_db, self.streaming_test_table_query + ";" + self.streaming_test_table_query)
            counter = 0

            result.set_skip_incomplete_tables(True)
            async for primary in result.iter_primary_results():
                counter += 1
                streaming_data_iter = iter(self.test_streaming_data)
                async for row in primary:
                    expected_row = next(streaming_data_iter, None)
                    if expected_row is None:
                        break

                    actual_row = self.normalize_row(row.to_list())
                    for i in range(len(expected_row)):
                        if isinstance(actual_row[i], float):
                            assert pytest.approx(float(expected_row[i]), 0.1) == actual_row[i]
                        else:
                            assert expected_row[i] == actual_row[i]

            assert counter == 2
            assert result.finished
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    def test_streaming_query_internal(self):
        with self.get_client() as client:
            frames = client._execute_streaming_query_parsed(self.test_db, self.streaming_test_table_query)

            initial_frame = next(frames)
            expected_initial_frame = {
                "FrameType": FrameType.DataSetHeader,
                "IsProgressive": False,
                "Version": "v2.0",
            }
            assert initial_frame == expected_initial_frame
            query_props = next(frames)
            assert query_props["FrameType"] == FrameType.DataTable
            assert query_props["TableKind"] == WellKnownDataSet.QueryProperties.value
            assert isinstance(query_props["Columns"], list)
            assert isinstance(query_props["Rows"], list)
            assert len(query_props["Rows"][0]) == len(query_props["Columns"])

            primary_result = next(frames)
            assert primary_result["FrameType"] == FrameType.DataTable
            assert primary_result["TableKind"] == WellKnownDataSet.PrimaryResult.value
            assert isinstance(primary_result["Columns"], list)
            assert not isinstance(primary_result["Rows"], list)

            row = next(primary_result["Rows"])
            assert len(row) == len(primary_result["Columns"])

    @pytest.mark.asyncio
    async def test_streaming_query_internal_async(self):
        async with await self.get_async_client() as client:
            frames = await client._execute_streaming_query_parsed(self.test_db, self.streaming_test_table_query)
            frames.__aiter__()
            initial_frame = await frames.__anext__()
            expected_initial_frame = {
                "FrameType": FrameType.DataSetHeader,
                "IsProgressive": False,
                "Version": "v2.0",
            }
            assert initial_frame == expected_initial_frame
            query_props = await frames.__anext__()
            assert query_props["FrameType"] == FrameType.DataTable
            assert query_props["TableKind"] == WellKnownDataSet.QueryProperties.value
            assert isinstance(query_props["Columns"], list)
            assert isinstance(query_props["Rows"], list)
            assert len(query_props["Rows"][0]) == len(query_props["Columns"])

            primary_result = await frames.__anext__()
            assert primary_result["FrameType"] == FrameType.DataTable
            assert primary_result["TableKind"] == WellKnownDataSet.PrimaryResult.value
            assert isinstance(primary_result["Columns"], list)
            assert not isinstance(primary_result["Rows"], list)

            row = await primary_result["Rows"].__anext__()
            assert len(row) == len(primary_result["Columns"])

    # Arrow

    ARROW_ALL_TYPES_QUERY = (
        "print b=true, i=int(5), l=long(5000000000), r=real(1.5), d=decimal(1.25), s='text', g=guid(6f3c1072-2739-461c-8aa7-3cfc8ff528a8), "
        "dt=datetime(2021-12-22T11:43:00.1234567Z), ts=timespan(1.02:03:04.5), dyn=dynamic({'a':[1,2]}), "
        "nb=bool(null), ni=int(null), nl=long(null), nr=real(null), nd=decimal(null), ng=guid(null), ndt=datetime(null), nts=timespan(null), ndyn=dynamic(null), "
        "max_dt=datetime(9999-12-31T23:59:59.9999999Z), min_dt=datetime(0001-01-01), nan=real(nan), inf=real(+inf), ninf=real(-inf)"
    )

    @staticmethod
    def _assert_arrow_all_types(table, time_unit="us"):
        import pyarrow as pa

        assert table.num_rows == 1
        timestamp, duration = pa.timestamp(time_unit, "UTC"), pa.duration(time_unit)
        expected_types = {"b": pa.bool_(), "i": pa.int32(), "l": pa.int64(), "r": pa.float64(), "d": pa.float64(), "s": pa.string(), "g": pa.string()}
        expected_types.update({"dt": timestamp, "ts": duration, "dyn": pa.string(), "max_dt": timestamp, "min_dt": timestamp})
        expected_types.update({"nan": pa.float64(), "inf": pa.float64(), "ninf": pa.float64()})
        for name in ("b", "i", "l", "r", "d", "g", "dt", "ts", "dyn"):
            expected_types["n" + name] = expected_types[name]
        assert {field.name: field.type for field in table.schema} == expected_types

        row = {name: table.column(name).to_pylist()[0] for name in ("b", "i", "l", "r", "d", "s", "g", "dyn", "nan", "inf", "ninf")}
        assert row == {
            "b": True,
            "i": 5,
            "l": 5000000000,
            "r": 1.5,
            "d": 1.25,
            "s": "text",
            "g": "6f3c1072-2739-461c-8aa7-3cfc8ff528a8",
            "dyn": '{"a":[1,2]}',
            "nan": row["nan"],
            "inf": float("inf"),
            "ninf": float("-inf"),
        }
        assert row["nan"] != row["nan"]
        for name in ("nb", "ni", "nl", "nr", "nd", "ng", "ndt", "nts", "ndyn"):
            assert table.column(name).null_count == 1, name

        as_int = lambda name: table.column(name).cast(pa.int64()).to_pylist()[0]
        dt_nanoseconds = 1640173380 * 10**9 + 123456700  # 2021-12-22T11:43:00.1234567Z
        ts_nanoseconds = 93784500000000  # 1.02:03:04.5
        if time_unit == "ns":
            assert (as_int("dt"), as_int("ts")) == (dt_nanoseconds, ts_nanoseconds)
            # The earliest and the latest datetimes of Kusto don't fit in nanoseconds
            assert table.column("max_dt").null_count == 1 and table.column("min_dt").null_count == 1
        else:
            assert (as_int("dt"), as_int("ts")) == (dt_nanoseconds // 1000, ts_nanoseconds // 1000)
            assert as_int("max_dt") == 253402300799 * 10**6 + 999999
            assert as_int("min_dt") == -62135596800 * 10**6

    def test_arrow_all_types(self):
        pytest.importorskip("pyarrow")
        from azure.kusto.data.arrow_helpers import arrow_table_from_result_table

        with self.get_client() as client:
            response = client.execute_query(self.test_db, self.ARROW_ALL_TYPES_QUERY)
            self._assert_arrow_all_types(arrow_table_from_result_table(response.primary_results[0]))
            self._assert_arrow_all_types(arrow_table_from_result_table(response.primary_results[0], time_unit="ns"), "ns")

    def test_arrow_streaming(self):
        pytest.importorskip("pyarrow")
        from azure.kusto.data.arrow_helpers import arrow_reader_from_result_table, arrow_table_from_result_table

        with self.get_client() as client:
            result = client.execute_streaming_query(self.test_db, self.streaming_test_table_query)
            reader = arrow_reader_from_result_table(next(result.iter_primary_results()), batch_size=100)
            streamed = reader.read_all()
            assert next(result.iter_primary_results(), None) is None
            assert result.errors_count == 0

            not_streamed = arrow_table_from_result_table(client.execute_query(self.test_db, self.streaming_test_table_query).primary_results[0])
            assert streamed.equals(not_streamed)
            assert streamed.num_rows == len(self.test_streaming_data)
            assert streamed.schema.field("AvgTicketPrice").metadata == {b"kusto.type": b"real"}

    @pytest.mark.asyncio
    async def test_arrow_async(self):
        pytest.importorskip("pyarrow")
        from azure.kusto.data.aio.arrow_helpers import arrow_record_batches_from_streaming_result_table, arrow_table_from_streaming_result_table
        from azure.kusto.data.arrow_helpers import arrow_table_from_result_table

        async with await self.get_async_client() as client:
            response = await client.execute_query(self.test_db, self.ARROW_ALL_TYPES_QUERY)
            self._assert_arrow_all_types(arrow_table_from_result_table(response.primary_results[0]))

            expected = arrow_table_from_result_table((await client.execute_query(self.test_db, self.streaming_test_table_query)).primary_results[0])

            result = await client.execute_streaming_query(self.test_db, self.streaming_test_table_query)
            streamed = await arrow_table_from_streaming_result_table(await result.iter_primary_results().__anext__(), batch_size=100)
            assert streamed.equals(expected)

            result = await client.execute_streaming_query(self.test_db, self.streaming_test_table_query)
            batches = [
                batch async for batch in arrow_record_batches_from_streaming_result_table(await result.iter_primary_results().__anext__(), batch_size=100)
            ]
            assert sum(batch.num_rows for batch in batches) == len(self.test_streaming_data)

    def test_arrow_control_command(self):
        pytest.importorskip("pyarrow")
        from azure.kusto.data.arrow_helpers import arrow_tables_from_response

        with self.get_client() as client:
            tables = arrow_tables_from_response(client.execute_mgmt(self.test_db, ".show version"))
            assert len(tables) == 1
            assert tables[0].num_rows == 1
            assert "BuildVersion" in tables[0].schema.names

    def test_log_analytics_query(self):
        if not self.ai_engine_cs:
            pytest.skip("No application insights connection string provided")
        with self.get_client(True) as client:
            result = client.execute_mgmt(self.ai_test_db, self.ai_test_table_cmd)
            assert len(result.primary_results) == 1

            actual_table_names = {row["TableName"] for primary in result.primary_results for row in primary.rows}
            expected_table_names = set(self.application_insights_tables())
            missing_table_names = expected_table_names - actual_table_names

            assert not missing_table_names, f"Missing expected Application Insights tables: {sorted(missing_table_names)}"
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    @pytest.mark.asyncio
    async def test_log_analytics_query_async(self):
        if not self.ai_engine_cs:
            pytest.skip("No application insights connection string provided")
        async with await self.get_async_client(True) as ai_client:
            result = await ai_client.execute_mgmt(self.ai_test_db, self.ai_test_table_cmd)
            assert len(result.primary_results) == 1

            actual_table_names = {row["TableName"] for primary in result.primary_results for row in primary.rows}
            expected_table_names = set(self.application_insights_tables())
            missing_table_names = expected_table_names - actual_table_names

            assert not missing_table_names, f"Missing expected Application Insights tables: {sorted(missing_table_names)}"
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    def test_cloud_info(self):
        if ".dev." in self.engine_cs:
            pytest.skip("This test is not relevant for dev clusters")
        cloud_info = CloudSettings.get_cloud_info_for_cluster(self.engine_cs)
        assert cloud_info is not CloudSettings.DEFAULT_CLOUD
        assert cloud_info == CloudSettings.DEFAULT_CLOUD
        assert cloud_info is CloudSettings.get_cloud_info_for_cluster(self.engine_cs)
        assert cloud_info == CloudSettings.get_cloud_info_for_cluster(urljoin(self.engine_cs, "/test1/test2/test3"))

    def test_cloud_info_404(self):
        pytest.skip("This test is currently wrong - until all cluster are updated to the redirect uri, this test will fail")
        cloud_info = CloudSettings.get_cloud_info_for_cluster("https://statusreturner.azurewebsites.net/404/test")
        default_dev_cloud = dataclasses.replace(CloudSettings.DEFAULT_CLOUD, kusto_service_resource_id=DEFAULT_DEV_KUSTO_SERVICE_RESOURCE_ID)
        assert cloud_info == CloudSettings.DEFAULT_CLOUD or cloud_info == default_dev_cloud

    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    def test_no_redirects_fail_in_cloud(self, code):
        pytest.skip("This test is currently not supported as it relies on the URI path witch we now ignore")
        with KustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(f"https://statusreturner.azurewebsites.net/{code}/nocloud", self.__class__.cred())
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                client.execute("db", "table")
            assert ex.value.http_response.status_code == code

    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    def test_no_redirects_fail_in_client(self, code):
        well_known_kusto_endpoints.add_trusted_hosts([MatchRule("statusreturner.azurewebsites.net", False)], False)
        with KustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(f"https://statusreturner.azurewebsites.net/{code}/segment", self.__class__.cred())
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                client.execute("db", "table")
            assert ex.value.http_response.status_code == code

    @pytest.mark.asyncio
    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    async def test_no_redirects_fail_in_cloud_async(self, code):
        pytest.skip("This test is currently not supported as it relies on the URI path witch we now ignore")
        async with AsyncKustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(f"https://statusreturner.azurewebsites.net/{code}/nocloud", self.__class__.async_cred())
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                await client.execute("db", "table")
            assert ex.value.http_response.status_code == code

    @pytest.mark.asyncio
    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    async def test_no_redirects_fail_in_client_async(self, code):
        well_known_kusto_endpoints.add_trusted_hosts([MatchRule("statusreturner.azurewebsites.net", False)], False)
        async with AsyncKustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(f"https://statusreturner.azurewebsites.net/{code}/segment", self.__class__.async_cred())
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                await client.execute("db", "table")
            assert ex.value.http_response.status == code

    def test_semantic_error(self):
        with self.get_client() as client:
            with pytest.raises(KustoServiceError) as ex:
                client.execute(self.test_db, "Nonexistent")
            assert ex.value.is_semantic_error()

    @pytest.mark.asyncio
    async def test_semantic_error_async(self):
        async with await self.get_async_client() as client:
            with pytest.raises(KustoServiceError) as ex:
                await client.execute(self.test_db, "Nonexistent")
            assert ex.value.is_semantic_error()
