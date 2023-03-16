# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime
from decimal import Decimal
import pytest
from unittest import mock

import pandas as pd

from util.jdbc.jdbc_input_manager import JDBCInputManager
from util.jdbc.jdbc_input_manager_interface import (
    JDBCInputManagerException,
    JDBCInputManagerInterface,
)
from util.jdbc.engines.oracle_input_manager import OracleInputManager


ALCHEMY_DB = mock.MagicMock()


def test_input_manager_init():
    for db_type in ["oracle", "mysql"]:
        mgr = JDBCInputManager.create(db_type, ALCHEMY_DB)
        assert isinstance(mgr, JDBCInputManagerInterface)


def test_oracle_enclose_identifier():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert mgr._enclose_identifier("a", "'") == "'a'"
    assert mgr._enclose_identifier("a") == '"a"'
    assert mgr._enclose_identifier("a", '"') == '"a"'
    assert mgr._enclose_identifier("A", '"') == '"A"'


def test_mysql_enclose_identifier():
    mgr = JDBCInputManager.create("mysql", ALCHEMY_DB)
    assert mgr._enclose_identifier("a", "'") == "'a'"
    assert mgr._enclose_identifier("a") == "`a`"
    assert mgr._enclose_identifier("a", "`") == "`a`"
    assert mgr._enclose_identifier("A", "`") == "`A`"


def test__filter_table_list():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    table_list = ["table1", "TABLE2", "Table3"]
    assert mgr._filter_table_list(table_list, ["TABLE1", "table2", "table4"]) == [
        "table1",
        "TABLE2",
    ]
    assert mgr._filter_table_list(table_list, None) == table_list
    assert mgr._filter_table_list(table_list, []) == table_list
    # table_list can be list of tuples if fed directly from SQL output.
    assert mgr._filter_table_list([(_,) for _ in table_list], ["TABLE1"]) == ["table1"]
    assert mgr._filter_table_list([(_,) for _ in table_list], []) == table_list


def test_oracle_qualified_name():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=False) == "SCHEMA1.TABLE1"
    assert (
        mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=True) == '"SCHEMA1"."TABLE1"'
    )


def test_mysql_qualified_name():
    mgr = JDBCInputManager.create("mysql", ALCHEMY_DB)
    assert mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=False) == "SCHEMA1.TABLE1"
    assert (
        mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=True) == "`SCHEMA1`.`TABLE1`"
    )


def test_table_list():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    table_list = ["table1", "TABLE2"]
    mgr.set_table_list(table_list)
    assert mgr.get_table_list() == table_list


def test_get_table_list_with_counts():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    mgr._get_table_count = mock.MagicMock(return_value=42)
    table_list = ["table1", "table2"]
    mgr.set_table_list(table_list)
    assert mgr.get_table_list_with_counts() == [42, 42]


def test_read_partitioning_df():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    table_list = ["table1", "table2", "table3"]
    mgr.set_table_list(table_list)
    read_partitioning_dict = {
        "table2": {
            "partitionColumn": "ID",
            "numPartitions": 15,
            "lowerBound": 1,
            "upperBound": 444,
        }
    }

    df = mgr.read_partitioning_df(read_partitioning_dict)

    assert isinstance(df, pd.DataFrame)
    assert len(df["table"]) == 3
    assert "table1" in list(df["table"])
    assert "table2" in list(df["table"])
    assert "table3" in list(df["table"])
    table2_row = list(df["table"]).index("table2")
    assert list(df["partition_column"])[table2_row] == "ID"
    assert list(df["num_partitions"])[table2_row] == 15
    assert list(df["lower_bound"])[table2_row] == 1
    assert list(df["upper_bound"])[table2_row] == 444


def test__get_count_sql():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert isinstance(mgr._get_count_sql("TABLE"), str)


def test__get_max_sql():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert isinstance(mgr._get_max_sql("TABLE", "COLUMN"), str)


def test__get_min_sql():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert isinstance(mgr._get_min_sql("TABLE", "COLUMN"), str)


def test__normalise_oracle_data_type():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    for oracle_type, normalised_type in [
        ("DATE", "DATE"),
        ("TIMESTAMP(0)", "TIMESTAMP"),
        ("TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"),
        ("TIMESTAMP(6) WITH LOCAL TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"),
        ("INTERVAL DAY(5) TO SECOND(1)", "INTERVAL DAY TO SECOND"),
    ]:
        assert mgr._normalise_oracle_data_type(oracle_type) == normalised_type


def test__read_partitioning_num_partitions():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    # Numeric ranges
    for lowerbound, upperbound, stride, expected_partitions in [
        [1, 1, 10, 1],
        [1, 100, 10, 10],
        [float(1), float(100), float(10), 10],
        [Decimal(1), Decimal(100), Decimal(10), 10],
        [int(1), float(100), Decimal(10), 10],
        [
            Decimal(1),
            Decimal(9_999_999_999_999_999_999),
            Decimal(1_000_000_000_000_000_000),
            10,
        ],
        [1, 105, 10, 11],
        [-99, 1, 10, 10],
    ]:
        assert (
            mgr._read_partitioning_num_partitions(lowerbound, upperbound, stride)
            == expected_partitions
        )

    # Unsupported boundary inputs
    for lowerbound, upperbound, stride, expected_partitions in [
        # Datetime ranges are currently unsupported
        [datetime(2020, 1, 1), datetime(2020, 1, 20), 2, None],
        # Upperbound < lowerbound
        [100, 1, 10, None],
    ]:
        with pytest.raises(JDBCInputManagerException):
            assert (
                mgr._read_partitioning_num_partitions(lowerbound, upperbound, stride)
                == expected_partitions
            )
