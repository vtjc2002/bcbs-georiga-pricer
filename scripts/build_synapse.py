"""Build deployment artifacts without Azure access or third-party dependencies."""

import argparse
import hashlib
import json
import re
import zipfile
from pathlib import Path


def expression(value):
    return {"value": value, "type": "Expression"}


def notebook_activity(name, phase, run_id=""):
    return {
        "name": name, "type": "SynapseNotebook",
        "policy": {"timeout": "0.12:00:00", "retry": 0, "secureOutput": True, "secureInput": True},
        "typeProperties": {
            "notebook": {"referenceName": "GeorgiaPrices", "type": "NotebookReference"},
            "parameters": {
                "phase": {"value": phase, "type": "string"},
                "run_id": {"value": run_id, "type": "string"},
            },
        },
    }


def build(root, output, workspace, private, serving, pool="etl", database="prices"):
    for account in (private, serving):
        if not re.fullmatch(r"[a-z0-9]{3,24}", account):
            raise ValueError("Invalid storage account name")
    for name in (workspace, pool, database):
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{0,62}", name):
            raise ValueError("Invalid workspace, Spark pool, or database name")
    root, output = Path(root), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    bundle = output / "etl.zip"
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted((root / "etl").glob("*.py")):
            info = zipfile.ZipInfo("etl/" + path.name, date_time=(2020, 1, 1, 0, 0, 0))
            archive.writestr(info, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED)
    bundle_hash = hashlib.sha256(bundle.read_bytes()).hexdigest()
    bundle_path = f"code/{bundle_hash}/etl.zip"
    config = json.loads((root / "metadata" / "source.json").read_text())
    code = (
        "import json\nimport logging\nfrom notebookutils import mssparkutils\n"
        "logging.basicConfig(level=logging.INFO)\n"
        f"spark.sparkContext.addPyFile('abfss://workspace@{private}.dfs.core.windows.net/{bundle_path}')\n"
        "from etl.synapse_job import execute\n"
        f"config = json.loads({json.dumps(json.dumps(config))})\n"
        f"result = execute(phase, '{private}', '{serving}', config, run_id=run_id, spark=spark)\n"
        "mssparkutils.notebook.exit(json.dumps(result))\n"
    )
    notebook = {
        "nbformat": 4, "nbformat_minor": 2,
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark", "language": "python", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
        },
        "cells": [
            {"cell_type": "code", "execution_count": None,
             "metadata": {"tags": ["parameters"]}, "outputs": [],
             "source": ["phase = 'ingest'\n", "run_id = ''\n"]},
            {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
             "source": code.splitlines(keepends=True)},
        ],
    }
    ingest = notebook_activity("Ingest", "ingest")
    publish = notebook_activity(
        "Publish", "publish",
        expression("@json(activity('Ingest').output.status.Output.result.exitValue).run_id"))
    publish["dependsOn"] = [{"activity": "Transform", "dependencyConditions": ["Succeeded"]}]
    retain = notebook_activity("Retain", "retain")
    retain["dependsOn"] = [{"activity": "Publish", "dependencyConditions": ["Succeeded"]}]
    transform = {
        "name": "Transform", "type": "Script",
        "linkedServiceName": {"referenceName": "ServerlessPrices", "type": "LinkedServiceReference"},
        "policy": {"timeout": "0.04:00:00", "retry": 0, "secureInput": True, "secureOutput": True},
        "typeProperties": {
            "scripts": expression("@json(activity('Ingest').output.status.Output.result.exitValue).scripts"),
            "scriptBlockExecutionTimeout": "02:00:00",
        },
    }
    pipeline = {
        "properties": {
            "concurrency": 1,
            "activities": [
                ingest,
                {"name": "IfChanged", "type": "IfCondition",
                 "dependsOn": [{"activity": "Ingest", "dependencyConditions": ["Succeeded"]}],
                 "typeProperties": {
                     "expression": expression(
                         "@not(json(activity('Ingest').output.status.Output.result.exitValue).skip)"),
                     "ifTrueActivities": [transform, publish, retain],
                     "ifFalseActivities": [],
                 }},
            ],
        },
    }
    linked = {"properties": {"type": "AzureSqlDW", "typeProperties": {
        "connectionString": f"Data Source=tcp:{workspace}-ondemand.sql.azuresynapse.net,1433;"
                            f"Initial Catalog={database};Encrypt=True;TrustServerCertificate=False;",
        "authenticationType": "SystemAssignedManagedIdentity",
    }}}
    trigger = {"properties": {
        "type": "ScheduleTrigger",
        "pipelines": [{"pipelineReference": {"referenceName": "GeorgiaMonthly", "type": "PipelineReference"},
                       "parameters": {}}],
        "typeProperties": {"recurrence": {
            "frequency": "Month", "interval": 1, "timeZone": "UTC",
            "schedule": {"monthDays": [3, 10, 17], "hours": [6], "minutes": [0]},
        }},
    }}
    bootstrap = f"""-- Run as the configured Entra SQL administrator on the serverless endpoint.
CREATE DATABASE [{database}] COLLATE Latin1_General_100_BIN2_UTF8;
GO
USE [{database}];
GO
CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity WITH IDENTITY='Managed Identity';
GO
CREATE USER [{workspace}] FROM EXTERNAL PROVIDER;
ALTER ROLE db_ddladmin ADD MEMBER [{workspace}];
ALTER ROLE db_datareader ADD MEMBER [{workspace}];
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [{workspace}];
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::WorkspaceIdentity TO [{workspace}];
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [{workspace}];
GRANT ALTER ANY EXTERNAL FILE FORMAT TO [{workspace}];
GO
-- Whole-TB safeguards, not spending caps. Do not terminate an in-flight query.
USE master;
GO
EXEC sp_set_data_processed_limit @type=N'daily', @limit_tb=1;
EXEC sp_set_data_processed_limit @type=N'monthly', @limit_tb=2;
"""
    files = {"GeorgiaPrices.ipynb": notebook, "GeorgiaMonthly.json": pipeline,
             "ServerlessPrices.json": linked, "GeorgiaSchedule.json": trigger,
             "bundle.json": {"blob": bundle_path, "sha256": bundle_hash}}
    for name, value in files.items():
        (output / name).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    (output / "bootstrap.sql").write_text(bootstrap, encoding="utf-8")
    return files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("workspace", "private-account", "serving-account"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--pool", default="etl")
    parser.add_argument("--database", default="prices")
    parser.add_argument("--output", default=".local/synapse")
    args = parser.parse_args()
    build(Path(__file__).resolve().parents[1], args.output, args.workspace,
          args.private_account, args.serving_account, args.pool, args.database)
