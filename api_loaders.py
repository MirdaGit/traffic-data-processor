import pandas as pd
import importlib
import logging
import numpy as np
import os
from abc import ABC, abstractmethod
from geopandas import GeoDataFrame
from pathlib import Path


class BaseLoader(ABC):
    """
        Abstract loader class which serves as a base
        for all other loaders.
    """

    def __init__(self, config: dict, api_config: dict):
        """
            BaseLoader constructor

        Args:
            config (dict): Script configuration
            api_config (dict): API configuration
        """
        self.config = config
        self.api_config = api_config

    @abstractmethod
    def store_data(self, in_df):
        """
            Store extracted data

        Args:
            in_df (DataFrame): Input dataframe
        """
        raise NotImplementedError


class ArcpyLoader(BaseLoader):
    """
        Loader which uses ArcPy methods to store data.
    """

    def __init__(self, config: dict, api_config: dict):
        """
            ArcpyLoader constructor. Additional ArcGIS modules need
            to be imported for spatial arcpy methods to function.
            Direct arcpy import in the constructor does not function
            correctly when being called from different part of the
            script. Arcpy imports are provided where needed.

        Args:
            config (dict): Script configuration
            api_config (dict): API configuration
        """

        self.config = config
        self.api_config = api_config
        self.arcpy_config = api_config["arcpy_config"]
        self.workspace = os.path.join(
            self.arcpy_config["gdb_path"], self.arcpy_config["gdb_name"], "")
        self.logger = logging.getLogger(__name__)

        importlib.import_module("arcgis.features", "GeoSeriesAccessor")
        importlib.import_module("arcgis.features", "GeoAccessor")

    def addTableFields(self, arcpy, in_df):
        """
            Add fields which are included in the input dataframe,
            but are not present in the ArcGIS database table.

        Args:
            arcpy: Arcpy module
            in_df (Dataframe): Input dataframe
        """

        # Get current fields of specified ArcGIS table
        table_fields = [field.name for field in arcpy.ListFields(
            self.arcpy_config["entry_name"])]

        # Get field names which are not present in the table, but are present in the dataframe
        fields = [
            field for field in in_df.columns if not field in table_fields and field != "Shape"]

        # Determine data types of these fields and add them to the table
        for field in fields:
            if field in self.arcpy_config["dtypes"]:
                # Get dtype from configuration file
                dtype = self.arcpy_config["dtypes"][field]
            else:
                # Try to determine correct type automatically
                dtype = self.getFieldTypes(in_df[field].dtype)

            arcpy.management.AddField(
                self.arcpy_config["entry_name"], field, dtype)

    def getFieldTypes(self, dtype) -> str:
        """
            Try to automatically map input datatypes
            to their counterparts in ArcGIS database.

        Args:
            dtype: Input data type

        Returns:
            str: ArcGIS database type
        """
        if dtype == np.int64:
            return "DOUBLE"
        elif dtype == np.int32:
            return "LONG"
        elif dtype == np.int16:
            return "SHORT"
        elif dtype == np.int_:
            return "DOUBLE"
        elif dtype == np.float64:
            return "DOUBLE"
        elif dtype == np.float32:
            return "FLOAT"
        elif dtype == np.float_:
            return "DOUBLE"
        else:
            return "TEXT"

    def modifyDatabase(self, arcpy, update_list=[], update_fields=[], update_mask=[], insert_list=[], insert_fields=[]):
        """
            Insert and/or update database with provided data on specified fields.

        Args:
            arcpy: Arcpy module
            update_list (list, optional): List of entries which will be used to update the database. Defaults to [].
            update_fields (list, optional): Fields of the update data. Defaults to [].
            update_mask (list, optional): Mask which determines which entries should be updated. Defaults to [].
            insert_list (list, optional): List of entries which will be inserted into the database. Defaults to [].
            insert_fields (list, optional): Fields of the instert data. Defaults to [].
        """
        self.logger.info(
            f"Inserting {len(insert_list)} new entries, updating {update_mask.count(True)} existing entries")
        versioned = arcpy.Describe(
            self.arcpy_config["entry_name"]).isVersioned

        # Execute database modifications under Editor, so either all operations succeed or all fail
        with arcpy.da.Editor(self.workspace, multiuser_mode=versioned):
            # Update existing data
            if len(update_list) > 0:
                with arcpy.da.UpdateCursor(self.arcpy_config["entry_name"], update_fields) as cursor:
                    for idx, row in enumerate(cursor):
                        # Update only masked entries
                        if update_mask[idx]:
                            cursor.updateRow(update_list[idx])

            # Insert new data
            if len(insert_list) > 0:
                with arcpy.da.InsertCursor(self.arcpy_config["entry_name"], insert_fields) as cursor:
                    for row in insert_list:
                        cursor.insertRow(row)

    def storeFeatureClass(self, arcpy, in_df: GeoDataFrame):
        """
            Process provided spatial data, modify fields of relevant tables
            and update existing and/or insert new entries.

        Args:
            arcpy: Arcpy module
            in_df (GeoDataFrame): Input dataframe containing spatial data
        """

        if arcpy.Exists(self.arcpy_config["entry_name"]):
            # Load current data from the database
            processed_data = self.load_processed_data()

            # Create masks of existing and new entries
            id_df = in_df.astype({self.arcpy_config["id_column"]: "int64"})
            present_id_mask = id_df[self.arcpy_config["id_column"]].isin(
                processed_data[self.arcpy_config["id_column"]])
            update_mask = processed_data[self.arcpy_config["id_column"]].isin(
                id_df[self.arcpy_config["id_column"]]).to_list()

            in_df = in_df.replace({float("nan"): None})
            insert_df = in_df[~present_id_mask]
            update_df = in_df[present_id_mask]

            shared_columns = [
                col for col in update_df.columns if col in processed_data.columns]
            not_shared_columns = [
                col for col in update_df.columns if col not in processed_data.columns]

            # Common ID should always be shared between multiple tables
            not_shared_columns.append(self.arcpy_config["id_column"])

            # Add missing columns to new entries
            for col in processed_data.columns:
                if col not in insert_df.columns:
                    insert_df[col] = None

            update_df = update_df.astype(
                {self.arcpy_config["id_column"]: "int64"})

            # Split update data into existing and new columns
            current_cols_df = update_df[shared_columns]
            new_cols_df = update_df[not_shared_columns]
            update_data = processed_data.copy()

            # Update existing columns
            if len(shared_columns) > 1:
                current_cols_df = current_cols_df.set_index(
                    self.arcpy_config["id_column"])
                update_data = update_data.set_index(
                    self.arcpy_config["id_column"])
                update_data.update(current_cols_df)
                update_data = update_data.reset_index()

            # Add new columns to entries
            if len(not_shared_columns) > 1:
                update_data = update_data.merge(
                    new_cols_df,
                    "left",
                    self.arcpy_config["id_column"])

            if "Shape" in update_data.columns:
                update_data = update_data.drop("Shape", axis=1)

            self.addTableFields(arcpy, in_df)

            # Modify table with new data
            insert_fields = [field.name for field in arcpy.ListFields(
                self.arcpy_config["entry_name"]) if field.type != "OID"]
            update_fields = [field.name for field in arcpy.ListFields(
                self.arcpy_config["entry_name"]) if field.type != "OID" and field.name != "Shape"]
            self.modifyDatabase(
                arcpy,
                update_data.values.tolist(),
                update_fields,
                update_mask,
                insert_df.values.tolist(),
                insert_fields)
        else:
            arcpy.management.CreateFeatureclass(
                os.path.join(self.workspace,
                             self.arcpy_config["dataset_name"]),
                self.arcpy_config["entry_name"],
                "POINT",
                spatial_reference=arcpy.SpatialReference(self.api_config["crs"]))

            self.addTableFields(arcpy, in_df)

            # Insert new data into the table
            insert_list = in_df.values.tolist()
            insert_fields = [field.name for field in arcpy.ListFields(
                self.arcpy_config["entry_name"]) if field.type != "OID"]
            self.modifyDatabase(
                arcpy,
                insert_list=insert_list,
                insert_fields=insert_fields)

    def load_processed_data(self) -> GeoDataFrame:
        """
            Load stored data from the database.

        Returns:
            GeoDataFrame: Data loaded from the database
        """
        arcpy = importlib.import_module("arcpy")
        dataset_workspace = os.path.join(
            self.workspace, self.arcpy_config["dataset_name"], self.arcpy_config["entry_name"])
        if arcpy.Exists(dataset_workspace):
            # Get list of entries
            table_fields = [field.name for field in arcpy.ListFields(
                dataset_workspace) if field.type != "OID"]
            with arcpy.da.SearchCursor(dataset_workspace, table_fields) as cursor:
                data = [row for row in cursor]

            # Convert list of entries to dataframe
            gdf = pd.DataFrame(data, columns=table_fields)
            gdf = gdf.astype({self.arcpy_config["id_column"]: "int64"})
            return gdf

        return pd.DataFrame()

    def storeTable(self, arcpy, in_df: GeoDataFrame):
        """
            Process provided data, modify fields of relevant tables
            and update existing and/or insert new entries.

        Args:
            arcpy: Arcpy module
            in_df (GeoDataFrame): Input dataframe which does not contain spatial data
        """

        if arcpy.Exists(self.arcpy_config["entry_name"]):
            # Load current data from the database
            processed_data = self.load_processed_data()

            # Create masks of existing and new entries
            id_df = in_df.astype({self.arcpy_config["id_column"]: "int64"})
            present_id_mask = id_df[self.arcpy_config["id_column"]].isin(
                processed_data[self.arcpy_config["id_column"]])
            update_mask = processed_data[self.arcpy_config["id_column"]].isin(
                id_df[self.arcpy_config["id_column"]]).to_list()

            update_df = in_df[present_id_mask]
            insert_df = in_df[~present_id_mask]

            shared_columns = [
                col for col in update_df.columns if col in processed_data.columns]
            not_shared_columns = [
                col for col in update_df.columns if col not in processed_data.columns]

            # Common ID should always be shared between multiple tables
            not_shared_columns.append(self.arcpy_config["id_column"])

            # Add missing columns to new entries
            for col in processed_data.columns:
                if col not in insert_df.columns:
                    insert_df[col] = None

            update_df = update_df.astype(
                {self.arcpy_config["id_column"]: "int64"})

            # Split update data into existing and new columns
            current_cols_df = update_df[shared_columns]
            new_cols_df = update_df[not_shared_columns]
            update_data = processed_data.copy()

            # Update existing columns
            if len(shared_columns) > 1:
                # Group entries by ID and add new column to track how many have the same ID
                current_cols_df["idx_duplicates"] = current_cols_df.groupby(
                    self.arcpy_config["id_column"]).cumcount()
                update_data["idx_duplicates"] = update_data.groupby(
                    self.arcpy_config["id_column"]).cumcount()

                # Set indexing to ID and duplicate count
                update_keys = update_data[
                    [self.arcpy_config["id_column"], "idx_duplicates"]]
                new_cols_keys = current_cols_df[
                    [self.arcpy_config["id_column"], "idx_duplicates"]]

                # Find missing entries
                new_cols_keys = new_cols_keys.reset_index()
                intersect_df = new_cols_keys.merge(
                    update_keys, "left",
                    [self.arcpy_config["id_column"],
                     "idx_duplicates"],
                    indicator=True)
                intersect_mask = intersect_df["_merge"] == "left_only"

                # Filter missing entries
                current_cols_df = current_cols_df.reset_index()
                new_df = current_cols_df[intersect_mask]
                current_cols_df = current_cols_df.set_index("index")
                new_df = new_df.set_index("index")
                new_df = new_df.drop(["idx_duplicates"], axis=1)

                # Add missing entries to insert dataframe
                insert_df = pd.concat([insert_df, new_df])

                current_cols_df = current_cols_df.set_index(
                    [self.arcpy_config["id_column"], "idx_duplicates"])
                update_data = update_data.set_index(
                    [self.arcpy_config["id_column"], "idx_duplicates"])

                # Update exisiting entries
                update_data.update(current_cols_df)
                update_data = update_data.reset_index()
                update_data = update_data.drop(["idx_duplicates"], axis=1)

            # Add new columns to entries
            if len(not_shared_columns) > 1:
                update_data = update_data.merge(
                    new_cols_df,
                    "left",
                    self.arcpy_config["id_column"])

            self.addTableFields(arcpy, in_df)

            # Modify table with new data
            table_fields = [field.name for field in arcpy.ListFields(
                self.arcpy_config["entry_name"]) if field.type != "OID"]
            insert_list = insert_df.values.tolist()
            self.modifyDatabase(
                arcpy,
                update_data.values.tolist(),
                table_fields,
                update_mask,
                insert_list,
                table_fields)
        else:
            arcpy.management.CreateTable(
                self.workspace,
                self.arcpy_config["entry_name"])

            self.addTableFields(arcpy, in_df)

            # Insert new data into the table
            insert_list = in_df.values.tolist()
            insert_fields = [field.name for field in arcpy.ListFields(
                self.arcpy_config["entry_name"]) if field.type != "OID"]
            self.modifyDatabase(
                arcpy,
                insert_list=insert_list,
                insert_fields=insert_fields)

    def addTableRelation(self, arcpy):
        """
            Create relation between specified tables.

        Args:
            arcpy: Arcpy module
        """

        relation_orig = self.arcpy_config["entry_relation"]["relation_orig"]
        relation_dest = self.arcpy_config["entry_relation"]["relation_dest"]
        relation_name = self.arcpy_config["entry_relation"]["relation_name"]
        dataset_name = self.arcpy_config["entry_relation"]["dataset_name"]

        orig_table = os.path.join(
            self.arcpy_config["gdb_name"],
            dataset_name,
            relation_orig)
        dest_table = os.path.join(self.arcpy_config["gdb_name"], relation_dest)
        rel_table = os.path.join(
            self.arcpy_config["gdb_name"],
            dataset_name,
            relation_name)

        if not arcpy.Exists(rel_table) and arcpy.Exists(orig_table) and arcpy.Exists(dest_table):
            arcpy.CreateRelationshipClass_management(
                origin_table=orig_table,
                destination_table=dest_table,
                out_relationship_class=rel_table,
                relationship_type="COMPOSITE",
                message_direction="NONE",
                cardinality="ONE_TO_MANY",
                attributed="NONE",
                origin_primary_key=self.arcpy_config["entry_relation"]["key_orig"],
                destination_primary_key=self.arcpy_config["entry_relation"]["key_dest"]
            )

    def store_data(self, in_df: GeoDataFrame):
        """
            Store provided data in specified database tables

        Args:
            in_df (GeoDataFrame): Input dataframe
        """

        arcpy = importlib.import_module("arcpy")

        if not Path(self.workspace).exists():
            # Create file geodatabase
            arcpy.management.CreateFileGDB(
                self.arcpy_config["gdb_path"],
                self.arcpy_config["gdb_name"],
                self.arcpy_config["version"]
            )

        arcpy.env.workspace = self.workspace

        # Create dataset
        if self.arcpy_config["dataset_name"] != "" and not arcpy.Exists(self.arcpy_config["dataset_name"]):
            arcpy.management.CreateFeatureDataset(
                self.workspace,
                self.arcpy_config["dataset_name"],
                arcpy.SpatialReference(self.api_config["crs"])
            )

        if self.arcpy_config["entry_type"] == "featureclass":
            arcpy.env.workspace = os.path.join(
                self.workspace, self.arcpy_config["dataset_name"])
            self.storeFeatureClass(arcpy, in_df)
        elif self.arcpy_config["entry_type"] == "table":
            self.storeTable(arcpy, in_df)
            self.addTableRelation(arcpy)


class GeoPandasLoader(BaseLoader):
    """
        Loader which uses GeoPandas methods to store extracted data.
    """

    def __init__(self, config: dict, api_config: dict):
        """
            GeoPandasLoader constructor

        Args:
            config (dict): Script configuration
            api_config (dict): API configuration
        """

        self.config = config
        self.api_config = api_config
        self.gpd_config = api_config["geopandas_config"]

    def store_data(self, in_gdf: GeoDataFrame):
        """
            Store provided data

        Args:
            in_gdf (GeoDataFrame): Input geodataframe
        """
        if not Path(self.gpd_config["file_path"]).exists():
            in_gdf.to_file(self.gpd_config["file_path"])
