import pandas as pd
import importlib
import geopandas as gpd
import logging
import numpy as np
import os
from abc import ABC, abstractmethod
from pandas import DataFrame
from pathlib import Path


class BaseLoader(ABC):
    """
        Abstract loader class which serves as a base for all other loaders.
    """

    def __init__(self, config: dict, file_config: dict, loader_config: dict, export_config: dict, last_dir: bool):
        """
            BaseLoader constructor.

        Args:
            config (dict): Script configuration
            file_config (dict): Specific file configuration
            loader_config (dict): Selected loader configuration
            export_config (dict): Database storage configuration
            last_dir (bool): Flag for identifying the last directory when multiple are processed
        """

        self.config = config
        self.file_config = file_config
        self.loader_config = loader_config
        self.export_config = export_config
        self.logger = logging.getLogger(__name__)
        self.last_dir = last_dir

    @abstractmethod
    def load_processed_data(self):
        """
            Load stored data from the database.
        """
        raise NotImplementedError

    @abstractmethod
    def store_processed_data(self, merged_data: DataFrame):
        """
            Store processed data in the database.
        """
        raise NotImplementedError


class ArcpyFileLoader(BaseLoader):
    """
        Loader which uses ArcPy methods to store processed data.
    """

    def __init__(self, config: dict, file_config: dict, loader_config: dict, export_config: dict, last_dir: bool):
        """
            ArcpyFileLoader constructor.

        Args:
            config (dict): Script configuration
            file_config (dict): Specific file configuration
            loader_config (dict): Selected loader configuration
            export_config (dict): Database storage configuration
            last_dir (bool): Flag for identifying the last directory when multiple are processed
        """

        self.config = config
        self.file_config = file_config
        self.loader_config = loader_config
        self.export_config = export_config
        self.workspace = os.path.join(
            self.loader_config["gdb_path"], self.loader_config["gdb_name"], "")
        self.logger = logging.getLogger(__name__)
        self.last_dir = last_dir

    def load_processed_data(self) -> DataFrame:
        """
            Load stored data from the database.

        Returns:
            DataFrame: Loaded data from the database.
        """
        arcpy = importlib.import_module("arcpy")
        dataset_workspace = os.path.join(
            self.workspace, self.export_config["dataset_name"], self.export_config["entry_name"])
        if arcpy.Exists(dataset_workspace):
            # Get list of entries
            table_fields = [field.name for field in arcpy.ListFields(
                dataset_workspace) if field.type != "OID"]
            with arcpy.da.SearchCursor(dataset_workspace, table_fields) as cursor:
                data = [row for row in cursor]

            # Convert list of entries to dataframe
            gdf = pd.DataFrame(data, columns=table_fields)
            gdf = gdf.astype({self.file_config["id_column"]: "int64"})
            return gdf

        return pd.DataFrame()

    def addTableRelation(self, arcpy):
        """
            Create relation between specified tables.

        Args:
            arcpy: Arcpy module
        """

        relation_orig = self.export_config["entry_relation"]["relation_orig"]
        relation_dest = self.export_config["entry_relation"]["relation_dest"]
        relation_name = self.export_config["entry_relation"]["relation_name"]
        dataset_name = self.export_config["entry_relation"]["dataset_name"]

        orig_table = os.path.join(
            self.loader_config["gdb_name"],
            dataset_name,
            relation_orig)
        dest_table = os.path.join(
            self.loader_config["gdb_name"],
            relation_dest)
        rel_table = os.path.join(
            self.loader_config["gdb_name"],
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
                origin_primary_key=self.file_config["id_column"],
                destination_primary_key=self.file_config["id_column"]
            )

    def store_processed_data(self, in_df: DataFrame):
        """
            Store provided data in specified database tables

        Args:
            in_df (DataFrame): Processed dataframe
        """

        arcpy = importlib.import_module("arcpy")

        if not Path(self.workspace).exists():
            # Create file geodatabase
            arcpy.management.CreateFileGDB(
                self.loader_config["gdb_path"],
                self.loader_config["gdb_name"],
                self.loader_config["version"]
            )

        if "geometry" in in_df.columns:
            in_df.drop("geometry", axis=1, inplace=True)

        # Set ID to string so it can be later used as primary key
        in_df = in_df.astype({self.file_config["id_column"]: "str"})

        arcpy.env.workspace = self.workspace

        # Create dataset
        if self.export_config["dataset_name"] != "" and not arcpy.Exists(self.export_config["dataset_name"]):
            arcpy.management.CreateFeatureDataset(
                self.workspace,
                self.export_config["dataset_name"],
                arcpy.SpatialReference(self.loader_config["crs"])
            )

        if self.export_config["entry_type"] == "featureclass":
            arcpy.env.workspace = os.path.join(
                self.workspace, self.export_config["dataset_name"])
            self.storeFeatureClass(arcpy, in_df)
        elif self.export_config["entry_type"] == "table":
            self.storeTable(arcpy, in_df)
            self.addTableRelation(arcpy)

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

    def create_point_geometry(self, x, y):
        """
            Create ArcPy PointGeometry from provided coordinates.

        Args:
            x: X coordinate
            y: Y coordinate

        Returns:
            PointGeometry: ArcPy specific geometry
        """

        arcpy = importlib.import_module("arcpy")
        return arcpy.PointGeometry(
            arcpy.Point(x, y),
            arcpy.SpatialReference(
                self.loader_config["crs"])
        )

    def drop_xy(self, df: DataFrame) -> DataFrame:
        """
            Remove X and Y columns from dataframe.

        Args:
            df (DataFrame): Input dataframe

        Returns:
            DataFrame: Dataframe without X and Y columns
        """
        if "x" in df.columns:
            df = df.drop("x", axis=1)
        if "y" in df.columns:
            df = df.drop("y", axis=1)
        return df

    def add_geometry(self, df: DataFrame) -> DataFrame:
        """
            Add shape information to dataframe containing coordinates

        Args:
            df (DataFrame): Input dataframe

        Returns:
            DataFrame: Dataframe which contains shape information
        """
        if "x" in df.columns and "y" in df.columns:
            point_geo = list(map(self.create_point_geometry, df["x"], df["y"]))
            if "Shape" in df.columns:
                df = df.drop("Shape", axis=1)
            df = df.drop(["x", "y"], axis=1)
            df.insert(0, "Shape", point_geo)
        return df

    def set_db_flags(self, in_df: DataFrame, flag: str) -> DataFrame:
        """
            Add column with flags to provided dataframe. This can be used
            to identify last inserts and/or updates of the database,
            which in turn can be used to trigger Arcade scripts on the side
            of the database. This prevents excessive triggers of these
            scripts, which significantly slow down the process.

        Args:
            in_df (DataFrame): Input dataframe
            flag (str): Flag to be set

        Returns:
            DataFrame: Dataframe with flags set
        """
        if not self.last_dir:
            in_df[flag] = 0
            return in_df

        # The folder which is processed last should have some data,
        # otherwise flag will not be set and no Arcade expressions
        # will be triggered.
        if (len(in_df) > 0):
            in_df[flag] = in_df[self.file_config["id_column"]
                                ] == in_df.iloc[-1][self.file_config["id_column"]]
        return in_df

    def addTableFields(self, arcpy, in_df: DataFrame, flags: list[str]):
        """
            Add fields which are included in the input dataframe,
            but are not present in the ArcGIS database table.

        Args:
            arcpy: Arcpy module
            in_df: Input dataframe
            flags (list[str]): List of flags to be added to table columns
        """

        table_fields = [field.name for field in arcpy.ListFields(
            self.export_config["entry_name"])]
        fields = [
            field for field in in_df.columns if not field in table_fields and field != "Shape"]

        # Add flag fields
        for flag in flags:
            if flag not in table_fields:
                fields.append(flag)

        for field in fields:
            field_alias = self.loader_config["aliases"][field] if field in self.loader_config["aliases"] else field

            if field in flags:
                dtype = "SHORT"
            elif field in self.loader_config["dtypes"]:
                # Get dtype from configuration file
                dtype = self.loader_config["dtypes"][field]
            else:
                # Try to determine correct type automatically
                dtype = self.getFieldTypes(in_df[field].dtype)

            arcpy.management.AddField(
                self.export_config["entry_name"],
                field,
                dtype,
                field_alias=field_alias)

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
            self.export_config["entry_name"]).isVersioned

        # Execute database modifications under Editor, so either all operations succeed or all fail
        with arcpy.da.Editor(self.workspace, multiuser_mode=versioned):
            # Update existing data
            if len(update_list) > 0:
                with arcpy.da.UpdateCursor(self.export_config["entry_name"], update_fields) as cursor:
                    for idx, row in enumerate(cursor):
                        # Update only masked entries
                        if update_mask[idx]:
                            cursor.updateRow(update_list[idx])

            # Insert new data
            if len(insert_list) > 0:
                with arcpy.da.InsertCursor(self.export_config["entry_name"], insert_fields) as cursor:
                    for row in insert_list:
                        cursor.insertRow(row)

    def storeFeatureClass(self, arcpy, in_df: DataFrame):
        """
            Process provided spatial data, modify fields of relevant tables
            and update existing and/or insert new entries.

        Args:
            arcpy: Arcpy module
            in_df (DataFrame): Input dataframe containing spatial data
        """

        if arcpy.Exists(self.export_config["entry_name"]):
            # Load current data from the database
            processed_data = self.load_processed_data()

            # Create masks of existing and new entries
            id_df = in_df.astype({self.file_config["id_column"]: "int64"})
            present_id_mask = id_df[self.file_config["id_column"]].isin(
                processed_data[self.file_config["id_column"]])
            update_mask = processed_data[self.file_config["id_column"]].isin(
                id_df[self.file_config["id_column"]]).to_list()

            in_df = in_df.replace({float("nan"): None})

            insert_df = in_df[~present_id_mask].copy()
            insert_df = self.set_db_flags(insert_df, "last_modify")
            update_df = in_df[present_id_mask].copy()
            update_df = self.set_db_flags(update_df, "last_modify")

            insert_df = self.add_geometry(insert_df)

            in_df = self.drop_xy(in_df)
            insert_df = self.drop_xy(insert_df)
            update_df = self.drop_xy(update_df)

            shared_columns = [
                col for col in update_df.columns if col in processed_data.columns]
            not_shared_columns = [
                col for col in update_df.columns if col not in processed_data.columns]

            # Common ID should always be shared between multiple tables
            not_shared_columns.append(self.file_config["id_column"])

            # Add missing columns to new entries
            for col in processed_data.columns:
                if col not in insert_df.columns:
                    insert_df[col] = None

            update_df = update_df.astype(
                {self.file_config["id_column"]: "int64"})

            # Split update data into existing and new columns
            current_cols_df = update_df[shared_columns]
            new_cols_df = update_df[not_shared_columns]
            update_data = processed_data.copy()

            # Update existing columns
            if len(shared_columns) > 1:
                current_cols_df = current_cols_df.set_index(
                    self.file_config["id_column"])
                update_data = update_data.set_index(
                    self.file_config["id_column"])
                print(update_data.dtypes)
                print(current_cols_df.dtypes)
                exit()
                update_data.update(current_cols_df)
                update_data = update_data.reset_index()

            # Add new columns to entries
            if len(not_shared_columns) > 1:
                update_data = update_data.merge(
                    new_cols_df,
                    "left",
                    self.file_config["id_column"])

            if "Shape" in update_data.columns:
                update_data = update_data.drop("Shape", axis=1)

            shape_col = insert_df.pop("Shape")
            insert_df.insert(0, "Shape", shape_col)

            self.addTableFields(arcpy, insert_df, ["last_modify"])

            insert_fields = [field.name for field in arcpy.ListFields(
                self.export_config["entry_name"]) if field.type != "OID"]

            # Reorder columns to match the table
            insert_df = insert_df[insert_fields]

            update_fields = [field.name for field in arcpy.ListFields(
                self.export_config["entry_name"]) if field.type != "OID" and field.name != "Shape"]

            # Modify table with new data
            self.modifyDatabase(
                arcpy,
                update_data.values.tolist(),
                update_fields,
                update_mask,
                insert_df.values.tolist(),
                insert_fields)
        else:
            in_df = self.add_geometry(in_df)
            arcpy.management.CreateFeatureclass(
                os.path.join(self.workspace,
                             self.export_config["dataset_name"]),
                self.export_config["entry_name"],
                "POINT",
                spatial_reference=arcpy.SpatialReference(self.loader_config["crs"]))

            self.addTableFields(arcpy, in_df, ["last_modify"])

            in_df = self.set_db_flags(in_df, "last_modify")

            # Insert new data into the table
            insert_list = in_df.values.tolist()
            insert_fields = [field.name for field in arcpy.ListFields(
                self.export_config["entry_name"]) if field.type != "OID"]
            self.modifyDatabase(
                arcpy,
                insert_list=insert_list,
                insert_fields=insert_fields)

    def storeTable(self, arcpy, in_df: DataFrame):
        """
            Process provided data, modify fields of relevant tables
            and update existing and/or insert new entries.

        Args:
            arcpy: Arcpy module
            in_df (DataFrame): Input dataframe which does not contain spatial data
        """

        if arcpy.Exists(self.export_config["entry_name"]):
            processed_data = self.load_processed_data()
            id_df = in_df.astype({self.file_config["id_column"]: "int64"})
            present_id_mask = id_df[self.file_config["id_column"]].isin(
                processed_data[self.file_config["id_column"]])

            update_mask = processed_data[self.file_config["id_column"]].isin(
                id_df[self.file_config["id_column"]]).to_list()

            update_df = in_df[present_id_mask].copy()
            insert_df = in_df[~present_id_mask].copy()

            shared_columns = [
                col for col in update_df.columns if col in processed_data.columns]
            not_shared_columns = [
                col for col in update_df.columns if col not in processed_data.columns]

            # Common ID should always be shared between multiple tables
            not_shared_columns.append(self.file_config["id_column"])

            # Add missing columns to new entries
            for col in processed_data.columns:
                if col not in insert_df.columns:
                    insert_df[col] = None

            update_df = update_df.astype(
                {self.file_config["id_column"]: "int64"})

            # Split update data into existing and new columns
            current_cols_df = update_df[shared_columns]
            new_cols_df = update_df[not_shared_columns]
            update_data = processed_data.copy()

            # Update existing columns
            if len(shared_columns) > 1:
                # Group entries by ID and add new column to track how many have the same ID
                current_cols_df["idx_duplicates"] = current_cols_df.groupby(
                    self.file_config["id_column"]).cumcount()
                update_data["idx_duplicates"] = update_data.groupby(
                    self.file_config["id_column"]).cumcount()

                # Set indexing to ID and duplicate count
                update_keys = update_data[
                    [self.file_config["id_column"], "idx_duplicates"]]
                new_cols_keys = current_cols_df[
                    [self.file_config["id_column"], "idx_duplicates"]]

                # Find missing entries
                new_cols_keys = new_cols_keys.reset_index()
                intersect_df = new_cols_keys.merge(
                    update_keys, "left",
                    [self.file_config["id_column"],
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
                    [self.file_config["id_column"], "idx_duplicates"])
                update_data = update_data.set_index(
                    [self.file_config["id_column"], "idx_duplicates"])

                # Update exisiting entries
                update_data.update(current_cols_df)
                update_data = update_data.reset_index()
                update_data = update_data.drop(["idx_duplicates"], axis=1)

            # Add new columns to entries
            if len(not_shared_columns) > 1:
                update_data = update_data.merge(
                    new_cols_df,
                    "left",
                    self.file_config["id_column"])

            self.addTableFields(arcpy, insert_df, [])

            # Modify table with new data
            table_fields = [field.name for field in arcpy.ListFields(
                self.export_config["entry_name"]) if field.type != "OID"]

            # Reorder columns to match the table
            insert_df = insert_df[table_fields]

            insert_list = insert_df.values.tolist()
            self.modifyDatabase(
                arcpy,
                update_data.values.tolist(),
                table_fields,
                update_mask,
                insert_list,
                table_fields)
        else:
            in_df = self.add_geometry(in_df)

            arcpy.management.CreateTable(
                self.workspace, self.export_config["entry_name"])

            self.addTableFields(arcpy, in_df, [])

            # Insert new data into the table
            insert_list = in_df.values.tolist()
            insert_fields = [field.name for field in arcpy.ListFields(
                self.export_config["entry_name"]) if field.type != "OID"]
            self.modifyDatabase(
                arcpy,
                insert_list=insert_list,
                insert_fields=insert_fields)


class GeoPandasFileLoader(BaseLoader):
    """
        Loader which uses GeoPandas methods to store processed data.
    """

    def load_processed_data(self) -> DataFrame:
        """
            Load stored data from the database.

        Returns:
            DataFrame: Loaded data from the database.
        """
        if Path(self.export_config["filename"]).exists():
            gdf = gpd.read_file(self.export_config["filename"])
            gdf = gdf.convert_dtypes()
            gdf = gdf.replace({float("nan"): None})
            return gdf
        return pd.DataFrame()

    def store_processed_data(self, in_df: DataFrame):
        """
            Save dataframe to a file. File type and mode can be set in config file.
            For additional information refer to geopandas.GeoDataFrame.to_file.

        Args:
            in_df (DataFrame): Processed data
        """
        if "Shape" in in_df.columns:
            in_df = in_df.drop("Shape", axis=1)

        processed_data = self.load_processed_data()

        if not processed_data.empty:
            present_id_mask = in_df[self.file_config["id_column"]].isin(
                processed_data[self.file_config["id_column"]])

            present_ids = in_df[present_id_mask]
            new_ids = in_df[~present_id_mask]
            if not present_ids.empty:
                shared_columns = [col for col in processed_data.columns if col in present_ids and col !=
                                  self.file_config["id_column"] and col != "geometry"]

                processed_data = processed_data.set_index(
                    self.file_config["id_column"])
                present_ids = present_ids.set_index(
                    self.file_config["id_column"])

                if len(shared_columns) > 1:

                    duplicate_ids_mask = pd.Index.duplicated(
                        processed_data.index, False)
                    unique_ids = processed_data[~duplicate_ids_mask]
                    duplicate_ids = processed_data[duplicate_ids_mask]

                    # Fill missing values in rows with UID
                    for col in shared_columns:
                        fill_col = present_ids.loc[~pd.Index.duplicated(
                            present_ids.index), col]
                        unique_ids.loc[:, col] = unique_ids.loc[:, col].fillna(
                            fill_col)
                    processed_data = pd.concat([duplicate_ids, unique_ids])

                    new_cols = [col for col in present_ids.columns if col not in shared_columns and col !=
                                self.file_config["id_column"] and col != "geometry"]
                    if len(new_cols) > 1:
                        duplicate_ids_mask = pd.Index.duplicated(
                            present_ids.index, "first")
                        unique_ids = present_ids[~duplicate_ids_mask]
                        duplicate_ids = present_ids[duplicate_ids_mask]
                        if len(duplicate_ids) > 0:
                            self.logger.warning(
                                "Multiple entries with the same ID found, consider storing these entries in separate table.")

                        # Join new columns on ID index
                        processed_data = processed_data.join(
                            unique_ids[new_cols], how="left", validate="1:1")
                else:
                    # No shared columns present, join on ID index
                    processed_data = processed_data.join(
                        present_ids, how="left")

                processed_data = processed_data.reset_index()
                in_df = processed_data

            if not new_ids.empty:
                in_df = pd.concat([processed_data, new_ids])

        if "coord_cols" not in self.file_config or self.file_config["coord_cols"] == None:
            if "x" not in in_df.columns:
                in_df["x"] = 0
            else:
                in_df.loc[:, "x"] = in_df.loc[:, "x"].fillna(0)
            if "y" not in in_df.columns:
                in_df["y"] = 0
            else:
                in_df.loc[:, "y"] = in_df.loc[:, "y"].fillna(0)

        in_df = gpd.GeoDataFrame(in_df, geometry=gpd.points_from_xy(
            in_df["x"], in_df["y"]), crs=self.loader_config["crs"])

        in_df = in_df.convert_dtypes()
        in_df = in_df.replace({float("nan"): None})

        in_df.to_file(self.export_config["filename"], mode="w")
