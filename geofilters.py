import pandas as pd
import geopandas as gpd
import importlib
from pandas import DataFrame
from abc import ABC, abstractmethod


class BaseGeofilter(ABC):
    """
        Abstract geofilter class which serves as a base
        for all other geofilters.
    """

    @abstractmethod
    def __init__(self, config: dict, file_config: dict):
        raise NotImplementedError

    @abstractmethod
    def point_xy_validation(self, in_df: DataFrame) -> tuple[list[pd.Series], list[pd.Series]]:
        """
            Split data into two lists containing entries with valid
            and invalid coordinates respectively. For S-JTSK (EPSG:5514) a valid
            x coordinate must always be larger than y coordinate, otherwise
            they need to be swapped.

        Args:
            in_df (DataFrame): Dataframe containing spatial data
            geo_col (str): Name of the column containing point geometry data

        Returns:
            tuple[list[pd.Series], list[pd.Series]]: Lists of entries with
              valid and invalid coordinates
        """
        raise NotImplementedError

    @abstractmethod
    def point_poly_intersect(self, in_df: DataFrame, polygon) -> list[pd.Series]:
        """
            Create list of entries which intersect with the area of the provided polygon.

        Args:
            in_df (DataFrame): Dataframe containing spatial data
            polygon (_type_): Polygon with which the points will be intersected
            geo_col (str): Name of the column containing point geometry data

        Returns:
            list[pd.Series]: List of intersecting entries.
        """
        raise NotImplementedError

    @abstractmethod
    def load_polygon_filter(self):
        """
            Load polygon which will be used to filter point geometry entries.

        Returns:
            Loaded polygon entry
        """
        raise NotImplementedError

    @abstractmethod
    def add_geometry(self, in_df: DataFrame) -> DataFrame:
        """
            Convert input dataframe into a spatial dataframe.

        Args:
            in_df (DataFrame): Dataframe containing spatial data

        Returns:
            DataFrame: Dataframe with appropriate geometry column
        """
        raise NotImplementedError
        # If ArcPy is enabled, then the dataframe is converted
        # using GeoAccessor and GeoSeriesAccessor libraries and new SHAPE column is added.

        # If ArcPy is not enabled geometry column with point values is created using GeoPandas.

    @abstractmethod
    def swap_xy(self, in_df: DataFrame) -> DataFrame:
        """
            Swap contents of x and y columns in a given dataframe.

        Args:
            in_df (DataFrame): Dataframe with incorrectly labeled x and y columns.

        Returns:
            DataFrame: Dataframe with swapped content of x and y columns.
        """
        raise NotImplementedError


class ArcpyGeofilter(BaseGeofilter):
    """
        Geofilter which uses ArcPy methods to process spatial data.
    """

    def __init__(self, config: dict, file_config: dict):
        """
            ArcpyGeofilter constructor.

        Args:
            config (dict): Script configuration
            file_config (dict): Specific file configuration
        """

        self.config = config
        self.file_config = file_config
        self.polygon_filter = config["polygon_filter"]

    def point_xy_validation(self, in_df: DataFrame):
        in_df = in_df[~in_df.is_empty]

        valid_mask = in_df["x"] > in_df["y"]
        valid_rows = in_df[valid_mask]
        invalid_rows = in_df[~valid_mask]

        return valid_rows, invalid_rows

    def point_poly_intersect(self, in_df: DataFrame, polygon):
        in_df = in_df[~in_df.is_empty]

        intersect_mask = in_df["geometry"].intersects(polygon)

        return in_df[intersect_mask]

    def load_polygon_filter(self):
        arcpy = importlib.import_module("arcpy")

        # Load polygons into the dataframe
        polygon_df = pd.DataFrame(arcpy.da.SearchCursor(
            self.polygon_filter["arcpy_gdb"]["file_path"] +
            self.polygon_filter["arcpy_gdb"]["feature_class"],
            self.polygon_filter["arcpy_gdb"]["polygon_cols"]),
            columns=self.polygon_filter["arcpy_gdb"]["polygon_cols"]
        )

        assert polygon_df.empty == False, f"No polygon found at {self.polygon_filter['arcpy_gdb']['file_path']}."

        # Select target polygon to filter with
        polygon_df = polygon_df[polygon_df[self.polygon_filter['arcpy_gdb']["polygon_id_col"]]
                                == self.polygon_filter['arcpy_gdb']["polygon_id"]]

        assert len(
            polygon_df) == 1, f"Multiple polygons with the value {self.polygon_filter['arcpy_gdb']['polygon_id']} in column {self.polygon_filter['arcpy_gdb']['polygon_id_col']} found. Check 'polygon_filter' in the configuration file."

        polygon = gpd.GeoDataFrame(polygon_df).set_geometry(
            "SHAPE@").iloc[0]["SHAPE@"]
        return polygon

    def add_geometry(self, in_df):
        return gpd.GeoDataFrame(in_df, geometry=gpd.points_from_xy(in_df["x"], in_df["y"]), crs=5514)

    def swap_xy(self, in_df):
        # Swap names of columns containing coordinates
        renamed_df = in_df.rename(columns={"x": "y", "y": "x"})

        # Drop existing shape column
        if "geometry" in renamed_df.columns:
            renamed_df.drop("geometry", axis=1)

        # Recreate point shapes with swapped coordinates
        return self.add_geometry(renamed_df)


class GeoPandasGeofilter(BaseGeofilter):
    """
        Geofilter which uses GeoPandas methods to process spatial data.
    """

    def __init__(self, config: dict, file_config: dict):
        """
            GeoPandasGeofilter constructor.

        Args:
            config (dict): Script configuration
            file_config (dict): Specific file configuration
        """

        self.config = config
        self.file_config = file_config
        self.polygon_filter = config["polygon_filter"]

    def point_xy_validation(self, in_df: DataFrame):
        in_df = in_df[~in_df.is_empty]

        valid_mask = in_df["x"] > in_df["y"]
        valid_rows = in_df[valid_mask]
        invalid_rows = in_df[~valid_mask]

        return valid_rows, invalid_rows

    def point_poly_intersect(self, in_df, polygon):
        in_df = in_df[~in_df.is_empty]

        intersect_mask = in_df["geometry"].intersects(polygon)

        return in_df[intersect_mask]

    def load_polygon_filter(self):
        polygon_df = gpd.read_file(
            self.polygon_filter["gpd_file"]["file_path"])
        assert polygon_df.empty == False, f"No polygon found at {self.polygon_filter['gpd_file']['file_path']}."

        if len(polygon_df) > 1:
            # Select target polygon to filter with
            polygon_df = polygon_df[polygon_df[self.polygon_filter['gpd_file']["polygon_id_col"]]
                                    == self.polygon_filter['gpd_file']["polygon_id"]]

            assert len(
                polygon_df) == 1, f"Multiple polygons with the value {self.polygon_filter['gpd_file']['polygon_id']} in column {self.polygon_filter['gpd_file']['polygon_id_col']} found. Check 'polygon_filter' in the configuration file."

        # Convert Coordinate Reference System
        polygon = gpd.GeoDataFrame(polygon_df).to_crs(5514).iloc[0]["geometry"]

        return polygon

    def add_geometry(self, in_df):
        return gpd.GeoDataFrame(in_df, geometry=gpd.points_from_xy(in_df["x"], in_df["y"]), crs=5514)

    def swap_xy(self, in_df):
        # Swap names of columns containing coordinates
        renamed_df = in_df.rename(columns={"x": "y", "y": "x"})

        # Drop existing geometry column
        if "geometry" in renamed_df.columns:
            renamed_df.drop("geometry", axis=1)

        # Recreate point shapes with swapped coordinates
        return self.add_geometry(renamed_df)
