import geopandas as gpd
import pandas as pd
import importlib
import requests
from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """
        Abstract extractor class which serves as a base
        for all other API extractors.
    """

    def __init__(self, config: dict, api_config: dict):
        """
            BaseExtractor constructor

        Args:
            config (dict): Script configuration
            api_config (dict): API configuration
        """
        self.config = config
        self.api_config = api_config

    @abstractmethod
    def extract_data(self):
        """Extract data from specified API

        Raises:
            NotImplementedError: Implement custom data extraction
        """
        raise NotImplementedError


class ArcPyExtractor(BaseExtractor):
    """
        Extractor which processes API data using ArcPy methods.
    """

    def __init__(self, config: dict, api_config: dict):
        """
            ArcPyExtractor constructor. Additional ArcGIS modules need
            to be imported for spatial arcpy methods to function.

        Args:
            config (dict): Script configuration
            api_config (dict): API configuration
        """

        self.config = config
        self.api_config = api_config

        importlib.import_module("arcgis.features", "GeoSeriesAccessor")
        importlib.import_module("arcgis.features", "GeoAccessor")

    def create_point_geometry(self, shape):
        """
            Convert input geometry to ArcPy PointGeometry

        Args:
            shape: Input geometry

        Returns:
            PointGeometry: ArcPy specific geometry
        """

        arcpy = importlib.import_module("arcpy")
        return arcpy.PointGeometry(
            arcpy.Point(shape.centroid[0], shape.centroid[1]),
            arcpy.SpatialReference(self.api_config["crs"])
        )

    def extract_data(self) -> pd.DataFrame:
        """
            Fetch data from specified API and process it.

        Returns:
            pd.DataFrame: Processed API data
        """

        # Fetch data from specified API
        request = self.api_config["url"] + \
            f"&resultOffset={0}&resultRecordCount={self.api_config['result_record_count']}"
        resp = requests.get(request)
        df = gpd.read_file(request)

        # When the data did not fit into a single response "exceededTransferLimit" flag is set to true.
        # If it was raised then the request are sent until it becomes false to fetch all of the data.
        if "exceededTransferLimit" in resp.json():
            offset = self.api_config["result_record_count"]
            while "exceededTransferLimit" in resp.json() and resp.json()["exceededTransferLimit"]:

                # Contruct new request to fetch data beggining from the offset
                request = self.api_config["url"] + \
                    f"&resultOffset={offset}&resultRecordCount={offset + self.api_config['result_record_count']}"
                resp = requests.get(request)
                tmp_df = gpd.read_file(request)
                df = pd.concat([df, tmp_df])
                offset += self.api_config["result_record_count"]

        # Update data which contains spatial data to match arcpy
        if any(df["geometry"] != None):
            df = df.to_crs(epsg=self.api_config["crs"])

            # Convert GeoPandas geometry to arcpy shape
            df = pd.DataFrame.spatial.from_geodataframe(df)
            df = df.rename(columns={"SHAPE": "Shape"})

            point_geo = list(map(self.create_point_geometry, df["Shape"]))
            df = df.drop(["Shape"], axis=1)
            df.insert(0, "Shape", point_geo)
        else:
            # Remove empty geometry
            df = df.drop(["geometry"], axis=1)

        # Drop columns in the config
        df = df.drop(self.api_config["drop_columns"], axis=1)

        return df


class GeoPandasExtractor(BaseExtractor):
    """
        Extractor which processes data only using freely available GeoPandas methods.
    """

    def extract_data(self) -> pd.DataFrame:
        """
            Fetch data from specified API and process it.

        Returns:
            pd.DataFrame: Processed API data
        """

        # Fetch data from specified API
        request = self.api_config["url"] + \
            f"&resultOffset={0}&resultRecordCount={self.api_config['result_record_count']}"
        resp = requests.get(request)
        df = gpd.read_file(request)

        # When the data did not fit into a single response "exceededTransferLimit" flag is set to true.
        # If it was raised then the request are sent until it becomes false to fetch all of the data.
        if "exceededTransferLimit" in resp.json():
            offset = self.api_config["result_record_count"]
            while "exceededTransferLimit" in resp.json() and resp.json()["exceededTransferLimit"]:

                # Contruct new request to fetch data beggining from the offset
                request = self.api_config["url"] + \
                    f"&resultOffset={offset}&resultRecordCount={offset + self.api_config['result_record_count']}"
                resp = requests.get(request)
                tmp_df = gpd.read_file(request)
                df = pd.concat([df, tmp_df])
                offset += self.api_config["result_record_count"]

        return df
